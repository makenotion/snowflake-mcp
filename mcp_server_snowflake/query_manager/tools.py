from typing import Annotated

import sqlglot
from fastmcp import FastMCP
from pydantic import Field

from mcp_server_snowflake.query_manager.prompts import query_tool_prompt
from mcp_server_snowflake.utils import SnowflakeException


def run_query(
    statement: str, snowflake_service, tool_name: str = "run_snowflake_query"
):
    """
    Execute SQL statement and fetch all results using Snowflake connector.

    Establishes a connection to Snowflake, executes the provided SQL statement,
    and returns all results using a dictionary cursor for easier data access.

    Parameters
    ----------
    statement : str
        SQL statement to execute
    snowflake_service : SnowflakeService
        The Snowflake service instance to use for connection
    tool_name : str
        Name of the tool executing the query (for query comments)

    Returns
    -------
    list[dict]
        List of dictionaries containing query results with column names as keys

    Raises
    ------
    snowflake.connector.errors.Error
        If connection fails or SQL execution encounters an error
    """
    try:
        # Get statement type for query comment
        statement_type = get_statement_type(statement)

        # Build query comment if enabled
        query_comment = snowflake_service.build_query_comment(
            tool_name=tool_name,
            statement_type=statement_type,
        )

        # Prepend comment to statement if enabled
        if query_comment:
            statement_with_comment = f"/* {query_comment} */\n{statement}"
        else:
            statement_with_comment = statement

        with snowflake_service.get_connection(
            use_dict_cursor=True,
            session_parameters=snowflake_service.get_query_tag_param(),
        ) as (
            con,
            cur,
        ):
            cur.execute(statement_with_comment)
            return cur.fetchall()
    except Exception as e:
        raise SnowflakeException(
            tool="query_manager",
            message=f"Error executing query: {e}",
            status_code=500,
        )


def initialize_query_manager_tool(server: FastMCP, snowflake_service):
    @server.tool(
        name="run_snowflake_query",
        description=query_tool_prompt,
    )
    def run_query_tool(
        statement: Annotated[
            str,
            Field(description="SQL query to execute"),
        ],
    ):
        return run_query(statement, snowflake_service)

    @server.tool(
        name="set_query_context",
        description="""Set runtime context for query comments and observability.

Call this tool at the start of a session to register context information that will be
included in all subsequent SQL query comments. This enables tracking queries back to
specific agents, models, or sessions in Snowflake's query history.

Common context keys:
- model: The AI model name (e.g., "claude-sonnet-4-5-20250929")
- agent_name: Name of the agent or application (e.g., "Claude Code")
- user_email: Email of the user running the agent
- user_name: Name of the user running the agent
- intent: Object describing query intent (category, confidence, domains, question)
- query_parameters: Object describing query details (datasets, dimensions, time_range)
- session_id: A unique session identifier for grouping related queries

Context persists for the lifetime of the MCP server connection. Call this tool again
to update intent/query_parameters for different queries.""",
    )
    def set_query_context_tool(
        model: Annotated[
            str,
            Field(
                default=None,
                description="AI model name (e.g., 'claude-sonnet-4-5-20250929')",
            ),
        ] = None,
        agent_name: Annotated[
            str,
            Field(
                default=None,
                description="Name of the agent or application (e.g., 'Claude Code')",
            ),
        ] = None,
        user_email: Annotated[
            str,
            Field(
                default=None,
                description="Email of the user running the agent",
            ),
        ] = None,
        user_name: Annotated[
            str,
            Field(
                default=None,
                description="Name of the user running the agent",
            ),
        ] = None,
        intent: Annotated[
            dict,
            Field(
                default=None,
                description="Query intent: {category, confidence, domains, question}",
            ),
        ] = None,
        query_parameters: Annotated[
            dict,
            Field(
                default=None,
                description="Query parameters: {datasets, dimensions, time_range}",
            ),
        ] = None,
        session_id: Annotated[
            str,
            Field(
                default=None,
                description="Unique session identifier for grouping queries",
            ),
        ] = None,
        custom_context: Annotated[
            dict,
            Field(
                default=None,
                description="Additional custom key-value pairs for context",
            ),
        ] = None,
    ):
        """Set query context for observability."""
        context = {}
        if model is not None:
            context["model"] = model
        if agent_name is not None:
            context["agent_name"] = agent_name
        if user_email is not None:
            context["user_email"] = user_email
        if user_name is not None:
            context["user_name"] = user_name
        if intent is not None:
            context["intent"] = intent
        if query_parameters is not None:
            context["query_parameters"] = query_parameters
        if session_id is not None:
            context["session_id"] = session_id
        if custom_context is not None:
            context.update(custom_context)

        updated_context = snowflake_service.set_query_context(**context)
        return {
            "status": "success",
            "message": "Query context updated successfully",
            "context": updated_context,
        }

    @server.tool(
        name="get_query_context",
        description="Get the current query context that will be included in query comments.",
    )
    def get_query_context_tool():
        """Get current query context."""
        return {
            "context": snowflake_service.get_query_context(),
            "query_comments_enabled": snowflake_service.query_comment_enabled,
        }


def get_statement_type(sql_string):
    """
    Parses a SQL statement and returns its primary command type.
    """
    try:
        # Parse the SQL statement. The root of the AST is the statement type.
        expression_tree = sqlglot.parse_one(sql_string, dialect="snowflake")

        # The expression type is the class of the root node.
        statement_type = type(expression_tree).__name__

        return statement_type
    except (
        sqlglot.errors.ParseError
    ):  # We will map this back to user's Unknown statement type setting
        return "Unknown"


def validate_sql_type(
    sql_string: str, sql_allow_list: list[str], sql_disallow_list: list[str]
) -> tuple[str, bool]:
    """
    Validates a SQL statement type against a list of allowed and disallowed statement types.
    """
    statement_type = get_statement_type(sql_string)
    if (
        "all" in sql_allow_list
    ):  # Escape hatch for allowing all statements if user elects to explicitly
        valid = True
    elif (
        statement_type.lower() in sql_disallow_list
    ):  # Allow/Disallow lists should already be lowercase at load
        valid = False
    elif statement_type.lower() in sql_allow_list:
        valid = True
    # There may be a new unmapped type that is not in the allow/disallow lists. If the user has set Unknown to True, allow it.
    elif "unknown" in sql_allow_list:
        valid = True

    # User has not added any permissions, so we default to disallowing all statements
    elif len(sql_allow_list) == 0 and len(sql_disallow_list) == 0:
        valid = False

    else:  # If not in allowed or disallowed and unknown in disallow or omitted, return error. User can always add to list as they find statements not otherwise allowed.
        valid = False

    return (statement_type, valid)
