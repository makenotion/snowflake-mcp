# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from mcp_server_snowflake.query_manager.tools import (
    get_statement_type,
    validate_sql_type,
)


class TestGetStatementType:
    """Tests for get_statement_type function."""

    def test_select_statement(self):
        """Test that SELECT statements are correctly identified."""
        assert get_statement_type("SELECT * FROM my_table") == "Select"

    def test_insert_statement(self):
        """Test that INSERT statements are correctly identified."""
        assert get_statement_type("INSERT INTO my_table VALUES (1, 2)") == "Insert"

    def test_update_statement(self):
        """Test that UPDATE statements are correctly identified."""
        assert get_statement_type("UPDATE my_table SET col = 1") == "Update"

    def test_delete_statement(self):
        """Test that DELETE statements are correctly identified."""
        assert get_statement_type("DELETE FROM my_table WHERE id = 1") == "Delete"

    def test_create_table_statement(self):
        """Test that CREATE TABLE statements are correctly identified."""
        assert get_statement_type("CREATE TABLE my_table (id INT)") == "Create"

    def test_drop_table_statement(self):
        """Test that DROP TABLE statements are correctly identified."""
        assert get_statement_type("DROP TABLE my_table") == "Drop"

    def test_copy_statement(self):
        """Test that COPY statements are correctly identified (Snowflake-specific)."""
        assert get_statement_type("COPY INTO my_table FROM @my_stage") == "Copy"

    def test_copy_statement_with_options(self):
        """Test COPY with Snowflake-specific options is correctly identified."""
        sql = "COPY INTO my_table FROM @my_stage FILE_FORMAT = (TYPE = 'CSV')"
        assert get_statement_type(sql) == "Copy"

    def test_merge_statement(self):
        """Test that MERGE statements are correctly identified."""
        sql = """
        MERGE INTO target_table USING source_table
        ON target_table.id = source_table.id
        WHEN MATCHED THEN UPDATE SET col = source_table.col
        """
        assert get_statement_type(sql) == "Merge"

    def test_invalid_sql_returns_unknown(self):
        """Test that invalid SQL returns 'Unknown'."""
        assert get_statement_type("THIS IS NOT VALID SQL @#$%") == "Unknown"

    def test_empty_string_returns_unknown(self):
        """Test that empty string returns 'Unknown'."""
        assert get_statement_type("") == "Unknown"


class TestValidateSqlType:
    """Tests for validate_sql_type function."""

    def test_allowed_statement_is_valid(self):
        """Test that an allowed statement type returns valid=True."""
        statement_type, valid = validate_sql_type(
            "SELECT * FROM my_table",
            sql_allow_list=["select"],
            sql_disallow_list=[],
        )
        assert statement_type == "Select"
        assert valid is True

    def test_disallowed_statement_is_invalid(self):
        """Test that a disallowed statement type returns valid=False."""
        statement_type, valid = validate_sql_type(
            "DROP TABLE my_table",
            sql_allow_list=["select"],
            sql_disallow_list=["drop"],
        )
        assert statement_type == "Drop"
        assert valid is False

    def test_all_allows_any_statement(self):
        """Test that 'all' in allow list permits any statement."""
        statement_type, valid = validate_sql_type(
            "DROP TABLE my_table",
            sql_allow_list=["all"],
            sql_disallow_list=[],
        )
        assert statement_type == "Drop"
        assert valid is True

    def test_unknown_allowed_permits_unparsable(self):
        """Test that 'unknown' in allow list permits unparsable statements."""
        statement_type, valid = validate_sql_type(
            "SOME INVALID SQL @#$%",
            sql_allow_list=["unknown"],
            sql_disallow_list=[],
        )
        assert statement_type == "Unknown"
        assert valid is True

    def test_unknown_disallowed_rejects_unparsable(self):
        """Test that unparsable statements are rejected when not in allow list."""
        statement_type, valid = validate_sql_type(
            "SOME INVALID SQL @#$%",
            sql_allow_list=["select"],
            sql_disallow_list=["unknown"],
        )
        assert statement_type == "Unknown"
        assert valid is False

    def test_empty_lists_disallow_all(self):
        """Test that empty allow/disallow lists default to disallowing all."""
        statement_type, valid = validate_sql_type(
            "SELECT * FROM my_table",
            sql_allow_list=[],
            sql_disallow_list=[],
        )
        assert statement_type == "Select"
        assert valid is False

    def test_copy_statement_allowed(self):
        """Test that COPY statements work with allow list (issue #161 fix)."""
        statement_type, valid = validate_sql_type(
            "COPY INTO my_table FROM @my_stage",
            sql_allow_list=["copy"],
            sql_disallow_list=["unknown"],
        )
        assert statement_type == "Copy"
        assert valid is True

    def test_disallow_takes_precedence(self):
        """Test that disallow list takes precedence over allow list."""
        statement_type, valid = validate_sql_type(
            "SELECT * FROM my_table",
            sql_allow_list=["select"],
            sql_disallow_list=["select"],
        )
        assert statement_type == "Select"
        assert valid is False

    def test_unlisted_statement_rejected(self):
        """Test that statement types not in either list are rejected."""
        statement_type, valid = validate_sql_type(
            "CREATE TABLE my_table (id INT)",
            sql_allow_list=["select", "insert"],
            sql_disallow_list=[],
        )
        assert statement_type == "Create"
        assert valid is False


class TestSnowflakeSpecificSyntax:
    """Tests for Snowflake-specific SQL syntax parsing."""

    def test_json_colon_path_access(self):
        """Test Snowflake JSON colon notation: data:field."""
        sql = "SELECT data:name FROM my_table"
        assert get_statement_type(sql) == "Select"

    def test_json_nested_colon_path(self):
        """Test Snowflake nested JSON path: data:field.subfield."""
        sql = "SELECT data:user.address.city FROM my_table"
        assert get_statement_type(sql) == "Select"

    def test_json_bracket_notation(self):
        """Test Snowflake JSON bracket notation: data['field']."""
        sql = "SELECT data['name'] FROM my_table"
        assert get_statement_type(sql) == "Select"

    def test_json_array_index(self):
        """Test Snowflake JSON array index: data[0]."""
        sql = "SELECT items[0] FROM my_table"
        assert get_statement_type(sql) == "Select"

    def test_parse_json_function(self):
        """Test Snowflake PARSE_JSON function."""
        sql = 'SELECT PARSE_JSON(\'{"name": "test"}\') AS json_data'
        assert get_statement_type(sql) == "Select"

    def test_flatten_function(self):
        """Test Snowflake FLATTEN function for JSON arrays."""
        sql = """
        SELECT f.value
        FROM my_table,
        LATERAL FLATTEN(input => data:items) f
        """
        assert get_statement_type(sql) == "Select"

    def test_object_construct(self):
        """Test Snowflake OBJECT_CONSTRUCT function."""
        sql = "SELECT OBJECT_CONSTRUCT('key1', val1, 'key2', val2) FROM my_table"
        assert get_statement_type(sql) == "Select"

    def test_array_agg(self):
        """Test Snowflake ARRAY_AGG function."""
        sql = "SELECT ARRAY_AGG(col) FROM my_table GROUP BY id"
        assert get_statement_type(sql) == "Select"

    def test_try_parse_json(self):
        """Test Snowflake TRY_PARSE_JSON function."""
        sql = "SELECT TRY_PARSE_JSON(json_string) FROM my_table"
        assert get_statement_type(sql) == "Select"

    def test_get_path_function(self):
        """Test Snowflake GET_PATH function."""
        sql = "SELECT GET_PATH(data, 'user.name') FROM my_table"
        assert get_statement_type(sql) == "Select"

    def test_complex_json_query(self):
        """Test complex Snowflake JSON query with multiple features."""
        sql = """
        SELECT
            t.id,
            t.data:user.name::STRING AS user_name,
            f.value:item_id::INTEGER AS item_id,
            PARSE_JSON(t.metadata) AS parsed_meta
        FROM my_table t,
        LATERAL FLATTEN(input => t.data:items) f
        WHERE t.data:active::BOOLEAN = TRUE
        """
        assert get_statement_type(sql) == "Select"

    def test_variant_cast(self):
        """Test Snowflake VARIANT type casting with ::."""
        sql = "SELECT data:count::INTEGER, data:name::VARCHAR FROM my_table"
        assert get_statement_type(sql) == "Select"

    def test_object_keys(self):
        """Test Snowflake OBJECT_KEYS function."""
        sql = "SELECT OBJECT_KEYS(data) FROM my_table"
        assert get_statement_type(sql) == "Select"

    def test_array_construct(self):
        """Test Snowflake ARRAY_CONSTRUCT function."""
        sql = "SELECT ARRAY_CONSTRUCT(1, 2, 3) AS my_array"
        assert get_statement_type(sql) == "Select"

    def test_json_in_where_clause(self):
        """Test JSON path in WHERE clause."""
        sql = "SELECT * FROM my_table WHERE data:status = 'active'"
        assert get_statement_type(sql) == "Select"
