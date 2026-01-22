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

import json
import os
from unittest.mock import MagicMock, patch

import yaml

from mcp_server_snowflake.server import (
    DEFAULT_QUERY_COMMENT_TEMPLATE,
    SnowflakeService,
)


def create_config_with_query_comment(tmp_path, query_comment_config):
    """Helper to create a config file with query_comment settings."""
    config = {
        "other_services": {
            "query_manager": True,
        },
        "query_comment": query_comment_config,
    }
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config, f)
    return config_file


class TestQueryCommentConfiguration:
    """Tests for query comment configuration parsing."""

    def test_query_comment_disabled_by_default(self, tmp_path):
        """Test that query comments are disabled when not configured."""
        config = {"other_services": {"query_manager": True}}
        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        with (
            patch("mcp_server_snowflake.server.connect") as mock_connect,
            patch("mcp_server_snowflake.server.Root") as mock_root,
        ):
            mock_connect.return_value = MagicMock()
            mock_root.return_value = MagicMock()
            service = SnowflakeService(
                service_config_file=str(config_file),
                transport="stdio",
                connection_params={
                    "account": "test",
                    "user": "test",
                    "password": "test",
                },
            )

        assert service.query_comment_enabled is False
        assert service.query_comment_template is None

    def test_query_comment_enabled_with_default_template(self, tmp_path):
        """Test that enabling query comments uses default template."""
        config_file = create_config_with_query_comment(tmp_path, {"enabled": True})

        with (
            patch("mcp_server_snowflake.server.connect") as mock_connect,
            patch("mcp_server_snowflake.server.Root") as mock_root,
        ):
            mock_connect.return_value = MagicMock()
            mock_root.return_value = MagicMock()
            service = SnowflakeService(
                service_config_file=str(config_file),
                transport="stdio",
                connection_params={
                    "account": "test",
                    "user": "test",
                    "password": "test",
                },
            )

        assert service.query_comment_enabled is True
        assert service.query_comment_template == DEFAULT_QUERY_COMMENT_TEMPLATE

    def test_query_comment_custom_template(self, tmp_path):
        """Test that custom template overrides default."""
        custom_template = {
            "agent": "my-agent",
            "request_id": "{request_id}",
            "custom_field": "custom_value",
        }
        config_file = create_config_with_query_comment(
            tmp_path, {"enabled": True, "template": custom_template}
        )

        with (
            patch("mcp_server_snowflake.server.connect") as mock_connect,
            patch("mcp_server_snowflake.server.Root") as mock_root,
        ):
            mock_connect.return_value = MagicMock()
            mock_root.return_value = MagicMock()
            service = SnowflakeService(
                service_config_file=str(config_file),
                transport="stdio",
                connection_params={
                    "account": "test",
                    "user": "test",
                    "password": "test",
                },
            )

        assert service.query_comment_enabled is True
        assert service.query_comment_template == custom_template


class TestBuildQueryComment:
    """Tests for build_query_comment method."""

    def test_returns_none_when_disabled(self, tmp_path):
        """Test that build_query_comment returns None when disabled."""
        config = {"other_services": {"query_manager": True}}
        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        with (
            patch("mcp_server_snowflake.server.connect") as mock_connect,
            patch("mcp_server_snowflake.server.Root") as mock_root,
        ):
            mock_connect.return_value = MagicMock()
            mock_root.return_value = MagicMock()
            service = SnowflakeService(
                service_config_file=str(config_file),
                transport="stdio",
                connection_params={
                    "account": "test",
                    "user": "test",
                    "password": "test",
                },
            )

        result = service.build_query_comment(
            tool_name="test_tool", statement_type="Select"
        )
        assert result is None

    def test_substitutes_template_variables(self, tmp_path):
        """Test that template variables are substituted correctly."""
        config_file = create_config_with_query_comment(tmp_path, {"enabled": True})

        with (
            patch("mcp_server_snowflake.server.connect") as mock_connect,
            patch("mcp_server_snowflake.server.Root") as mock_root,
            patch.dict(os.environ, {"SNOWFLAKE_MCP_MODEL": "claude-sonnet-4"}),
        ):
            mock_connect.return_value = MagicMock()
            mock_root.return_value = MagicMock()
            service = SnowflakeService(
                service_config_file=str(config_file),
                transport="stdio",
                connection_params={
                    "account": "test",
                    "user": "test",
                    "password": "test",
                },
            )

            result = service.build_query_comment(
                tool_name="run_snowflake_query", statement_type="Select"
            )

        assert result is not None
        comment = json.loads(result)

        # Check substituted values
        assert comment["tool"] == "run_snowflake_query"
        assert comment["statement_type"] == "Select"
        assert comment["model"] == "claude-sonnet-4"
        assert comment["source"] == "mcp-server-snowflake"

        # Check that UUID and timestamp were generated (not literal template strings)
        assert "{request_id}" not in comment["request_id"]
        assert "{timestamp}" not in comment["timestamp"]
        assert len(comment["request_id"]) == 36  # UUID format

    def test_nested_template_substitution(self, tmp_path):
        """Test that nested template variables are substituted."""
        custom_template = {
            "metadata": {
                "tool": "{tool_name}",
                "type": "{statement_type}",
            },
            "tags": ["{model}", "{server_name}"],
        }
        config_file = create_config_with_query_comment(
            tmp_path, {"enabled": True, "template": custom_template}
        )

        with (
            patch("mcp_server_snowflake.server.connect") as mock_connect,
            patch("mcp_server_snowflake.server.Root") as mock_root,
            patch.dict(os.environ, {"SNOWFLAKE_MCP_MODEL": "test-model"}),
        ):
            mock_connect.return_value = MagicMock()
            mock_root.return_value = MagicMock()
            service = SnowflakeService(
                service_config_file=str(config_file),
                transport="stdio",
                connection_params={
                    "account": "test",
                    "user": "test",
                    "password": "test",
                },
            )

            result = service.build_query_comment(
                tool_name="my_tool", statement_type="Insert"
            )

        comment = json.loads(result)
        assert comment["metadata"]["tool"] == "my_tool"
        assert comment["metadata"]["type"] == "Insert"
        assert comment["tags"][0] == "test-model"
        assert comment["tags"][1] == "mcp-server-snowflake"

    def test_model_defaults_to_unknown(self, tmp_path):
        """Test that model defaults to 'unknown' when env var not set."""
        config_file = create_config_with_query_comment(tmp_path, {"enabled": True})

        with (
            patch("mcp_server_snowflake.server.connect") as mock_connect,
            patch("mcp_server_snowflake.server.Root") as mock_root,
            patch.dict(os.environ, {}, clear=True),
        ):
            # Ensure SNOWFLAKE_MCP_MODEL is not set
            os.environ.pop("SNOWFLAKE_MCP_MODEL", None)

            mock_connect.return_value = MagicMock()
            mock_root.return_value = MagicMock()
            service = SnowflakeService(
                service_config_file=str(config_file),
                transport="stdio",
                connection_params={
                    "account": "test",
                    "user": "test",
                    "password": "test",
                },
            )

            result = service.build_query_comment(
                tool_name="test_tool", statement_type="Select"
            )

        comment = json.loads(result)
        assert comment["model"] == "unknown"
