# Claude Code Instructions

## Publishing to pyx

To manually publish a version to the pyx registry:

```bash
# Build the wheel
uv build --wheel

# Publish (requires authentication via `uv auth` or UV_PUBLISH_TOKEN env var)
uv publish --publish-url https://api.pyx.dev/v1/upload/notion/main dist/notion_snowflake_mcp-<version>-py3-none-any.whl
```

### Automated Publishing (CI/CD)

Publishing happens automatically in CI when:
1. A commit starting with `chore(release): notion-snowflake-mcp` is pushed to main
2. The workflow creates a git tag, builds the wheel, and publishes to pyx

The release flow:
1. Push changes to main
2. CI creates a release PR with version bump (via semantic-release)
3. Merge the release PR
4. CI publishes to pyx and creates a GitHub Release
