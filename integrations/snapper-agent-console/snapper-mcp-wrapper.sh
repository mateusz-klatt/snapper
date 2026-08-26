#!/bin/sh
# snapper-mcp launcher: inject the compose-mounted PAT config without breaking
# the CLI's subcommand dispatch (argv[0] must stay check|watch) and without
# letting an explicit --base-url silently discard the PAT token. Only an
# explicit --config suppresses the default.
PAT=/run/secrets/snapper-mcp/config.json
MCP="node /usr/local/lib/snapper-mcp/dist/index.js"

explicit_config=0
for arg in "$@"; do
    case "$arg" in
        --config|--config=*) explicit_config=1 ;;
    esac
done

if [ "$explicit_config" -eq 1 ] || [ ! -r "$PAT" ]; then
    exec $MCP "$@"
fi

case "$1" in
    check|watch)
        sub="$1"; shift
        exec $MCP "$sub" --config "$PAT" "$@"
        ;;
    *)
        exec $MCP --config "$PAT" "$@"
        ;;
esac
