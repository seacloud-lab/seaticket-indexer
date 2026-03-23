#!/bin/bash

set -e

export PYTHONPATH=$PYTHONPATH:/usr/lib/python3.12/dist-packages:/usr/lib/python3.12/site-packages:/usr/local/lib/python3.12/dist-packages:/usr/local/lib/python3.12/site-packages:/data/dev:/data/dev/seaqa-indexer/

# === Configuration ===
export SEAQA_MYSQL_DB_HOST=xxx
export SEAQA_MYSQL_DB_PORT=3306
export SEAQA_MYSQL_DB_USER=xxx
export SEAQA_MYSQL_DB_PASSWORD=xxx
export SEAQA_MYSQL_DB_NAME=xxx


INDEXER_SCRIPT="/data/dev/seaqa-indexer/seaqa_indexer/index/script/index_local.py"

# ==================== Help ====================
show_help() {
    cat <<EOF
Usage: $0 [options] <command> [arguments]

Commands:
  update    Update index
  clear     Delete index

Options:
  -h, --help                Show this help
  --log-file FILE           Log file path (default: output to terminal)
  --log-level LEVEL         Log level (default: info)

Business arguments）:
  --project-uuid UUID       UUID
  --connection-id ID        ID

Examples:
  $0 --connection-id 25 update
  $0 --project-uuid "19c91227-3b62-47f6-b3db-4db8d6c2c98b" update
  $0 --project-uuid "19c91227-3b62-47f6-b3db-4db8d6c2c98b" --log-file /tmp/clear.log clear

EOF
}


if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
    exit 0
fi

COMMAND=""
for arg in "$@"; do
    if [[ "$arg" == "update" || "$arg" == "clear" ]]; then
        COMMAND="$arg"
        break
    fi
done

if [[ -z "$COMMAND" ]]; then
    echo "Error: The command 'update' or 'clear' must be specified." >&2
    echo "Use -h to view help." >&2
    exit 1
fi


LOG_LEVEL="info"
LOG_FILE=""
TEMP_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --log-file)
            LOG_FILE="$2"
            shift 2
            ;;
        --log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        *)
            TEMP_ARGS+=("$1")
            shift
            ;;
    esac
done

CMD_ARGS=(--loglevel "$LOG_LEVEL")

# Only pass `--logfile` if the user specifies a log file.
if [[ -n "$LOG_FILE" ]]; then
    CMD_ARGS+=(--logfile "$LOG_FILE")
fi

CMD_ARGS+=("${TEMP_ARGS[@]}")

# ==================== Build and execute commands ====================
exec python "$INDEXER_SCRIPT" "${CMD_ARGS[@]}"

