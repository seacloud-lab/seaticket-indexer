#!/bin/bash

set -e

export PYTHONPATH=$PYTHONPATH:/usr/lib/python3.12/dist-packages:/usr/lib/python3.12/site-packages:/usr/local/lib/python3.12/dist-packages:/usr/local/lib/python3.12/site-packages:/data/dev:/data/dev/seaqa-indexer/


INDEXER_SCRIPT="/data/dev/seaqa-indexer/seaqa_indexer/index/script/index_local.py"

# ==================== Help ====================
show_help() {
    cat <<EOF
Usage: $0 [options] <command> [arguments]

Commands:
  update    Update index for a specific project
  update-all Update index for all projects
  clear     Delete index

Options:
  -h, --help                        Show this help
  --log-file FILE                   Log file path (default: output to terminal)
  --log-level LEVEL                 Log level (default: info)
  --db-host <mysql_addr>            MySQL host (default: SEAQA_MYSQL_DB_HOST in env or yaml)
  --db-port <mysql_port>            MySQL port (default: SEAQA_MYSQL_DB_PORT in env or yaml)
  --db-user <mysql_user>            MySQL user (default: SEAQA_MYSQL_DB_USER in env or yaml)
  --db-password <mysql_password>    MySQL password (default: SEAQA_MYSQL_DB_PASSWORD in env or yaml)
  --db-name <db_name>               MySQL database name (default: SEAQA_MYSQL_DB_NAME in env or yaml)
  --s3-host <s3_host>               S3 host (default: S3_HOST in env or yaml)
  --s3-file-bucket <bucket>         S3 file bucket (default: S3_FILE_BUCKET in env or yaml)
  --s3-web-crawl-bucket <bucket>    S3 web crawl bucket (default: S3_WEB_CRAWL_BUCKET in env or yaml)
  --s3-key-id <key_id>              S3 key ID (default: S3_KEY_ID in env or yaml)
  --s3-secret-key <secret_key>      S3 secret key (default: S3_SECRET_KEY in env or yaml)
  --seasearch-url <url>             SeaSearch URL (default: SEASEARCH_URL in env or yaml)
  --seasearch-token <token>         SeaSearch token (default: SEASEARCH_TOKEN in env or yaml)
  --seadb-server-url <url>          SeaDB server URL (default: SEADB_SERVER_URL in env or yaml)
  --seadb-server-access-token <token> SeaDB server access token (default: SEADB_SERVER_ACCESS_TOKEN in env or yaml)

Business arguments for update/clear commands:
  --project-uuid UUID               UUID
  --connection-id ID                ID

Optional business argument for update-all command:
  --threads                         Number of worker threads during updating

Examples:
  $0 --connection-id 25 update
  $0 --project-uuid "19c91227-3b62-47f6-b3db-4db8d6c2c98b" update
  $0 --project-uuid "19c91227-3b62-47f6-b3db-4db8d6c2c98b" --log-file /tmp/clear.log clear
  $0 --threads 4 update-all

EOF
}


if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
    exit 0
fi

COMMAND=""
for arg in "$@"; do
    if [[ "$arg" == "update" || "$arg" == "clear" || "$arg" == "update-all" ]]; then
        COMMAND="$arg"
        break
    fi
done

if [[ -z "$COMMAND" ]]; then
    echo "Error: The command 'update' or 'clear' or 'update-all' must be specified." >&2
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
        --db-host)
            export SEAQA_MYSQL_DB_HOST="$2"
            shift 2
            ;;
        --db-port)
            export SEAQA_MYSQL_DB_PORT="$2"
            shift 2
            ;;
        --db-user)
            export SEAQA_MYSQL_DB_USER="$2"
            shift 2
            ;;
        --db-password)
            export SEAQA_MYSQL_DB_PASSWORD="$2"
            shift 2
            ;;
        --db-name)
            export SEAQA_MYSQL_DB_NAME="$2"
            shift 2
            ;;
        --s3-host)
            export S3_HOST="$2"
            shift 2
            ;;
        --s3-file-bucket)
            export S3_FILE_BUCKET="$2"
            shift 2
            ;;
        --s3-web-crawl-bucket)
            export S3_WEB_CRAWL_BUCKET="$2"
            shift 2
            ;;
        --s3-key-id)
            export S3_KEY_ID="$2"
            shift 2
            ;;
        --s3-secret-key)
            export S3_SECRET_KEY="$2"
            shift 2
            ;;
        --seasearch-url)
            export SEASEARCH_URL="$2"
            shift 2
            ;;
        --seasearch-token)
            export SEASEARCH_TOKEN="$2"
            shift 2
            ;;
        --seadb-server-url)
            export SEADB_SERVER_URL="$2"
            shift 2
            ;;
        --seadb-server-access-token)
            export SEADB_SERVER_ACCESS_TOKEN="$2"
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

