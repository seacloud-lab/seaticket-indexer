#!/bin/bash

set -e

export PYTHONPATH=$PYTHONPATH:/opt/seaticket/seaqa-indexer/

MANAGE_COLUMN_SCRIPT="/opt/seaticket/seaqa-indexer/seaqa_indexer/script/manage_seadb_column.py"

# ==================== Help ====================
show_help() {
    cat <<EOF
Usage: $0 <command> [options] [arguments]

Commands:
  add-column        Add column
  delete-column     Delete column
  update-column-format
                    Update column date format


Options:
  -h, --help                Show this help
  --log-file FILE           Log file path (default: output to terminal)
  --log-level LEVEL         Log level (default: info)


BUSINESS_ARGS (for all commands):
  --table-name NAME                Name of the SeaDB table. Supports '*' wildcard
                                   (e.g. 'linear_issue_*', quote it to avoid shell glob
                                   expansion). Wildcard patterns never match sub tables
                                   such as linear_issue_comments_*.
  --column-name column_name        Name of the target column.


BUSINESS_ARGS (only for 'add-column'):
  --column-type           TYPE             column's type
  --column-data-name      NAME             Optional configuration for the column (e.g., text_compressed).
  --need-add-index        VALUE            Whether to create an index. Value can be true/false, 1/0, etc.


BUSINESS_ARGS (only for 'update-column-format'):
  --format                FORMAT           Date format stored in column data.


Examples:
  # Add a compressed text column
  $0 add-column --table-name tickets --column-name test1 --column-type text --column-data-name text_compressed


  # Add a column with an index
  $0 add-column --table-name tickets --column-name test1 --column-type text --need-add-index true


  # Add a column to all linear issue main tables via wildcard (sub tables like
  # linear_issue_comments_* are excluded automatically; quote the pattern)
  $0 add-column --table-name 'linear_issue_*' --column-name state_type --column-type text


  # Delete a column
  $0 delete-column --table-name tickets --column-name test1 --log-file /tmp/alter_column.log


  # Mark a datetime column as a calendar date
  $0 update-column-format --table-name tickets --column-name due_date --format YYYY-MM-DD


EOF
}


if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
    exit 0
fi


COMMAND="$1"


if [[ "$COMMAND" != "add-column" && "$COMMAND" != "delete-column" && "$COMMAND" != "update-column-format" ]]; then
    echo "Error: The first argument must be 'add-column', 'delete-column', or 'update-column-format'." >&2
    echo "Use -h to view help." >&2
    exit 1
fi

shift 1


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


# ==================== Build and execute commands ====================
CMD_ARGS=("$COMMAND" "${TEMP_ARGS[@]}" --loglevel "$LOG_LEVEL")


if [[ -n "$LOG_FILE" ]]; then
    CMD_ARGS+=(--logfile "$LOG_FILE")
fi
echo ${CMD_ARGS[@]}
exec python "$MANAGE_COLUMN_SCRIPT" "${CMD_ARGS[@]}"
