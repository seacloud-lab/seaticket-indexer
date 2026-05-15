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


Options:
  -h, --help                Show this help
  --log-file FILE           Log file path (default: output to terminal)
  --log-level LEVEL         Log level (default: info)


BUSINESS_ARGS (for both 'add-column' and 'delete-column'):
  --table-name NAME                Name of the SeaDB table.
  --table-type TYPE                Type of the table (project or connection).
  --column-name column_name        Name of the column to add/delete.


BUSINESS_ARGS (only for 'add-column'):
  --column-type           TYPE             column's type
  --column-data-name      NAME             Optional configuration for the column (e.g., text_compressed).
  --need-add-index        VALUE            Whether to create an index. Value can be true/false, 1/0, etc.


Examples:
  # Add a compressed text column
  $0 add-column --table-name tickets --table-type project --column-name test1 --column-type text --column-data-name text_compressed


  # Add a column with an index
  $0 add-column --table-name tickets --table-type project --column-name test1 --column-type text --need-add-index true


  # Delete a column
  $0 delete-column --table-name tickets --table-type project --column-name test1 --log-file /tmp/alter_column.log


EOF
}


if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
    exit 0
fi


COMMAND="$1"


if [[ "$COMMAND" != "add-column" && "$COMMAND" != "delete-column" ]]; then
    echo "Error: The first argument must be the command 'add-column' or 'delete-column'." >&2
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
