from seaqa_indexer.seasearch.index_master_base import IndexMasterBase


class TicketIndexMaster(IndexMasterBase):
    """
    Index master for ticket updates.
    Listens on the ticket_update channel and adds tasks to index_task and ai_process_task.
    """
    def __init__(self):
        super().__init__(
            channel='ticket_update',
            message_type='ticket',
            task_queues=['index_task', 'ai_process_task']
        )
        