from seaqa_indexer.seasearch.index_master_base import IndexMasterBase


class KnowledgeBaseIndexMaster(IndexMasterBase):
    """
    Index master for knowledge base updates.
    Subscribes to knowledge_base_update and enqueues tasks to both index_task and ai_process_task.
    """
    def __init__(self):
        super().__init__(
            channel='knowledge_base_update',
            message_type='knowledge_base',
            task_queues=['index_task', 'ai_process_task']
        )
