import json
import logging

from seaqa_indexer.workers.base_task_worker import LockingTaskWorker, run_worker
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.ai_processor.ai_processor import (
    generate_summaries_and_vectors,
    process_knowledge_base_ai,
    process_ticket_ai,
    process_portal_issue_ai,
)
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seaqa_db import SeaqaDB


logger = logging.getLogger(__name__)


class AIProcessTaskWorker(LockingTaskWorker):
    def get_queue_name(self) -> str:
        return 'ai_process_task'

    def get_worker_name(self) -> str:
        return 'ai_process_task_worker'

    def get_lock_key(self, task_id) -> str:
        return f'ai_process_{task_id}'

    def init_dependencies(self):
        self.seadb_api = SeaDBAPI()
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)

    def process_message(self, data: dict):
        task_type = data.get('type')
        if task_type == 'knowledge_base':
            project_uuid = data.get('project_uuid')
            self.kb_task_handler(project_uuid)
        elif task_type == 'ticket':
            project_uuid = data.get('project_uuid')
            self.tickets_task_handler(project_uuid)
        elif task_type == 'portal_issue':
            project_uuid = data.get('project_uuid')
            self.portal_issues_task_handler(project_uuid)
        else:
            connection_id = data.get('connection_id')
            self.work_task_handler(connection_id)

    def work_task_handler(self, connection_id):
        self.execute_with_lock(
            connection_id,
            self.process_ai_summary,
            connection_id
        )

    def tickets_task_handler(self, project_uuid):
        self.execute_with_lock(
            f'ticket_{project_uuid}',
            self.process_tickets_ai_summary,
            project_uuid
        )

    def kb_task_handler(self, project_uuid):
        self.execute_with_lock(
            f'kb_{project_uuid}',
            self.process_kb_ai_summary,
            project_uuid
        )

    def portal_issues_task_handler(self, project_uuid):
        self.execute_with_lock(
            f'portal_issue_{project_uuid}',
            self.process_portal_issues_ai_summary,
            project_uuid
        )

    def process_tickets_ai_summary(self, project_uuid):
        try:
            process_ticket_ai(self.seadb_api, project_uuid)

            msg_content = {
                'project_uuid': project_uuid,
                'type': 'ticket',
            }
            self.mq.lpush('summary_index_task', json.dumps(msg_content))
        except Exception as e:
            logger.exception('AI process tickets of project: %s failed, error: %s', project_uuid, e)

    def process_portal_issues_ai_summary(self, project_uuid):
        try:
            process_portal_issue_ai(self.seadb_api, project_uuid)

            msg_content = {
                'project_uuid': project_uuid,
                'type': 'portal_issue',
            }
            self.mq.lpush('summary_index_task', json.dumps(msg_content))
        except Exception as e:
            logger.exception('AI process portal issues of project: %s failed, error: %s', project_uuid, e)

    def process_ai_summary(self, connection_id):
        try:
            generate_summaries_and_vectors(self.seadb_api, self.seaqa_db, self.mq, connection_id)
        except Exception as e:
            logger.exception('AI process connection: %s failed, error: %s', connection_id, e)


    def process_kb_ai_summary(self, project_uuid):
        try:
            process_knowledge_base_ai(self.seadb_api, project_uuid)
            msg_content = {
                'project_uuid': project_uuid,
                'type': 'knowledge_base',
            }
            self.mq.lpush('summary_index_task', json.dumps(msg_content))
        except Exception as e:
            logger.exception('AI process knowledge base of project: %s failed, error: %s', project_uuid, e)


def main():
    run_worker(AIProcessTaskWorker)


if __name__ == "__main__":
    main()
