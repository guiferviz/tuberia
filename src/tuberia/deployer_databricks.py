import json

from tuberia.deployer import Deployer
from tuberia.flow import Flow, Table


class DatabricksMultiTaskJob(Deployer):
    """Deploy a flow to a Databricks multi task job.

    Attributes:
        existing_cluster_id: ID of your existing cluster. Job clusters are not
            supported yet.

    """

    existing_cluster_id: str = "0427-152327-dzx1y23l"
    libraries: list = [{"pypi": {"package": "tuberia"}}]

    def run(self, flow: Flow):
        tables = flow.dict_tables()
        obj = self._job_dict(flow, tables)
        print(json.dumps(obj, indent=4))

    def _job_dict(self, flow: Flow, tables: dict):
        return {
            "settings": {
                "name": flow.name,
                "email_notifications": {"no_alert_for_skipped_runs": False},
                "timeout_seconds": 0,
                "max_concurrent_runs": 1,
                "tasks": [self._tasks_dict(flow, i) for i in tables.values()],
                "format": "MULTI_TASK",
            },
        }

    def _tasks_dict(self, flow: Flow, table: Table):
        return {
            "task_key": table.id,
            "depends_on": [{"task_key": i.id} for i in table._dependencies()],
            "python_wheel_task": {
                "package_name": "tuberia",
                "entry_point": "tuberia",
                "parameters": ["run", flow.full_name, "--id", table.id],
            },
            "existing_cluster_id": self.existing_cluster_id,
            "libraries": self.libraries,
            "timeout_seconds": 0,
            "email_notifications": {},
        }
