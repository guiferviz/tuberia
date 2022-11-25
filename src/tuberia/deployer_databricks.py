from typing import Optional

from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient
from loguru import logger

from tuberia.deployer import Deployer
from tuberia.flow import Flow, Table


class DatabricksMultiTaskJob(
    Deployer, env_prefix="tuberia_deployer_databricks_multi_task_job_"
):
    """Deploy a flow to a Databricks multi task job.

    Attributes:
        existing_cluster_id: ID of your existing cluster. Job clusters are not
            supported yet.

    """

    databricks_host: Optional[str]
    databricks_token: Optional[str]
    databricks_deploy: bool = False
    existing_cluster_id: str
    libraries: list = [{"pypi": {"package": "tuberia"}}]

    def run(self, flow: Flow):
        tables = flow.dict_tables()
        obj = self._job_dict(flow, tables)
        if self.databricks_deploy:

            api_client = ApiClient(
                host=self.databricks_host,
                token=self.databricks_token,
            )
            jobs_api = JobsApi(api_client)
            job_ids = jobs_api._list_jobs_by_name(flow.name)
            logger.info(
                f"Number of jobs with name `{flow.name}`: {len(job_ids)}"
            )
            if not job_ids:
                logger.info("Creating job")
                jobs_api.create_job(obj)
            elif len(job_ids) == 1:
                job_id = job_ids[0]["job_id"]
                logger.info(f"Updating existing job with id `{job_id}`")
                jobs_api.client.update_job(job_id, obj)
            else:
                raise RuntimeError(f"More than one job with name `{flow.name}`")

    def _job_dict(self, flow: Flow, tables: dict):
        return {
            "name": flow.name,
            "email_notifications": {"no_alert_for_skipped_runs": False},
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "tasks": [self._tasks_dict(flow, i) for i in tables.values()],
            "format": "MULTI_TASK",
        }

    def _tasks_dict(self, flow: Flow, table: Table):
        table_id = table.id.replace(".", "__")
        return {
            "task_key": table_id,
            "depends_on": [
                {"task_key": i.id.replace(".", "__")}
                for i in table._dependencies()
            ],
            "python_wheel_task": {
                "package_name": "tuberia",
                "entry_point": "tuberia",
                "parameters": ["run", flow.full_name, "--id", table_id],
            },
            "existing_cluster_id": self.existing_cluster_id,
            "libraries": self.libraries,
            "timeout_seconds": 0,
            "email_notifications": {},
        }
