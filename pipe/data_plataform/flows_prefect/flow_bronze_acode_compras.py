from prefect import flow
from utils import python_task, gerenciar_run

PY_PATH = "/app/tasks_python/bronze/python"

@flow(name="Bronze Acode Compras", timeout_seconds=7200)
def pipeline():

    python_task(
        script_name="bronze_acode_compras", 
        python_base_path=PY_PATH
    )

if __name__ == "__main__":
    gerenciar_run(
        pipeline_flow=pipeline,
        entrypoint_name="flow_bronze_acode_compras.py:pipeline",
        deploy_name="Pipeline Bronze Acode Compras",
        cron_schedule="0 9 * * *"
    )