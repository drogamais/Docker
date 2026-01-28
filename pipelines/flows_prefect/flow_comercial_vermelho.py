from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect.client.schemas.schedules import CronSchedule

@flow(name="Pipeline Comercial", timeout_seconds=1800)
def pipeline_comercial():
    trigger_dbt_cli_command(
        command="dbt run --select silver_acode_compras_produto_comercial",
        project_dir="dbt_projects",
        profiles_dir="dbt_projects"
    )

if __name__ == "__main__":
    pipeline_comercial.from_source(
        source="/app", 
        entrypoint="flows_prefect/flow_comercial_vermelho.py:pipeline_comercial"
    ).deploy(
        name="Pipeline Comercial Vermelho",
        work_pool_name="process-pool",
        schedules=[
            CronSchedule(cron="30 8 * * *", timezone="America/Sao_Paulo")
        ],
    )