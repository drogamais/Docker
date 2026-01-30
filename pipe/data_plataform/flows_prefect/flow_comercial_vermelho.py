import sys
from prefect import flow, task
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect.client.schemas.schedules import CronSchedule
from dbt.comercial_vermelho.python.correcoes_mariadb import executar_correcoes

PATH = "/app/dbt/comercial_vermelho"

@task(retries=1)
def dbt_task(modelo: str):
    """Executa um modelo específico do dbt"""
    return trigger_dbt_cli_command(
        command=f"dbt run -s {modelo} --threads 1",
        project_dir=PATH,
        profiles_dir=PATH,
        task_custom_name=f"dbt run: {modelo}"
    )

@task(name="Debug dbt", retries=1)
def debug_dbt():
    trigger_dbt_cli_command(command="dbt clean", project_dir=PATH, profiles_dir=PATH, name="dbt clean")
    trigger_dbt_cli_command(command="dbt deps", project_dir=PATH, profiles_dir=PATH, name="dbt deps")
    trigger_dbt_cli_command(command="dbt debug",project_dir=PATH,profiles_dir=PATH, name="dbt debug")
    return

@task(name="Corrigir tabela MariaDB (Python)", retries=1)
def executar_python_task():
    return executar_correcoes()

@flow(name="Pipeline Comercial Vermelho Sell-In", timeout_seconds=7200)
def pipeline():
    # 1. Camada Staging
    silver = dbt_task("silver_acode_compras_produto_comercial")

    # 2. Camada Marts
    dbt_task("dim_fabricante_acode", wait_for=[silver])
    dbt_task("dim_fornecedor_acode", wait_for=[silver])
    dbt_task("dim_grupo_subclasse_acode", wait_for=[silver])
    dbt_task("dim_marca_acode", wait_for=[silver])
    dbt_task("dim_produto_acode", wait_for=[silver])
    gold = dbt_task("gold_acode_compras_produto_comercial", wait_for=[silver])

    # 3. Correção final
    executar_python_task(wait_for=[gold])

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        print("Iniciando Deploy para o Prefect Server...")

        pipeline.from_source(
            source="/app/flows_prefect", 
            entrypoint="flow_comercial_vermelho.py:pipeline"
        ).deploy(
            name="Pipeline Comercial Vermelho Sell-In",
            work_pool_name="process-pool",
            job_variables={"env": {"PYTHONPATH": "/app"}},
            schedules=[
                CronSchedule(cron="0 9 * * *", timezone="America/Sao_Paulo")
            ],
        )

    else:
        print("Executando Flow em modo de TESTE LOCAL...")
        pipeline()