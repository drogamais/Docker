from prefect import flow, task
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect.client.schemas.schedules import CronSchedule

@task(name="Debug dbt", retries=1)
def debug_dbt():
    """Verifica conexão com o Dremio e validade dos profiles"""
    return trigger_dbt_cli_command(
        command="dbt debug",
        project_dir="dbt_projects",
        profiles_dir="dbt_projects"
    )

@task(name="Execucao em Lote dbt", retries=1)
def executar_dbt(lista_modelos: list):
    # Juntamos a lista em uma única string separada por espaços
    comando_final = " ".join(lista_modelos)
    
    return trigger_dbt_cli_command(
        command=f"dbt run --select {comando_final}",
        project_dir="dbt_projects",
        profiles_dir="dbt_projects"
    )

@flow(name="Pipeline Comercial", timeout_seconds=1800)
def pipeline_comercial():
    # 1. Primeiro validamos se tudo está OK
    debug_dbt()

    # Aqui você seleciona "um por um" de forma visual e organizada
    models_escolhidos = [
        "silver_acode_compras_produto_comercial",
        "dim_fornecedor",
        "dim_grupo_subclasse",
        "dim_marca",
        "dim_produto",
        "gold_acode_compras_produto_comercial",
    ]
    
    # Roda todos de uma vez só, mas mantendo a sua lista acima legível
    executar_dbt(models_escolhidos)

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