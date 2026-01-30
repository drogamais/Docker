from prefect import flow
from utils import dbt_task, python_task, debug_ambiente, gerenciar_run

DBT_PATH = "/app/dbt/comercial_vermelho"
PY_PATH = "/app/dbt/comercial_vermelho/python"

@flow(name="Pipeline Comercial Vermelho", timeout_seconds=7200)
def pipeline():
    # 0. Debug
    debug_ambiente(project_path=DBT_PATH)
    
    # 1. Silver
    silver = dbt_task(modelo="silver_acode_compras_produto_comercial", project_path=DBT_PATH)

    # 2. Marts
    marts = [
        "dim_fabricante_acode", 
        "dim_fornecedor_acode", 
        "dim_grupo_subclasse_acode", 
        "dim_marca_acode", 
        "dim_produto_acode"
        ] 
     
    for m in marts:
        dbt_task(modelo=m, project_path=DBT_PATH, wait_for=[silver])

    # 3. Gold
    # gold = dbt_task(modelo="gold_acode_compras_produto_comercial", project_path=DBT_PATH, wait_for=[silver])
    dbt_task(script_name="gold_acode_compras_produto_comercial", python_base_path=PY_PATH, wait_for=[silver])

    # 4. Python(correções)
    # python_task(script_name="correcoes_mariadb", python_base_path=PY_PATH, wait_for=[gold])

if __name__ == "__main__":
    gerenciar_run(
        pipeline_flow=pipeline,
        entrypoint_name="flow_comercial_vermelho.py:pipeline",
        deploy_name="Pipeline Comercial Vermelho Sell-In",
        cron_schedule="0 9 * * *"
    )