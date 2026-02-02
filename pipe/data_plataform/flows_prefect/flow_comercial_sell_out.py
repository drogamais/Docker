from prefect import flow
from utils import python_task, gerenciar_run, aguardar_bronze

PY_PATH = "/app/tasks_python/comercial_sell_out/python"

@flow(name="Pipeline Plugpharma Vendas Comercial")
def pipeline():
    # 1. O Prefect primeiro executa esta task e espera ela dar "Success"
    # check_bronze = aguardar_bronze(storage="vendas-plugpharma")

    # 2. As tarefas seguintes esperam explicitamente pela confirmação
    silver = python_task(
        script_name="silver_acode_compras_comercial", 
        python_base_bath=PY_PATH
        # wait_for=[check_bronze]
    )
    
    dim = python_task(
        script_name="dimensoes_acode_comercial", 
        python_base_path=PY_PATH, 
        wait_for=[silver]
    )
    
    python_task(
        script_name="gold_acode_compras_comercial", 
        python_base_path=PY_PATH, 
        wait_for=[dim]
    )

if __name__ == "__main__":
    gerenciar_run(
        pipeline_flow=pipeline,
        entrypoint_name="flow_comercial_sell_out.py:pipeline",
        deploy_name="Pipeline Plugpharma Vendas Comercial",
        cron_schedule="0 9 * * *"
    )