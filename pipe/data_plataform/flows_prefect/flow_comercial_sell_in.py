from prefect import flow
from utils import python_task, gerenciar_run, aguardar_bronze

PY_PATH = "/app/tasks_python/comercial_sell_in/python"

@flow(name="Pipeline Acode Compras Comercial")
def pipeline():
    # 1. O Prefect primeiro executa esta task e espera ela dar "Success"
    check_bronze = aguardar_bronze(sistema="compras-acode")

    # 2. As tarefas seguintes esperam explicitamente pela confirmação
    silver = python_task(
        script_name="silver_acode_compras_comercial", 
        python_base_bath=PY_PATH,
        wait_for=[check_bronze] # <--- Referência de dependência
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
        entrypoint_name="flow_comercial_sell_in.py:pipeline",
        deploy_name="Pipeline Acode Compras Comercial",
        cron_schedule="0 9 * * *"
    )