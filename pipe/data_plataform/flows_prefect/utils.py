import sys
import subprocess
import duckdb
import time
from datetime import date
from prefect import task
from prefect.client.schemas.schedules import CronSchedule
from tasks_python.settings.config import DUCKDB_SECRET_SQL, setup_minio_env

def gerenciar_run(pipeline_flow, entrypoint_name, deploy_name, cron_schedule):
    """Gere o deploy com agendamento customizado ou execução local"""
    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        print(f"🚀 Iniciando Deploy: {deploy_name} com Cron: {cron_schedule}")
        pipeline_flow.from_source(
            source="/app/flows_prefect", 
            entrypoint=entrypoint_name
        ).deploy(
            name=deploy_name,
            work_pool_name="process-pool",
            job_variables={"env": {"PYTHONPATH": "/app"}},
            schedules=[
                CronSchedule(cron=cron_schedule, timezone="America/Sao_Paulo")
            ],
        )
    else:
        print("🧪 Executando Flow em modo de TESTE LOCAL...")
        pipeline_flow()

def marcar_sucesso_bronze(sistema: str):
    """Cria um arquivo de flag indicando que a Bronze específica de hoje rodou."""
    setup_minio_env()
    hoje = date.today().strftime('%Y-%m-%d')
    PATH_CONTROLE = "s3://bronze/controle_execucao"
    arquivo_flag = f"{PATH_CONTROLE}/BRONZE_{sistema}_OK_{hoje}.csv"
    
    print(f"🚩 [Controle] Criando flag de sucesso: {arquivo_flag}")
    
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)
        # Cria um arquivo CSV minúsculo apenas para marcar a presença no S3
        con.execute(f"COPY (SELECT 1 as status) TO '{arquivo_flag}' (FORMAT CSV)")
    finally:
        con.close()

@task(retries=1, task_run_name="Python: {script_name}")
def python_task(script_name: str, python_base_path: str):
    """Executa scripts Python genéricos"""
    script_full_path = f"{python_base_path}/{script_name}.py"
    result = subprocess.run(["python", script_full_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erro no script {script_name}: {result.stderr}")
    return result.stdout


@task(name="Aguardar Flag Bronze", retries=3, retry_delay_seconds=60)
def aguardar_bronze(sistema: str, timeout_minutos=60):
    """
    sistema: ex 'acode', 'plugpharma', 'comercial'
    """
    setup_minio_env()
    hoje = date.today().strftime('%Y-%m-%d')
    # O nome do arquivo agora depende do parâmetro 'sistema'
    PATH_CONTROLE = "s3://bronze/controle_execucao"
    arquivo_flag = f"{PATH_CONTROLE}/BRONZE_{sistema}_OK_{hoje}.csv"
    
    print(f"🕵️ Procurando flag: {arquivo_flag}")
    
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(DUCKDB_SECRET_SQL)

    inicio = time.time()
    while True:
        try:
            con.execute(f"SELECT * FROM '{arquivo_flag}'")
            print(f"✅ Bronze {sistema} detectada!")
            break
        except:
            if (time.time() - inicio) / 60 > timeout_minutos:
                raise TimeoutError(f"Timeout: Bronze {sistema} não encontrada.")
            time.sleep(60) 
    con.close()