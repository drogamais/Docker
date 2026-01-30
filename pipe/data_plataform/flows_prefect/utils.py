import sys
import subprocess
from prefect import task
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect.client.schemas.schedules import CronSchedule

@task(retries=1, task_run_name="dbt run: {modelo}")
def dbt_task(modelo: str, project_path: str):
    """Executa modelos dbt com threads limitadas"""
    return trigger_dbt_cli_command(
        command=f"dbt run -s {modelo} --threads 1",
        project_dir=project_path,
        profiles_dir=project_path
    )

@task(retries=1, task_run_name="Python: {script_name}")
def python_task(script_name: str, python_base_path: str):
    """Executa scripts Python genéricos"""
    script_full_path = f"{python_base_path}/{script_name}.py"
    result = subprocess.run(["python", script_full_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erro no script {script_name}: {result.stderr}")
    return result.stdout

@task(name="Debug dbt")
def debug_ambiente(project_path: str):
    """Limpeza e validação do ambiente"""
    trigger_dbt_cli_command(command="dbt clean", project_dir=project_path, profiles_dir=project_path)
    trigger_dbt_cli_command(command="dbt deps", project_dir=project_path, profiles_dir=project_path)
    trigger_dbt_cli_command(command="dbt debug", project_dir=project_path, profiles_dir=project_path)

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