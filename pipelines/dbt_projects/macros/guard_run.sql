{% macro prevent_full_run() %}

    {# Acessa os argumentos que foram passados no comando CLI #}
    {% set command = invocation_args_dict.get('which', '') %}
    {% set selection = invocation_args_dict.get('select') %}
    {% set selector = invocation_args_dict.get('selector') %}
    
    {# Lógica Simplificada: Se for 'run' e não tiver filtro, MORRE AQUI. #}
    {# Removemos a checagem do 'force_run' #}
    {% if command == 'run' and not selection and not selector %}
        
        {{ exceptions.raise_compiler_error("
        
        ⛔ PARE! AÇÃO PROIBIDA ⛔
        
        O comando 'dbt run' (full refresh) está DESABILITADO permanentemente neste projeto.
        É obrigatório selecionar um subconjunto de modelos para evitar custos/travamentos.
        
        Use:
        -> dbt run --select <modelo_ou_tag>
        -> dbt run --selector <regra_yaml>
        
        ") }}

    {% endif %}

{% endmacro %}