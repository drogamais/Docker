{# macros/no_create_schema.sql #}

{% macro create_schema(relation) %}
  {{ log("Ignorando criação do schema " ~ relation, info=True) }}
  
  {# 
     Enviamos um comando inútil (SELECT 1) apenas para abrir uma transação.
     Assim, quando o dbt tentar fazer COMMIT no final, ele vai encontrar
     uma transação válida e não vai dar erro.
  #}
  SELECT 1
{% endmacro %}