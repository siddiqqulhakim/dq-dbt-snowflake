{% macro log_test_results() %}

  {% if execute and results %}

    {% set test_results = [] %}

    {% for res in results %}
      {% if res.node and res.node.resource_type == 'test' %}

        {% set test_name = res.node.name %}

        {% set refs = res.node.refs %}
        {% set model_name = refs[0]['name'] if refs | length > 0 else 'unknown' %}

        {% set column_name = res.node.column_name if res.node.column_name else 'N/A' %}
        {% set test_type = res.node.test_metadata.name if res.node.test_metadata else 'custom' %}

        {% set parts = model_name.lower().split('__') %}
        {% set layer  = parts[0] if parts | length >= 1 else 'unknown' %}
        {% set domain = parts[1] if parts | length >= 2 else 'unknown' %}

        {% set status    = res.status %}
        {% set failures  = res.failures if res.failures else 0 %}
        {% set exec_time = res.execution_time %}

        {% do test_results.append({
            'test_name':  test_name,
            'model_name': model_name,
            'column_name': column_name,
            'status':     status,
            'failures':   failures,
            'exec_time':  exec_time,
            'test_type':  test_type,
            'layer':      layer,
            'domain':     domain
        }) %}

      {% endif %}
    {% endfor %}

    {% if test_results | length > 0 %}

      {% set insert_sql %}
        INSERT INTO DEMO_ENTERPRISE_DBT_DB.PUBLIC.DBT_TEST_RESULTS
            (test_name, model_name, column_name, status, failures, execution_time_s,
             test_type, layer, domain, run_started_at)
        VALUES
        {% for row in test_results %}
          (
            '{{ row.test_name | replace("'", "''") }}',
            '{{ row.model_name | replace("'", "''") }}',
            '{{ row.column_name | replace("'", "''") }}',
            '{{ row.status }}',
            {{ row.failures }},
            {{ row.exec_time }},
            '{{ row.test_type }}',
            '{{ row.layer }}',
            '{{ row.domain }}',
            '{{ run_started_at }}'
          ){% if not loop.last %},{% endif %}
        {% endfor %}
      {% endset %}

      {% do run_query(insert_sql) %}

    {% endif %}

  {% endif %}

{% endmacro %}
