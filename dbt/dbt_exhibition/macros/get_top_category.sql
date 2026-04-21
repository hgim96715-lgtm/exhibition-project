{% macro get_top_category(column_map)%}
    (
        SELECT category_name
        FROM(
            VALUES
                {% for label,col in column_map.items() %}
                    ('{{label}}', COALESCE({{col}},0)){{ "," if not loop.last }}
                {% endfor %}
        ) AS v(category_name, category_value)
        ORDER BY category_value DESC
        LIMIT 1
    )
{% endmacro %}