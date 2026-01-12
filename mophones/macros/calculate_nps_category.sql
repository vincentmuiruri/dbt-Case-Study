-- Macro: Reusable function to categorize NPS scores
-- Usage: {{ calculate_nps_category('column_name') }}

{% macro calculate_nps_category(score_column) %}
    case
        when {{ score_column }} >= {{ var('nps_promoter_min') }} then 'Promoter'
        when {{ score_column }} >= {{ var('nps_passive_min') }} then 'Passive'
        when {{ score_column }} is not null then 'Detractor'
        else 'No Response'
    end
{% endmacro %}