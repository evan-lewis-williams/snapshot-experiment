{{
    config(
        materialized='incremental',
        unique_key='sk'
    )
}}


WITH source_a AS (
  SELECT *,
  concat(id,dbt_valid_from) as sk
 FROM {{ ref('source_cc_transaction_line_src_a') }}
    {% if is_incremental() %}
    where updatetime >= (select max(updatetime)::timestamp from {{this}})
  {% endif %}
),

source_b AS (
  SELECT *,
  concat(id,dbt_valid_from) as sk
FROM {{ ref('source_cc_transaction_line_src_b') }}
 {% if is_incremental() %}
    where updatetime > (select max(updatetime)::timestamp from {{this}})
  {% endif %}
)

SELECT * FROM source_a
UNION 
SELECT * FROM source_b