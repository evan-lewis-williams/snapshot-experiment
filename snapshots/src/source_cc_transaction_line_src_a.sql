{% snapshot source_cc_transaction_line_src_a %}

{{
    config(
      tags=["source"],
      unique_key='id',
      target_schema='dev_nat',
      strategy='timestamp',
      updated_at='updatetime'
    )
}}

select * from {{ source('dev_nat', 'cc_transaction_line_src_a') }}

{% endsnapshot %}