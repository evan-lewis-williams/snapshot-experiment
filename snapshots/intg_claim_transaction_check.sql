{% snapshot intg_claim_transaction_check %}

{{
    config(
      tags=["intgcheck"],
      unique_key='claim_transaction_key',
      target_schema='dev_evan',
      strategy='check',
      check_cols=['ts_updatetime', 't_updatetime', 'tl_updatetime'],
      updated_at='updatetime'
    )
}}

select concat(ts.id, '~', t.id, '~', tl.id) as claim_transaction_key
    , ts.trandate   as trans_date
    , ts.transet    as trans_set_type
    , ts.userid     as trans_set_userid
    , ts.updatetime as ts_updatetime

    , t."type"      as trans_type
    , t.auth        as trans_authorised
    , t.updatetime  as t_updatetime

    , tl."desc"     as trans_desc
    , tl."amount"   as trans_amount
    , tl.updatetime as tl_updatetime
    , greatest(ts.dbt_updated_at, t.dbt_updated_at, tl.dbt_updated_at) as updatetime
    
from {{ ref('source_cc_transaction_set') }} ts 
  left join {{ ref('source_cc_transaction') }} t on ts.id = t.transetid
  left join {{ ref('source_cc_transaction_line') }} tl on t.id = tl.tranid
where '2021-07-04 23:59:59' between ts.dbt_valid_from and coalesce(ts.dbt_valid_to,'9999-12-31 23:59:59')
  and '2021-07-04 23:59:59' between t.dbt_valid_from  and coalesce(t.dbt_valid_to,'9999-12-31 23:59:59')
  and '2021-07-04 23:59:59' between tl.dbt_valid_from and coalesce(tl.dbt_valid_to,'9999-12-31 23:59:59')

{% endsnapshot %}