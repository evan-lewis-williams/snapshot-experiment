{% snapshot intg_claim_transaction %}

{{
    config(
      tags=["intg"],
      unique_key='claim_transaction_key',
      target_schema='dev_nat',
      strategy='timestamp',
      updated_at='updatetime'
    )
}}

select claim_transaction_key
    ,src
    ,trans_date
    ,trans_set_type
    ,trans_set_userid
    ,trans_status
    ,trans_type
    ,trans_authorised
    ,trans_desc
    ,trans_amount
    ,updatetime
    
from {{ ref('intg_claim_transaction_src_a') }} 
where valid_from is max of valid_from

we are unioning the latest records from each
how do we get the latest records from each table 
-- how do you filter each of these 3 tables correctly

UNION ALL 

select claim_transaction_key
    ,src
    ,trans_date
    ,trans_set_type
    ,trans_set_userid
    ,trans_status
    ,trans_type
    ,trans_authorised
    ,trans_desc
    ,trans_amount
    ,updatetime
    
from {{ ref('intg_claim_transaction_src_b') }} 
{% endsnapshot %}