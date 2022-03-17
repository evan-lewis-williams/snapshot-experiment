SELECT 
    * 
    ,row_number() over (
    partition by claim_transaction_key
    order by updatetime
) as version_no
FROM {{ ref('intg_claim_transaction') }}