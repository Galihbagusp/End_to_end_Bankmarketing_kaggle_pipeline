select distinct
    {{ encode_credit_loan('`default`') }} as credit_id,
    `default` as credit_type
from {{ ref('stg_bank_marketing') }}
order by credit_id asc