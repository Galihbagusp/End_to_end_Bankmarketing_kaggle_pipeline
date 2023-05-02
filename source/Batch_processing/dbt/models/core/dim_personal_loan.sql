select distinct
    {{ encode_credit_loan('loan') }} as personal_loan_id,
    loan as personal_loan_type
from {{ ref('stg_bank_marketing') }}
order by personal_loan_id asc