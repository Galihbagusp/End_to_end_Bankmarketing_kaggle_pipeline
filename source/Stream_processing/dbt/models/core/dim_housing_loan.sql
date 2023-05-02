select distinct
    {{ encode_credit_loan('housing') }} as housing_loan_id,
    housing as housing_loan_type
from {{ ref('stg_bank_marketing') }}
order by housing_loan_id asc