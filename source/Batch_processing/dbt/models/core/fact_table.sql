{{ config(schema='prod')}}

SELECT
    id,
    day_of_week,
    month,
    age,
    job_id,
    marital_id,
    education_id,
    credit_id,
    housing_loan_id,
    personal_loan_id,
    contact_id,
    duration,
    campaign,
    poutcome,
    emp_var_rate,
    cons_price_idx,
    cons_conf_idx,
    euribor3m,
    nr_employed,
    y as subscribe
FROM
    {{ ref('stg_bank_marketing') }} as stg
    LEFT JOIN {{ ref('dim_contact') }} as ctc ON stg.contact = ctc.contact_type
    LEFT JOIN {{ ref('dim_credit') }} as cr ON stg.default = cr.credit_type
    LEFT JOIN {{ ref('dim_housing_loan') }} as hl ON stg.housing = hl.housing_loan_type
    LEFT JOIN {{ ref('dim_personal_loan') }} as pl ON stg.loan = pl.personal_loan_type
    LEFT JOIN {{ ref('dim_education') }} as edu ON stg.education = edu.education_type
    LEFT JOIN {{ ref('dim_job') }} as job ON stg.job = job.job_type
    LEFT JOIN {{ ref('dim_marital') }} as mrt ON stg.marital = mrt.marital_type
ORDER BY 1