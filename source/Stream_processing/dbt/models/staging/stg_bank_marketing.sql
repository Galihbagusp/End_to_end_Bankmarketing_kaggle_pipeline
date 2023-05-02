{{  config(materialized='ephemeral')}}

SELECT
    sha256(concat(

        age,

        job,

        marital,

        education,

        `default`,

        housing,

        loan,

        contact,

        month,

        day_of_week,

        duration,

        campaign,

        pdays,

        previous,

        poutcome,

        emp_var_rate,

        cons_price_idx,

        cons_conf_idx,

        euribor3m,

        nr_employed,

        y)) as id,

        *
FROM
    {{  source('staging','full_raw_streaming')}}