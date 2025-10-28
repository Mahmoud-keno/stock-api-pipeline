{{ config(materialized='table') }}
select 
    dividend_id,
    ticker,
    ex_dividend_date,  -- Already cast in staging, no need to cast again
    pay_date,          -- Already cast in staging, no need to cast again
    record_date,       -- Already cast in staging, no need to cast again
    declaration_date,  -- Already cast in staging, no need to cast again
    dividend_type,
    frequency,
    case 
        when frequency = 1 then 'annual'
        when frequency = 2 then 'semi_annual'
        when frequency = 4 then 'quarterly'
        when frequency = 12 then 'monthly'
        else 'unknown'
    end as dividend_frequency,
    cash_amount,
    currency,
    dividend_lifecycle_stage,
    loaded_at
from {{ ref('stg_stock_dividends') }}