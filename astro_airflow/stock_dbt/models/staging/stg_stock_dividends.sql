{{ config(materialized='table') }}

with deduplicated as(
    select *,
    row_number() over(
        partition by id 
        order by record_date desc
    ) as rn
    from {{ source('raw_stock_dividends', 'dividends') }}
    where id is not null
), structred_data as (
select 
    id as dividend_id,
    upper(trim(ticker)) as ticker,
    try_cast(ex_dividend_date as date) as ex_dividend_date,  -- Changed to try_cast
    try_cast(pay_date as date) as pay_date,                  -- Changed to try_cast
    try_cast(record_date as date) as record_date,            -- Changed to try_cast
    try_cast(declaration_date as date) as declaration_date,  -- Changed to try_cast
    dividend_type,
    try_cast(frequency as int) as frequency,                 -- Changed to try_cast
    try_cast(cash_amount as float) as cash_amount,           -- Changed to try_cast
    upper(trim(currency)) as currency,
    current_timestamp as loaded_at
from deduplicated
where rn = 1
), final_data as(
    select 
       *,
        case
            when pay_date is not null and pay_date <= current_date() then 'completed'
            when ex_dividend_date <= current_date() and pay_date > current_date() then 'awaiting_payment'
            when ex_dividend_date > current_date() then 'upcoming'
            when declaration_date is null then 'announced'
            else 'in_progress'
        end as dividend_lifecycle_stage
    from structred_data
)
select * from final_data