{{config(materialized='view')}}
select
    ticker,
    sum(cash_amount) over(partition by ticker, year(pay_date), record_date) as total_dividend_amount_of_year,
    avg(cash_amount) over(partition by ticker, year(pay_date), record_date) as average_dividend_amount_of_year,
    max(pay_date) over(partition by ticker, year(pay_date), record_date) as last_pay_date_of_year,
    min(pay_date) over(partition by ticker, year(pay_date), record_date) as first_pay_date_of_year
from {{ ref('dim_dividends') }}