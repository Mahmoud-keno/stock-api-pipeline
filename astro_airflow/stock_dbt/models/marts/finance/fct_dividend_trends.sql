{{config(
    materialized='table'
)}}
with calculate_total_cashes as (
  select
      ticker,
      year(pay_date) as year,
      sum(cash_amount) as total_cashes
  from polygon.dim_dividends
  group by 1,2
),
    top_10 as (
select
    ticker,
    year,
    total_cashes as total_cashes_current_year,
    lag(total_cashes) over (
        partition by ticker
        order by year
    ) as total_cashes_previous_year,
    round(total_cashes
      - coalesce(lag(total_cashes) over (partition by ticker order by year), 0),2)
      as year_over_year_change
from calculate_total_cashes
 order by YEAR_OVER_YEAR_CHANGE desc
limit 10)
select * from top_10
-- fct_dividend_trends.sql get a top 10 also