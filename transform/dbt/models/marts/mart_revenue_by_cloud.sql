-- mart_revenue_by_cloud: Compare revenue contribution from each cloud source.
-- Helps validate data completeness across AWS, Azure, GCP ingestion legs.

{{
  config(
    materialized = 'table',
    schema       = 'marts'
  )
}}

with txn as (
    select * from {{ ref('stg_transactions') }}
),

summary as (
    select
        source_cloud,
        txn_year,
        txn_month,
        count(distinct transaction_id)          as total_transactions,
        count(distinct customer_id)             as unique_customers,
        sum(amount)                             as total_revenue,
        avg(amount)                             as avg_order_value,
        sum(case when status = 'completed'
                 then amount else 0 end)        as completed_revenue,
        round(
            sum(case when status = 'completed'
                     then amount else 0 end)
            / nullif(sum(amount), 0) * 100, 2
        )                                       as completion_rate_pct,
        current_timestamp                       as dbt_updated_at

    from txn
    group by 1, 2, 3
)

select * from summary
order by txn_year desc, txn_month desc, source_cloud
