-- stg_transactions: Staged transactions from Silver Delta Lake
-- Exposed via Redshift Spectrum external table → dbt model

{{
  config(
    materialized = 'view',
    schema       = 'staging'
  )
}}

with source as (
    select * from {{ source('silver', 'transactions') }}
),

cleaned as (
    select
        transaction_id,
        customer_id,
        product_id,
        cast(amount    as numeric(18, 2))  as amount,
        cast(txn_date  as date)            as txn_date,
        txn_year,
        txn_month,
        lower(trim(status))                as status,
        _cloud                             as source_cloud,
        _ingested_at                       as ingested_at

    from source
    where transaction_id is not null
      and amount         > 0
      and txn_date       is not null
)

select * from cleaned
