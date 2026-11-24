{{ config(
    materialized = 'table',
    partition_by = {'field': 'transaction_date', 'data_type': 'date', 'granularity': 'day'},
    labels = {'layer': 'silver', 'source': 'ga4'}
) }}

-- Silver: 1 wiersz = 1 transakcja (transaction_id). Deduplikacja podwojnie wyslanych purchase.

with purchases as (
    select
        transaction_id,
        event_date  as transaction_date,
        event_ts as transaction_ts,
        session_key,
        event_key, 
        user_pseudo_id,
        value as revenue,   -- value zamiast ecommerce.purchase_revenue: w probce purchase_revenue jest zaokraglony do pelnych jednostek
        tax, 
        currency,
        count(*) over (partition by transaction_id)  as purchase_event_count
    from {{ ref('slv_ga4__events') }}
    where event_name = 'purchase'
)

select *
from purchases
qualify row_number() over (partition by transaction_id order by transaction_ts, event_key) = 1