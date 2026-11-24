-- Test uzgodnienia: sesje vs eventy i transakcje.
-- Test singular przechodzi, gdy zapytanie zwraca 0 wierszy; kazdy zwrocony wiersz = blad.
with sessions as (
    select
        count(*)              as session_count,
        sum(transactions)     as transaction_count,
        sum(session_revenue)  as revenue
    from {{ ref('slv_ga4__sessions') }}
),

events as (
    select count(distinct session_key) as session_count
    from {{ ref('slv_ga4__events') }}
),

transactions as (
    select
        count(*)       as transaction_count,
        sum(revenue)   as revenue
    from {{ ref('slv_ga4__transactions') }}
)

select
    s.session_count       as sessions_in_model,
    e.session_count       as sessions_in_events,
    s.transaction_count   as transactions_in_sessions,
    t.transaction_count   as transactions_in_model,
    s.revenue             as revenue_in_sessions,
    t.revenue             as revenue_in_transactions
from sessions s
cross join events e
cross join transactions t
where s.session_count     != e.session_count
   or s.transaction_count != t.transaction_count
   or s.revenue           != t.revenue