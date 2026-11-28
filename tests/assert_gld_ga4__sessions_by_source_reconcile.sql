-- Test uzgodnienia: model sesji z Time vs silver.
-- Kazdy event i kazda transakcja musza trafic do dokladnie jednej sesji, przychod zgodny do grosza.
-- Test singular przechodzi, gdy zapytanie zwraca 0 wierszy.
with gold as (
    select
        sum(event_count)      as events,
        sum(transactions)     as transactions,
        sum(session_revenue)  as revenue
    from {{ ref('gld_ga4__sessions_by_source') }}
),

events as (
    select count(*) as events
    from {{ ref('slv_ga4__events') }}
),

txn as (
    select
        count(*)      as transactions,
        sum(revenue)  as revenue
    from {{ ref('slv_ga4__transactions') }}
)

select
    g.events        as events_in_gold,
    e.events        as events_in_silver,
    g.transactions  as transactions_in_gold,
    t.transactions  as transactions_in_silver,
    g.revenue       as revenue_in_gold,
    t.revenue       as revenue_in_silver
from gold g
cross join events e
cross join txn t
where g.events       != e.events
   or g.transactions != t.transactions
   or g.revenue      != t.revenue