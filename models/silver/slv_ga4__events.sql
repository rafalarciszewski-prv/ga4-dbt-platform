{{ config(
    materialized = 'table',
    partition_by = {'field': 'event_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by = ['event_name', 'session_key'],
    labels = {'layer': 'silver', 'source': 'ga4'}
) }}

-- Silver: 1 wiersz = 1 event. Parametry wyciagniete z event_params,
-- typy ujednolicone, klucze eventu i sesji zbudowane.

with src as (
    select *
    from {{ ref('brz_ga4__events') }}
    where event_date between parse_date('%Y%m%d', '{{ var("ga4_start") }}')
                         and parse_date('%Y%m%d', '{{ var("ga4_end") }}')
),

params as (
    select
        event_date,
        event_timestamp,
        event_name,
        user_pseudo_id,
        platform,
        device.category as device_category,
        geo.country     as geo_country,

        -- gdyby w event_params jednego eventu ten sam klucz pojawil si dwukrotnie, to zapytanie wywali model -> rozwiazaniem jest makro, ale makro odlozone; sprawdzone: 0 powtorzonych kluczy na 7 dniach
        -- sesja
        (select value.int_value from unnest(event_params) where key = 'ga_session_id')                                                          as ga_session_id,
        (select value.int_value from unnest(event_params) where key = 'ga_session_number')                                                      as ga_session_number,
        (select value.int_value from unnest(event_params) where key = 'engagement_time_msec')                                                   as engagement_time_msec,
        (select coalesce(safe_cast(value.string_value as int64), value.int_value) from unnest(event_params) where key = 'session_engaged')      as session_engaged,

        -- strona
        (select value.string_value from unnest(event_params) where key = 'page_location')                                                       as page_location,
        (select value.string_value from unnest(event_params) where key = 'page_title')                                                          as page_title,
        (select value.string_value from unnest(event_params) where key = 'page_referrer')                                                       as page_referrer,

        -- zrodlo ruchu
        (select value.string_value from unnest(event_params) where key = 'source')                                                              as source,
        (select value.string_value from unnest(event_params) where key = 'medium')                                                              as medium,
        (select value.string_value from unnest(event_params) where key = 'campaign')                                                            as campaign,
        (select value.string_value from unnest(event_params) where key = 'term')                                                                as term,

        -- e-commerce
        (select cast(coalesce(value.double_value, cast(value.int_value as float64)) as numeric) from unnest(event_params) where key = 'value')  as value,
        (select cast(coalesce(value.double_value, cast(value.int_value as float64)) as numeric) from unnest(event_params) where key = 'tax')    as tax,
        (select coalesce(value.string_value, cast(value.int_value as string)) from unnest(event_params) where key = 'transaction_id')           as transaction_id,
        (select value.string_value from unnest(event_params) where key = 'currency')                                                            as currency,
        (select value.string_value from unnest(event_params) where key = 'payment_type')                                                        as payment_type,
        cast(ecommerce.purchase_revenue as numeric)                                                                                             as purchase_revenue,
        ecommerce.transaction_id                                                                                                                as ecommerce_transaction_id,

        _source_shard
    from src
)

select
    -- event_key: laczy id uzytkownika, timestamp eventu i typ eventu. Dwa te same typy eventow nie moga wystapic u jednego uzytkownika w tej samej mikrosekundzie (sprawdzone na 7 dniach - brak duplikatow na event_key).
    to_hex(md5(concat(user_pseudo_id, '|', cast(event_timestamp as string), '|', event_name)))      as event_key,
    concat(user_pseudo_id, '-', cast(ga_session_id as string))                                      as session_key,
    timestamp_micros(event_timestamp)                                                               as event_ts,
    *
from params