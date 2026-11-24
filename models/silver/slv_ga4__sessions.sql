{{ config(
    materialized = 'table',
    partition_by = {'field': 'session_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by = ['session_source', 'session_medium'],
    labels = {'layer': 'silver', 'source': 'ga4'}
) }}

-- Silver: 1 wiersz = 1 sesja (session_key = user_pseudo_id + ga_session_id).
-- Start sesji = pierwszy event (session_start brakuje w ok. 1,2% sesji).
-- Zrodlo (regula B): pierwsza para source/medium bez wlasnych domen i bez (direct);
-- (direct) tylko gdy w sesji nie ma innego zrodla; brak zrodla = '(not set)'.
-- Przychod z slv_ga4__transactions (po deduplikacji podwojnych purchase).
-- Remisy czasu rozstrzyga event_key, zeby kazdy build dawal ten sam wynik.

with events as (
    select
        session_key, event_key, user_pseudo_id, ga_session_id, ga_session_number,
        event_date, event_ts, event_name,
        session_engaged, engagement_time_msec,
        page_location, device_category, geo_country,
        source, medium, campaign,
        exists(
            select 1 from unnest({{ var('ga4_own_domains') }}) as d
            where lower(source) = d or ends_with(lower(source), concat('.', d))
        )                                   as is_own_domain,
        lower(source) = '(direct)'          as is_direct
    from {{ ref('slv_ga4__events') }}
),

sessions as (
    select
        session_key,

        -- identyfikatory: stale w sesji (profil: 0 konfliktow)
        any_value(user_pseudo_id)                                     as user_pseudo_id,
        any_value(ga_session_id)                                      as ga_session_id,
        any_value(ga_session_number)                                  as ga_session_number,

        -- czas: start = pierwszy event, nie event session_start
        min(event_date)                                               as session_date,
        min(event_ts)                                                 as session_start_ts,
        max(event_ts)                                                 as session_end_ts,
        timestamp_diff(max(event_ts), min(event_ts), second)          as duration_sec,

        -- aktywnosc
        count(*)                                                      as event_count,
        countif(event_name = 'page_view')                             as page_view_count,
        countif(event_name = 'session_start')                         as session_start_count,
        coalesce(max(session_engaged), 0) = 1                         as is_engaged,
        coalesce(sum(engagement_time_msec), 0)                        as engagement_time_msec,

        -- kontekst sesji
        array_agg(page_location ignore nulls order by event_ts, event_key limit 1)[safe_offset(0)]  as landing_page,
        any_value(device_category)                                    as device_category,
        array_agg(geo_country ignore nulls order by event_ts, event_key limit 1)[safe_offset(0)]    as geo_country,

        -- zrodlo sesji (regula B): najpierw pierwsza para bez wlasnych domen i bez (direct),
        -- a jesli takiej nie ma - pierwsza para bez wlasnych domen (zostaje wtedy juz tylko (direct))
        coalesce(
            array_agg(
                if(source is not null and not is_own_domain and not is_direct, struct(source, medium, campaign), null)
                ignore nulls order by event_ts, event_key limit 1
            )[safe_offset(0)],
            array_agg(
                if(source is not null and not is_own_domain, struct(source, medium, campaign), null)
                ignore nulls order by event_ts, event_key limit 1
            )[safe_offset(0)]
        )                                                             as src,
        -- zrodlo surowe: pierwsza niepusta para bez zadnego filtra (do audytu '(not set)')
        array_agg(
            if(source is not null, struct(source, medium), null)
            ignore nulls order by event_ts, event_key limit 1
        )[safe_offset(0)]                                             as src_raw
    from events
    group by session_key
),

txn as (
    select
        session_key,
        count(*)       as transactions,
        sum(revenue)   as revenue
    from {{ ref('slv_ga4__transactions') }}
    group by session_key
),

final as (
    select
        s.* except (src, src_raw),
        coalesce(s.src.source,   '(not set)')   as session_source,
        coalesce(s.src.medium,   '(not set)')   as session_medium,
        coalesce(s.src.campaign, '(not set)')   as session_campaign,
        s.src_raw.source                        as first_source_raw,
        s.src_raw.medium                        as first_medium_raw,
        -- sesja bez zakupu = 0, nie NULL: srednia na sesje musi liczyc tez sesje bez zakupu
        coalesce(t.transactions, 0)             as transactions,
        coalesce(t.revenue, 0)                  as session_revenue
    from sessions s
    left join txn t using (session_key)
)

select * from final