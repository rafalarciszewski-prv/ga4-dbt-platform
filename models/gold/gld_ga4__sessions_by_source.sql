{{ config(
    materialized = 'table',
    partition_by = {'field': 'session_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by = ['session_source', 'session_medium'],
    labels = {'layer': 'gold', 'source': 'ga4'}
) }}

-- Gold: sesje wg reguly z Time S.A. (Bank casow B10). 1 wiersz = 1 sesja w obrebie doby uzytkownika.
-- Wszystkie eventy uzytkownika z jednego event_date to jeden ciag, ponad granicami sesji GA4.
-- Nowa sesja zaczyna sie od pierwszego eventu dnia albo od odslony-wejscia (wariant C):
--   page_view z referrerem spoza wlasnych domen,
--   para source/medium z listy priorytetowej (var gld_priority_sources) albo (direct) na stronie glownej (var gld_homepages),
--   i para inna niz na poprzedniej odslonie dnia LUB pierwsza odslona nowej sesji GA4.
-- Czas przerwy nie ma znaczenia. Granica doby = swiadomy kompromis: sesje sumuja sie miedzy dniami.

with events as (
    select
        event_key,
        user_pseudo_id,
        event_date,
        event_ts,
        event_name,
        session_key                                               as ga4_session_key,
        source,
        medium,
        campaign,
        page_location,
        lower(concat(source, ' / ', coalesce(medium, '(none)')))  as pair,
        -- referrer z wlasnej domeny = klik wewnatrz serwisu, nigdy nowe wejscie
        exists(
            select 1 from unnest({{ var('ga4_own_domains') }}) as d
            where lower(net.host(page_referrer)) = d
               or ends_with(lower(net.host(page_referrer)), concat('.', d))
        )                                                         as is_internal_referrer,
        exists(
            select 1 from unnest({{ var('ga4_own_domains') }}) as d
            where lower(source) = d or ends_with(lower(source), concat('.', d))
        )                                                         as is_own_source,
        coalesce(lower(source) = '(direct)', false)               as is_direct,
        coalesce(
            lower(regexp_replace(page_location, r'[?#].*$', '')) in unnest({{ var('gld_homepages') }}),
            false
        )                                                         as is_homepage
    from {{ ref('slv_ga4__events') }}
),

page_views as (
    select
        event_key,
        pair,
        -- kandydat na wejscie: odslona spoza serwisu z priorytetowym zrodlem albo (direct) na stronie glownej
        not is_internal_referrer
            and (coalesce(pair in unnest({{ var('gld_priority_sources') }}), false)
                 or (is_direct and is_homepage))                  as is_entry_candidate,
        -- para poprzedniej odslony w dniu uzytkownika ('-' = odslona bez zrodla, NULL = pierwsza odslona dnia)
        lag(coalesce(pair, '-')) over (
            partition by user_pseudo_id, event_date order by event_ts, event_key
        )                                                         as prev_pv_pair,
        row_number() over (
            partition by ga4_session_key order by event_ts, event_key
        )                                                         as pv_nr_in_ga4_session
    from events
    where event_name = 'page_view'
),

triggers as (
    select event_key
    from page_views
    where is_entry_candidate
      and (prev_pv_pair is null                    -- pierwsza odslona dnia
           or prev_pv_pair != coalesce(pair, '-')  -- zrodlo zmienilo sie
           or pv_nr_in_ga4_session = 1)            -- GA4 wykryl nowe wejscie
),

flagged as (
    select
        e.*,
        t.event_key is not null  as is_trigger
    from events e
    left join triggers t using (event_key)
),

ordered as (
    select
        *,
        -- przy remisie czasu wejscie idzie pierwsze: session_start i first_visit z tym samym czasem trafiaja do nowej sesji
        row_number() over (
            partition by user_pseudo_id, event_date
            order by event_ts, is_trigger desc, event_key
        )                                                         as event_nr_in_day
    from flagged
),

numbered as (
    select
        *,
        -- gaps and islands: suma narastajaca flagi "nowa sesja" = numer sesji w dniu
        sum(if(is_trigger or event_nr_in_day = 1, 1, 0)) over (
            partition by user_pseudo_id, event_date
            order by event_ts, is_trigger desc, event_key
            rows between unbounded preceding and current row
        )                                                         as session_nr
    from ordered
),

sessions as (
    select
        user_pseudo_id,
        event_date                                                as session_date,
        session_nr,
        min(event_ts)                                             as session_start_ts,
        max(event_ts)                                             as session_end_ts,
        timestamp_diff(max(event_ts), min(event_ts), second)      as duration_sec,
        count(*)                                                  as event_count,
        countif(event_name = 'page_view')                         as page_view_count,
        count(distinct ga4_session_key)                           as ga4_session_count,
        array_agg(page_location ignore nulls order by event_ts, is_trigger desc, event_key limit 1)[safe_offset(0)]  as landing_page,

        -- pierwszy event sesji: czy to wejscie i z jakim zrodlem
        array_agg(
            struct(is_trigger, is_direct, source, medium, campaign)
            order by event_ts, is_trigger desc, event_key limit 1
        )[offset(0)]                                              as first_event,

        -- sesja otwarta pierwszym eventem dnia: zrodlo jak w silver (regula B)
        coalesce(
            array_agg(
                if(source is not null and not is_own_source and not is_direct, struct(source, medium, campaign), null)
                ignore nulls order by event_ts, event_key limit 1
            )[safe_offset(0)],
            array_agg(
                if(source is not null and not is_own_source, struct(source, medium, campaign), null)
                ignore nulls order by event_ts, event_key limit 1
            )[safe_offset(0)]
        )                                                         as src_rule_b
    from numbered
    group by user_pseudo_id, event_date, session_nr
),

txn as (
    -- transakcja trafia do sesji, w ktorej jest jej (pierwszy) event purchase
    select
        n.user_pseudo_id,
        n.event_date,
        n.session_nr,
        count(*)        as transactions,
        sum(t.revenue)  as revenue
    from {{ ref('slv_ga4__transactions') }} t
    join numbered n on n.event_key = t.event_key
    group by n.user_pseudo_id, n.event_date, n.session_nr
),

final as (
    select
        concat(s.user_pseudo_id, '-', format_date('%Y%m%d', s.session_date), '-', cast(s.session_nr as string))  as session_key,
        s.* except (first_event, src_rule_b),
        case
            when not s.first_event.is_trigger then 'first_event_of_day'
            when s.first_event.is_direct     then 'direct_homepage'
            else                                  'priority_source'
        end                                                       as opened_by,
        coalesce(if(s.first_event.is_trigger, s.first_event.source,   s.src_rule_b.source),   '(not set)')  as session_source,
        coalesce(if(s.first_event.is_trigger, s.first_event.medium,   s.src_rule_b.medium),   '(not set)')  as session_medium,
        coalesce(if(s.first_event.is_trigger, s.first_event.campaign, s.src_rule_b.campaign), '(not set)')  as session_campaign,
        coalesce(t.transactions, 0)                               as transactions,
        coalesce(t.revenue, 0)                                    as session_revenue
    from sessions s
    left join txn t
        on  t.user_pseudo_id = s.user_pseudo_id
        and t.event_date     = s.session_date
        and t.session_nr     = s.session_nr
)

select * from final