{{ config(
    materialized = 'table',
    partition_by = {'field': 'event_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by = ['event_name', 'user_pseudo_id'],
    labels = {'layer': 'bronze', 'source': 'ga4'}
) }}

-- Bronze: kopia 1:1 surowych eventow GA4 - bez czyszczenia i bez UNNEST.
-- Jedyne zmiany: event_date STRING -> DATE (do partycjonowania)
-- oraz kolumny techniczne: _source_shard (z ktorej tabeli dziennej) i _loaded_at.

select
    * replace (parse_date('%Y%m%d', event_date) as event_date),
    _table_suffix       as _source_shard,
    current_timestamp() as _loaded_at
from {{ source('ga4', 'events') }}
where _table_suffix between '{{ var("ga4_start") }}' and '{{ var("ga4_end") }}'
