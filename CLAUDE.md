# ga4-dbt-platform

Mini-platforma danych: BigQuery + dbt Core, architektura medalionowa (bronze -> silver -> gold)
na publicznej probce GA4 (`bigquery-public-data.ga4_obfuscated_sample_ecommerce`, region US).

## Zasady
- Srodowisko: Windows + PowerShell, venv w `.venv` (aktywacja: `.\.venv\Scripts\Activate.ps1`).
- Przed kazdym `dbt run` na pelnym zakresie: `dbt compile` i sprawdz koszt (dry run). Limit na zapytanie: 5 GB.
- Do testow uzywaj krotkiego zakresu: `--vars "{ga4_end: '20201107'}"`.
- Nigdy nie commituj `profiles.yml`, kluczy ani `.env`. Repo jest publiczne.
- Bronze = 1:1 ze zrodlem. Czyszczenie i UNNEST dopiero w silver.
- Nazewnictwo: `brz_`, `slv_`, `gld_` + `<zrodlo>__<obiekt>`.
