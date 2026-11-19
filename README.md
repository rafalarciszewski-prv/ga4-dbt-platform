# ga4-dbt-platform

Projekt do nauki: mini-platforma danych na BigQuery + dbt Core w architekturze medalionowej,
na publicznej probce GA4 e-commerce (92 dni, 4,3 mln eventow).

## Uruchomienie
```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install dbt-core dbt-bigquery
gcloud auth application-default login
Copy-Item profiles.example.yml profiles.yml
dbt debug
dbt build -s bronze
```

## Warstwy
| Warstwa | Dataset | Status |
|---|---|---|
| bronze | `dev_bronze` | `brz_ga4__events` - surowe eventy, partycje dzienne |
| silver | `dev_silver` | do zrobienia |
| gold   | `dev_gold`   | do zrobienia |
