{{ config(materialized='incremental', unique_key=['PATIENT_ID', 'INDICATION_ALERT']) }}

SELECT
    PATIENT_ID,
    '{{ var("indication_alert") }}' AS INDICATION_ALERT,
    LAST_ALERT_DATE,
    CURRENT_TIMESTAMP() AS UPDATED_AT
FROM KOMODO.RAW_KOMODO.{{ var(...) }}