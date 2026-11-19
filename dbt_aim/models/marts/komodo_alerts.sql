{{ config(materialized='incremental', unique_key=['ALERT_ID', 'INDICATION_ALERT']) }}

SELECT
    ALERT_ID, PATIENT_ID, NPI, HCP_FULL_NAME, HCPSPECIALTY1, STATE,
    '{{ var("indication_alert") }}' AS INDICATION_ALERT,
    DIAGNOSED_AHUS, TERRITORY_NAME, FILE_DATE,
    CURRENT_TIMESTAMP() AS LOADED_AT
FROM {{ ref(var('alert_staging_table')) }}