{{ config(
    materialized = 'table'
) }}

with raw_users as (
    select * from {{ source('raw_komodo', 'raw_users') }}
)

select
    user_id,
    first_name,
    last_name,
    trim(first_name || ' ' || last_name) as full_name,
    email,
    gender,
    age,
    city,
    signup_date,
    is_active
from raw_users
where is_active = true
