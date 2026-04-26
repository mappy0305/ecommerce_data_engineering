select
    customer_id,
    trim(first_name)       as first_name,
    trim(last_name)        as last_name,
    upper(trim(country))   as country,
    lower(trim(email))     as email,
    -- mask everything except the last 4 digits
    trim(phone_number)     as phone_number,
    created_at
from {{ ref('raw_customers') }}