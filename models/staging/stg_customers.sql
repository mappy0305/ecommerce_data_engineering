select
    customer_id,
    trim(first_name)       as first_name,
    trim(last_name)        as last_name,
    lower(trim(email))     as email,
    upper(trim(country))   as country,
    -- mask everything except the last 4 digits
    concat(
        split(phone_number,' ')[0],' ',
        repeat('*', length(split(phone_number,' ')[1]) - 4),
        right(split(phone_number,' ')[1],4)
    )                      as phone_number,
    created_at
from {{ ref('raw_customers') }}