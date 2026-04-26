-- Dimension: one row per customer
with customers as (
    select * from {{ ref('scd_customers') }}
    where dbt_valid_to IS NULL
),
final as (
    select
        customer_id,
        first_name,
        last_name,
        country,
        concat(
            left(email, 1),
            repeat('*', length(split(email, '@')[0]) - 1),
            '@',
            split(email, '@')[1]
        ) as email,
        -- mask everything except the last 4 digits
        concat(
            split(phone_number,' ')[0],' ',
            repeat('*', length(split(phone_number,' ')[1]) - 4),
            right(split(phone_number,' ')[1],4)
        )                      as phone_number
    from customers
)

select * from final
