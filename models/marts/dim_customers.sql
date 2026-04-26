-- Dimension: one row per customer
with customers as (
    select * from {{ ref('scd_customers') }}
),

final as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        country,
        phone_number
    from customers
    where dbt_valid_to IS NULL
)

select * from final
