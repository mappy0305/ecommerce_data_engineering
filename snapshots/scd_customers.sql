-- Type 2 SCD: customers (from raw_customers)
{% snapshot scd_customers %}
{{
    config(
      target_schema='silver',
      unique_key='customer_id',
      strategy='check',
      check_cols=['first_name', 'last_name', 'email', 'country', 'phone_number'],
    )
}}
select
*
from {{ ref('stg_customers') }}
{% endsnapshot %}
