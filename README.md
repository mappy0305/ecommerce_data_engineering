# ecommerce_data_engineering

A data engineering pipeline built with **dbt + Databricks** implementing a medallion architecture for an e-commerce dataset.

## Tech Stack
- **dbt-databricks** — data transformation
- **Databricks + Unity Catalog** — cloud data warehouse
- **Databricks Asset Bundles** — deployment (dev + prod)
- **GitHub Actions** — CI/CD

## Architecture

Data flows through three Unity Catalog schemas:

```
Bronze (raw seeds) → Silver (staging + intermediate views) → Gold (fact tables + SCD snapshot)
```

## Models

| Model | Layer | Type | Description |
|---|---|---|---|
| `stg_orders`, `stg_order_items`, `stg_products` | Silver | View | Type casting and normalisation |
| `int_orders_enriched` | Silver | View | Orders joined with customer and item aggregates |
| `int_order_items_with_product` | Silver | View | Order lines enriched with product context |
| `fct_orders` | Gold | Incremental | One row per order |
| `fct_order_items` | Gold | Incremental | One row per order line |
| `dim_products` | Gold | Table | Product dimension with surrogate key |
| `scd_customers` | Gold | Snapshot | Type 2 SCD tracking customer attribute changes |

## Data Quality
- Primary key uniqueness and not-null checks on all models
- Referential integrity across all foreign keys
- Custom tests for positive prices, non-negative revenue, valid emails, and no future order dates

## CI/CD
- Pushing to `dev` branch deploys to the dev target — schedule is paused (manual trigger only)
- Pushing to `main` branch deploys to prod — schedule runs daily automatically

## Running Locally

```bash
pip install -r requirements.txt
dbt deps
dbt seed        # load raw data
dbt snapshot    # run SCD snapshot
dbt run         # build all models
dbt test        # run data quality tests
```