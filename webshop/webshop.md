# webshop_data_reset

## Overview
- **Purpose**: Refreshes the webshop database by reloading seed data and rebuilding models using DBT. This resets specified tables to a baseline state defined in seed files and updates derived models daily.
- **Maintained by**:
  - Adhil Ahammed (aahammed@ecloudcontrol)
  - Akshai Surendran (asurendran@ecloudcontrol.com)
  - Ansu Anna Varghese (aannavarghese@ecloudcontrol.com)
- **Schedule**: Daily at 8:00 AM IST (2:30 AM UTC)

## Configuration
### Airflow Variables
- `WEBSHOP_POSTGRES_USER`: PostgreSQL username for webshop database
- `WEBSHOP_POSTGRES_PASSWORD`: PostgreSQL password for webshop database

### DAG Parameters
- **Owner**: lowtouch.ai_developers
- **DBT Project Path**: /appz/home/airflow/dags/agent_dags/dbt/webshop
- **DBT Executable**: /dbt_venv/bin/dbt

### Workflow Components
1. **DBT Seed Tasks**:
   - Seeds: address, articles, colors, customer, labels, order_positions, order_seed, products, stock, sizes
   - Purpose: Loads static data from seed files into the database
2. **DBT Run Tasks**:
   - Models: order
   - Purpose: Executes transformations to build derived tables/views
     
### Elementary Data

For generated docs, [click here](https://airflow2025a.lowtouch.ai/docs/edr_target/elementary_report.html).
