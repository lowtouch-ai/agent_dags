# webshop_data_reset

## Overview
- **Purpose**: Refreshes the webshop database by reloading seed data and rebuilding models using DBT. This resets specified tables to a baseline state defined in seed files and updates derived models daily.
- **Maintained by**: 
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

## Troubleshooting
- **Common Issues**:
  - Database connection failures due to invalid credentials
  - Virtual environment activation failures
  - Missing DBT dependencies
- **Logs to Check**:
  - Individual task logs (e.g., `dbt_seed_address`, `dbt_run_order`)
  - DBT execution logs in the project directory
- **Resolution Steps**:
  1. Verify Airflow variables are set correctly
  2. Check database connectivity
  3. Ensure DBT virtual environment is properly configured
