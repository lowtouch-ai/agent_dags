from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'bigquery_simulator',
    default_args=default_args,
    description='A DAG to simulate BigQuery queries by running good and bad queries sequentially',
    schedule=None,
    catchup=False,
    tags=["sre", "bigquery", "simulation"]
)

good_queries = [
    """
    SELECT
    c.gender,
    EXTRACT(YEAR FROM c.dateofbirth) as birth_year,
    COUNT(DISTINCT a.id) as articles_purchased
    FROM sre-agent-9809.webshop.customer c
    INNER JOIN sre-agent-9809.webshop.order o ON c.id = o.customerid
    INNER JOIN sre-agent-9809.webshop.order_positions op ON o.id = op.orderid
    INNER JOIN sre-agent-9809.webshop.articles a ON op.articleid = a.id
    WHERE o.ordertimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 8000 DAY)
    AND c.created >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 8000 DAY)
    GROUP BY c.gender, birth_year
    ORDER BY birth_year;
    """,
    """
    SELECT
    addr.zip,
    COUNT(DISTINCT c.id) as customers,
    SUM(o.total) as total_orders_value
    FROM sre-agent-9809.webshop.address addr
    INNER JOIN sre-agent-9809.webshop.customer c ON addr.id = c.currentaddressid
    LEFT JOIN sre-agent-9809.webshop.order o ON c.id = o.customerid
    WHERE o.ordertimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6000 DAY)
    AND addr.created >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6000 DAY)
    GROUP BY addr.zip
    ORDER BY customers DESC
    LIMIT 50;
    """,
    """
    SELECT
    sz.size_us,
    sz.gender,
    SUM(a.discountinpercent * a.reducedprice) as discounted_value
    FROM sre-agent-9809.webshop.sizes sz
    INNER JOIN sre-agent-9809.webshop.articles a ON sz.id = a.size
    WHERE a.created >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3000 DAY)
    GROUP BY sz.size_us, sz.gender
    ORDER BY discounted_value DESC
    LIMIT 100;
    """,
    """
    SELECT
    col.name as color,
    SUM(op.amount) as total_sold,
    COUNT(DISTINCT p.id) as products
    FROM sre-agent-9809.webshop.colors col
    INNER JOIN sre-agent-9809.webshop.articles a ON col.id = a.colorid
    INNER JOIN sre-agent-9809.webshop.products p ON a.productid = p.id
    INNER JOIN sre-agent-9809.webshop.order_positions op ON a.id = op.articleid
    INNER JOIN sre-agent-9809.webshop.order o ON op.orderid = o.id
    WHERE o.ordertimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5000 DAY)
    GROUP BY col.name
    ORDER BY total_sold DESC;
    """,
    """
    SELECT
    c.id as customer_id,
    COUNT(DISTINCT op.id) as positions_count,
    SUM(op.amount * op.price) as total_spent
    FROM sre-agent-9809.webshop.customer c
    INNER JOIN sre-agent-9809.webshop.order o ON c.id = o.customerid
    INNER JOIN sre-agent-9809.webshop.order_positions op ON o.id = op.orderid
    WHERE o.ordertimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4000 DAY)
    GROUP BY c.id
    ORDER BY total_spent DESC
    LIMIT 200;
    """,
    """
    SELECT
    c.id as customer_id,
    COUNT(DISTINCT op.id) as positions_count,
    SUM(op.amount * op.price) as total_spent
    FROM sre-agent-9809.webshop.customer c
    INNER JOIN sre-agent-9809.webshop.order o ON c.id = o.customerid
    INNER JOIN sre-agent-9809.webshop.order_positions op ON o.id = op.orderid
    WHERE o.ordertimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4000 DAY)
    GROUP BY c.id
    ORDER BY total_spent DESC
    LIMIT 200;
    """,
    """
    SELECT
    o1.id as order1,
    o2.id as order2,
    o1.customerid,
    o1.total + o2.total as combined_total
    FROM sre-agent-9809.webshop.order o1
    INNER JOIN sre-agent-9809.webshop.order o2 ON o1.customerid = o2.customerid
    AND o1.id != o2.id
    WHERE o1.ordertimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20000 DAY)
    AND o2.ordertimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20000 DAY)
    ORDER BY o1.customerid, combined_total DESC
    LIMIT 9000;
    """,
    """
    SELECT
    op1.id as pos1_id,
    op2.id as pos2_id,
    op1.articleid,
    op1.amount + op2.amount as total_amount
    FROM sre-agent-9809.webshop.order_positions op1
    INNER JOIN sre-agent-9809.webshop.order_positions op2 ON op1.articleid = op2.articleid
    AND op1.id != op2.id
    AND op1.orderid != op2.orderid
    INNER JOIN sre-agent-9809.webshop.order o1 ON op1.orderid = o1.id
    INNER JOIN sre-agent-9809.webshop.order o2 ON op2.orderid = o2.id
    WHERE o1.ordertimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15000 DAY)
    AND o2.ordertimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15000 DAY)
    ORDER BY op1.articleid
    LIMIT 10000;
    """
]

bad_queries = [
    """
    SELECT o.customerid, DATE_TRUNC(o.ordertimestamp, MONTH) AS order_month, COUNT(DISTINCT o.id) AS monthly_order_count, SUM(op.amount * op.price) AS monthly_revenue FROM `sre-agent-9809.webshop.order_partitioned` o INNER JOIN `sre-agent-9809.webshop.order_positions` op ON o.id = op.orderid WHERE o.ordertimestamp BETWEEN TIMESTAMP('2023-01-01') AND TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND o.customerid IN (SELECT DISTINCT customerid FROM `sre-agent-9809.webshop.order` LIMIT 50) GROUP BY o.customerid, order_month ORDER BY order_month DESC, monthly_revenue DESC LIMIT 500 
    """,
    """
    SELECT
    a1.id as article1_id,
    a2.id as article2_id,
    a1.productid,
    ABS(a1.reducedprice - a2.reducedprice) as price_diff
    FROM sre-agent-9809.webshop.articles a1
    INNER JOIN sre-agent-9809.webshop.articles a2 ON a1.productid = a2.productid
    AND a1.id != a2.id
    AND a1.colorid != a2.colorid
    WHERE a1.created >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10000 DAY)
    AND a2.created >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10000 DAY)
    ORDER BY a1.productid, price_diff
    LIMIT 12000;
    """,
    """
    SELECT
    op1.id as pos1_id,
    op2.id as pos2_id,
    op1.articleid,
    op1.amount + op2.amount as total_amount
    FROM sre-agent-9809.webshop.order_positions op1
    INNER JOIN sre-agent-9809.webshop.order_positions op2 ON op1.articleid = op2.articleid
    AND op1.id != op2.id
    AND op1.orderid != op2.orderid
    INNER JOIN sre-agent-9809.webshop.order o1 ON op1.orderid = o1.id
    INNER JOIN sre-agent-9809.webshop.order o2 ON op2.orderid = o2.id
    WHERE o1.ordertimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15000 DAY)
    AND o2.ordertimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15000 DAY)
    ORDER BY op1.articleid
    LIMIT 10000;
    """
]

all_queries = good_queries + bad_queries

prev_task = None
for i, query in enumerate(all_queries):
    # Determine if it's a good or bad query based on index
    query_type = 'good_query' if i < len(good_queries) else 'bad_query'
    # Calculate the index within its respective category
    category_index = i + 1 if i < len(good_queries) else i - len(good_queries) + 1
    
    task_id = f'{query_type}_{category_index}'
    task = BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False
            }
        },
        dag=dag,
    )
    if prev_task:
        prev_task >> task
    prev_task = task


