from airflow.sdk import DAG, Param, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
import requests
import json
import logging
import random
import time

from agent_dags.utils.think_logging import get_logger, set_request_id

logger = logging.getLogger(__name__)
lot = get_logger("webshop_sales_report")

BASE_URL = "http://agentconnector:8000/webshop"

default_args = {"owner": "webshop", "start_date": pendulum.datetime(2025, 1, 1), "retries": 1}


def parse_number(value):
    """Parse a number that may be formatted as currency (e.g. '$5,860.55')."""
    if isinstance(value, (int, float)):
        return value
    return float(str(value).replace("$", "").replace(",", "")) if value else 0


def demo_delay():
    delay = random.uniform(2, 5)
    logger.info(f"Demo delay: {delay:.1f}s")
    time.sleep(delay)


def api_get(endpoint, params=None, description="API call"):
    demo_delay()
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        logger.warning(f"{description} failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def fetch_categories(**context):
    set_request_id(context)
    lot.info("fetching product categories...")
    data = api_get("category", description="Fetch categories")
    categories = [item["name"] for item in data] if isinstance(data, list) else []
    logger.info(f"Fetched {len(categories)} categories: {categories}")
    lot.info(f"fetched {len(categories)} categories")
    return categories


def sales_overview(**context):
    set_request_id(context)
    p = context["params"]
    lot.info(f"fetching sales overview ({p['sales_aggregation']})...")
    data = api_get(
        "analytics/sales",
        params={
            "aggregation": p["sales_aggregation"],
            "start_date": p["start_date"],
            "end_date": p["end_date"],
            "page_size": 0,
        },
        description="Sales overview",
    )
    logger.info(f"Sales overview fetched ({p['sales_aggregation']} aggregation)")
    return data


def sales_by_category(**context):
    set_request_id(context)
    p = context["params"]
    categories = context["ti"].xcom_pull(task_ids="fetch_categories")
    if not categories:
        logger.warning("No categories available — skipping per-category breakdown")
        return []

    lot.info(f"analyzing sales for {len(categories)} categories...")
    results = []
    for cat in categories:
        data = api_get(
            "analytics/sales",
            params={
                "aggregation": p["sales_aggregation"],
                "start_date": p["start_date"],
                "end_date": p["end_date"],
                "category": cat,
                "page_size": 0,
            },
            description=f"Sales for category '{cat}'",
        )
        total_sales = sum(parse_number(row.get("total_sales", 0)) for row in data.get("contents", []))
        total_orders = sum(parse_number(row.get("total_orders", 0)) for row in data.get("contents", []))
        results.append({"category": cat, "total_sales": total_sales, "total_orders": total_orders})

    # Rank by total sales descending
    results.sort(key=lambda x: x["total_sales"], reverse=True)
    for i, row in enumerate(results, 1):
        row["rank"] = i

    logger.info(f"Sales by category computed for {len(results)} categories")
    lot.info("category breakdown complete")
    return results


def top_products(**context):
    set_request_id(context)
    p = context["params"]
    lot.info(f"fetching top {p['top_n']} products...")
    overall = api_get(
        "product/top-selling/",
        params={
            "from_date": p["start_date"],
            "to_date": p["end_date"],
            "number_threshold": p["top_n"],
        },
        description="Top-selling products (overall)",
    )
    result = {"overall": overall if isinstance(overall, list) else overall.get("contents", [])}

    if p.get("include_gender_breakdown"):
        for gender in ("male", "female"):
            data = api_get(
                "product/top-selling/",
                params={
                    "from_date": p["start_date"],
                    "to_date": p["end_date"],
                    "number_threshold": p["top_n"],
                    "gender": gender,
                },
                description=f"Top-selling products ({gender})",
            )
            result[gender] = data if isinstance(data, list) else data.get("contents", [])
    else:
        result["male"] = []
        result["female"] = []

    logger.info(f"Top products fetched (gender_breakdown={p.get('include_gender_breakdown', False)})")
    lot.info("top products fetched")
    return result


def slow_movers(**context):
    """Fetch all ranked products and return the bottom N as slow movers."""
    set_request_id(context)
    p = context["params"]
    lot.info("identifying slow-moving products...")
    # Request a large number to get the full product list
    data = api_get(
        "product/top-selling/",
        params={
            "from_date": p["start_date"],
            "to_date": p["end_date"],
            "number_threshold": 9999,
        },
        description="All products (for slow movers)",
    )
    all_products = data if isinstance(data, list) else data.get("contents", [])
    if not all_products:
        logger.warning("No products returned — cannot determine slow movers")
        return {"total_products": 0, "slow_movers": []}

    # Products come sorted by sales descending — take the bottom N
    bottom_n = p["top_n"]
    slow = list(reversed(all_products[-bottom_n:])) if len(all_products) > bottom_n else all_products
    logger.info(f"Slow movers: {len(slow)} out of {len(all_products)} total products")
    lot.info(f"{len(slow)} slow movers found")
    return {"total_products": len(all_products), "slow_movers": slow}


def top_buyers(**context):
    set_request_id(context)
    p = context["params"]
    lot.info(f"fetching top {p['top_n']} buyers...")
    data = api_get(
        f"customer/top_{p['top_n']}_buyers/",
        description=f"Top {p['top_n']} buyers",
    )
    logger.info("Top buyers fetched")
    return data


def frequent_buyers(**context):
    set_request_id(context)
    lot.info("fetching frequent buyers...")
    data = api_get("customer/frequent_buyers/", description="Frequent buyers")
    logger.info("Frequent buyers fetched")
    return data


def customer_roi(**context):
    set_request_id(context)
    lot.info("fetching customer ROI data...")
    data = api_get("analytics/customer_roi", description="Customer ROI")
    logger.info("Customer ROI fetched")
    return data


def assemble_report(**context):
    set_request_id(context)
    lot.info("assembling final report...")
    demo_delay()
    p = context["params"]
    ti = context["ti"]

    overview_data = ti.xcom_pull(task_ids="sales_analysis.sales_overview") or {}
    by_category_data = ti.xcom_pull(task_ids="sales_analysis.sales_by_category") or []
    products_data = ti.xcom_pull(task_ids="product_intelligence.top_products") or {}
    slow_data = ti.xcom_pull(task_ids="product_intelligence.slow_movers") or {}
    buyers_data = ti.xcom_pull(task_ids="customer_intelligence.top_buyers") or {}
    frequent_data = ti.xcom_pull(task_ids="customer_intelligence.frequent_buyers") or {}
    roi_data = ti.xcom_pull(task_ids="customer_intelligence.customer_roi") or {}

    # Compute totals from the time series
    time_series = overview_data.get("contents", [])
    total_sales = sum(parse_number(row.get("total_sales", 0)) for row in time_series)
    total_orders = sum(parse_number(row.get("total_orders", 0)) for row in time_series)

    # Normalize buyer lists
    buyers_list = buyers_data if isinstance(buyers_data, list) else buyers_data.get("contents", [])
    frequent_list = frequent_data if isinstance(frequent_data, list) else frequent_data.get("contents", [])
    roi_list = roi_data if isinstance(roi_data, list) else roi_data.get("contents", [])

    # Enrich top products with sales $ and % of total
    def _find_sales_value(product):
        """Find the monetary sales field in a product dict."""
        for key in ("total_sales", "revenue", "total_revenue", "sales_amount", "total_amount"):
            if key in product:
                return parse_number(product[key])
        return 0

    def enrich_products(product_list):
        enriched = []
        for prod in product_list:
            if not isinstance(prod, dict):
                enriched.append(prod)
                continue
            item = dict(prod)
            sales_value = _find_sales_value(item)
            item["sales_total"] = round(sales_value, 2)
            item["pct_of_total"] = round((sales_value / total_sales) * 100, 2) if total_sales else 0
            enriched.append(item)
        return enriched

    enriched_products = {}
    for segment, prods in products_data.items():
        if isinstance(prods, list):
            enriched_products[segment] = enrich_products(prods)
        else:
            enriched_products[segment] = prods

    # Best / worst category
    best_category = by_category_data[0] if by_category_data else None
    worst_category = by_category_data[-1] if by_category_data else None

    report = {
        "report_metadata": {
            "generated_at": pendulum.now("UTC").isoformat(),
            "period": {"start_date": p["start_date"], "end_date": p["end_date"]},
            "parameters": {
                "top_n": p["top_n"],
                "sales_aggregation": p["sales_aggregation"],
                "include_gender_breakdown": p.get("include_gender_breakdown", False),
            },
        },
        "sales_overview": {
            "aggregation": p["sales_aggregation"],
            "total_sales": total_sales,
            "total_orders": total_orders,
            "average_order_value": round(total_sales / total_orders, 2) if total_orders else 0,
            "time_series": time_series,
        },
        "sales_by_category": {
            "categories": by_category_data,
            "best_performing": best_category,
            "worst_performing": worst_category,
        },
        "top_products": enriched_products,
        "slow_moving_products": slow_data,
        "customer_intelligence": {
            "top_buyers": buyers_list,
            "frequent_buyers": frequent_list,
            "customer_roi": roi_list,
        },
    }

    ti.xcom_push(key="report", value=json.dumps(report))
    logger.info(
        f"Report assembled: {len(time_series)} periods, "
        f"{len(by_category_data)} categories, "
        f"{len(products_data.get('overall', []))} top products, "
        f"{len(slow_data.get('slow_movers', []))} slow movers, "
        f"{len(buyers_list)} top buyers"
    )
    lot.info(
        f"report ready: {len(time_series)} periods, "
        f"{len(by_category_data)} categories, "
        f"{len(products_data.get('overall', []))} top products"
    )
    return report


with DAG(
    "webshop_sales_report",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["webshop", "sales", "report"],
    description="Generates a 360-degree webshop sales report: sales trends, category breakdown, top/slow products, and customer intelligence.",
    params={
        "start_date": Param(
            default="2018-01-01",
            type="string",
            description="Report start date (YYYY-MM-DD)",
        ),
        "end_date": Param(
            default="2018-12-31",
            type="string",
            description="Report end date (YYYY-MM-DD)",
        ),
        "top_n": Param(
            default=10,
            type="integer",
            description="Number of top/bottom items (products, customers)",
        ),
        "sales_aggregation": Param(
            default="monthly",
            type="string",
            enum=["daily", "weekly", "monthly", "yearly"],
            description="Time aggregation for sales data",
        ),
        "include_gender_breakdown": Param(
            default=False,
            type="boolean",
            description="Include male/female split for top products",
        ),
    },
) as dag:

    fetch_cats = PythonOperator(task_id="fetch_categories", python_callable=fetch_categories)

    with TaskGroup("sales_analysis") as sales_tg:
        overview = PythonOperator(task_id="sales_overview", python_callable=sales_overview)
        by_category = PythonOperator(task_id="sales_by_category", python_callable=sales_by_category)
        overview >> by_category

    with TaskGroup("product_intelligence") as product_tg:
        top_prod = PythonOperator(task_id="top_products", python_callable=top_products)
        slow_prod = PythonOperator(task_id="slow_movers", python_callable=slow_movers)

    with TaskGroup("customer_intelligence") as customer_tg:
        PythonOperator(task_id="top_buyers", python_callable=top_buyers)
        PythonOperator(task_id="frequent_buyers", python_callable=frequent_buyers)
        PythonOperator(task_id="customer_roi", python_callable=customer_roi)

    assemble = PythonOperator(task_id="assemble_report", python_callable=assemble_report)

    # fetch_categories feeds into sales_by_category (inside the TaskGroup)
    fetch_cats >> by_category

    # All groups feed into the final assembly
    [fetch_cats, sales_tg, product_tg, customer_tg] >> assemble
