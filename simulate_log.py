import os
import psycopg2
import json
import time
from decimal import Decimal, InvalidOperation
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_pending_transactions():
    """Task 1: Fetch pending transactions from database - PASSES with normal logs"""
    logging.info("=" * 80)
    logging.info("[INFO] Starting transaction fetch process")
    logging.info("=" * 80)
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Connecting to payment database...")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Database: payment_gateway_prod | Host: pg-primary-01.internal")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Connection established successfully")
    
    logging.info(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Executing query: SELECT * FROM pending_transactions WHERE status='PENDING' AND created_at > NOW() - INTERVAL '24 hours'")
    
    transactions = [
        {"txn_id": "TXN-20241009-847562", "amount": 1250.00, "currency": "USD", "merchant_id": "MERCH-10293"},
        {"txn_id": "TXN-20241009-847563", "amount": 589.99, "currency": "USD", "merchant_id": "MERCH-10485"},
        {"txn_id": "TXN-20241009-847564", "amount": 3420.50, "currency": "USD", "merchant_id": "MERCH-10293"},
        {"txn_id": "TXN-20241009-847565", "amount": 125.00, "currency": "USD", "merchant_id": "MERCH-11024"},
    ]
    
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Query executed successfully")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Records fetched: {len(transactions)}")
    
    total_amount = sum(t['amount'] for t in transactions)
    
    logging.info(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Transaction Summary:")
    for txn in transactions:
        logging.info(f"  - {txn['txn_id']} | Amount: ${txn['amount']:.2f} {txn['currency']} | Merchant: {txn['merchant_id']}")
    
    logging.info(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Total transaction value: ${total_amount:.2f}")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Transaction fetch completed successfully")
    logging.info("=" * 80)

def process_payment_gateway():
    """Task 2: Process payments through gateway - FAILS with payment gateway error"""
    logging.info("=" * 80)
    logging.info("[INFO] Starting payment gateway processing")
    logging.info("=" * 80)
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Initializing payment gateway client...")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Gateway Provider: StripeConnect")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] API Version: 2024-06-20")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Environment: production")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Gateway client initialized successfully")
    
    logging.info(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Processing transaction batch...")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Transaction ID: TXN-20241009-847562")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Amount: $1250.00 USD")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Payment Method: card_1Nq8xYHKFD3j8hdK")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Customer ID: cus_OvPGX8Y4hKTZQn")
    
    # Simulate retry attempts
    logging.info(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Attempt 1/3: Sending payment request to gateway...")
    time.sleep(0.1)
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [WARN] Gateway response: 504 Gateway Timeout")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [WARN] Retrying in 2 seconds...")
    
    time.sleep(0.1)
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Attempt 2/3: Sending payment request to gateway...")
    time.sleep(0.1)
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [WARN] Gateway response: Connection reset by peer (errno: 104)")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [WARN] Retrying in 4 seconds...")
    
    time.sleep(0.1)
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Attempt 3/3: Sending payment request to gateway...")
    time.sleep(0.1)
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] Gateway response: 503 Service Unavailable")
    
    logging.info(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] ================================================")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] Payment Gateway Failure")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] ================================================")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] Error Code: GATEWAY_MAX_RETRY_EXCEEDED")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] Transaction ID: TXN-20241009-847562")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] Gateway Provider: StripeConnect")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] Details: Maximum retry attempts (3) exceeded for payment processing")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] Root Cause: stripe.error.APIConnectionError - Failed to connect to gateway API")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] Last Response: 503 Service Unavailable - Upstream service temporarily unavailable")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] HTTP Status: 503")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] Request ID: req_8kJdKx9YmPqZ2nR")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] Gateway Endpoint: https://api.stripe.com/v1/payment_intents")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] Timestamp: {datetime.now().isoformat()}")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] Recommended Action: Transaction marked as FAILED and queued for manual review")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [ERROR] ================================================")
    logging.info("=" * 80)
    
    raise Exception(
        f"PaymentGatewayError: Maximum retry attempts exceeded (3/3). "
        f"Transaction TXN-20241009-847562 failed. "
        f"stripe.error.APIConnectionError: Failed to connect to Stripe API after 3 attempts. "
        f"Last error: 503 Service Unavailable. Request ID: req_8kJdKx9YmPqZ2nR. "
        f"Gateway endpoint: https://api.stripe.com/v1/payment_intents. "
        f"Please check gateway status and retry manually."
    )

def update_transaction_status():
    """Task 3: Update transaction status in database - PASSES with normal logs"""
    logging.info("=" * 80)
    logging.info("[INFO] Starting transaction status update")
    logging.info("=" * 80)
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Connecting to payment database...")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Database: payment_gateway_prod | Host: pg-primary-01.internal")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Connection established successfully")
    
    updates = [
        {"txn_id": "TXN-20241009-847560", "old_status": "PROCESSING", "new_status": "COMPLETED", "amount": 450.00},
        {"txn_id": "TXN-20241009-847561", "old_status": "PROCESSING", "new_status": "COMPLETED", "amount": 1890.50},
        {"txn_id": "TXN-20241009-847562", "old_status": "PROCESSING", "new_status": "FAILED", "amount": 1250.00},
    ]
    
    logging.info(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Beginning transaction updates...")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Total records to update: {len(updates)}")
    
    for update in updates:
        logging.info(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Updating transaction: {update['txn_id']}")
        logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Executing: UPDATE transactions SET status='{update['new_status']}', updated_at=NOW() WHERE txn_id='{update['txn_id']}'")
        logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Status changed: {update['old_status']} → {update['new_status']}")
        logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Amount: ${update['amount']:.2f}")
        logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Update successful (1 row affected)")
    
    logging.info(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Committing transaction...")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Transaction committed successfully")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Database connection closed")
    logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Status update process completed successfully")
    logging.info("=" * 80)

def send_payment_notifications():
    """Task 4: Silent task - no logs (sends notifications in background)"""
    pass

def generate_payment_reports():
    """Task 5: Silent task - no logs (generates reports silently)"""
    pass

with DAG(
    dag_id="airflow_log_simulator",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["payments", "gateway", "transactions", "finance"],
    default_args={
        'owner': 'payment-ops',
    }
) as dag:

    fetch_transactions = PythonOperator(
        task_id="fetch_pending_transactions",
        python_callable=fetch_pending_transactions,
    )

    process_payments = PythonOperator(
        task_id="process_payment_gateway_failure",
        python_callable=process_payment_gateway,
        trigger_rule='all_done',
    )

    update_status = PythonOperator(
        task_id="update_transaction_status",
        python_callable=update_transaction_status,
        trigger_rule='all_done',
    )

    send_notifications = PythonOperator(
        task_id="send_payment_notifications_low_log",
        python_callable=send_payment_notifications,
        trigger_rule='all_done',
    )

    generate_reports = PythonOperator(
        task_id="generate_payment_reports_low_log",
        python_callable=generate_payment_reports,
        trigger_rule='all_done',
    )

    # Task dependencies
    fetch_transactions >> process_payments >> update_status >> send_notifications >> generate_reports
