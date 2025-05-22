from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pytz
import gspread
import json
from google.oauth2.service_account import Credentials
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Constants
SLACK_BOT_TOKEN = Variable.get("BOT_TOKEN")
CHANNEL_IDS = ["C08SSCBDT3R", "C08T2N7LL7P", "C08T81YTH0S"]
GOOGLE_SHEET_NAME = "Alert Count"
TIMEZONE = "Asia/Kolkata"

# Slack client
client = WebClient(token=SLACK_BOT_TOKEN)

# Google Sheets setup using service account info from Airflow Variable
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_JSON = Variable.get("COUNT_DAG", deserialize_json=True)
creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_JSON, scopes=SCOPES)
gc = gspread.authorize(creds)
sheet = gc.open(GOOGLE_SHEET_NAME).sheet1

def get_channel_name(channel_id):
    try:
        response = client.conversations_info(channel=channel_id)
        return response['channel']['name']
    except SlackApiError as e:
        return channel_id

def log_to_gsheet(date, counts_dict):
    header = sheet.row_values(1)
    if not header:
        header = ["Date"] + [f"{channel}_Count" for channel in counts_dict.keys()]
        sheet.append_row(header)
    else:
        for channel in counts_dict.keys():
            col_name = f"{channel}_Count"
            if col_name not in header:
                header.append(col_name)
        sheet.update('A1', [header])

    all_dates = sheet.col_values(1)[1:]
    try:
        row_index = all_dates.index(date) + 2
    except ValueError:
        row_index = len(all_dates) + 2
        new_row = [date] + [""] * (len(header) - 1)
        sheet.insert_row(new_row, row_index)

    for channel, count in counts_dict.items():
        col_name = f"{channel}_Count"
        if col_name in header:
            col_index = header.index(col_name) + 1
            sheet.update_cell(row_index, col_index, count)

def count_channel_messages(channel_id, start_ts, end_ts):
    total_messages = 0
    cursor = None
    try:
        while True:
            response = client.conversations_history(
                channel=channel_id,
                cursor=cursor,
                limit=1000,
                oldest=start_ts,
                latest=end_ts,
                inclusive=True
            )
            messages = response["messages"]
            total_messages += len(messages)
            if not response.get("has_more"):
                break
            cursor = response.get("response_metadata", {}).get("next_cursor")
    except SlackApiError:
        total_messages = None
    return total_messages

def slack_alert_count():
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    today = now.strftime("%Y-%m-%d")

    start_of_day = tz.localize(datetime(now.year, now.month, now.day, 0, 0, 0))
    end_of_day = tz.localize(datetime(now.year, now.month, now.day, 23, 59, 59))
    start_ts = start_of_day.timestamp()
    end_ts = end_of_day.timestamp()

    counts = {}
    for channel_id in CHANNEL_IDS:
        name = get_channel_name(channel_id)
        count = count_channel_messages(channel_id, start_ts, end_ts)
        counts[name] = count if count is not None else 0

    log_to_gsheet(today, counts)

# Airflow DAG Definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'slack_alert_count_dag',
    default_args=default_args,
    description='Slack message counter that logs to Google Sheets',
    schedule_interval='59 23 * * *',  # Every day at 11:59 PM
    catchup=False,
)

task = PythonOperator(
    task_id='count_slack_alerts',
    python_callable=slack_alert_count,
    dag=dag,
)
