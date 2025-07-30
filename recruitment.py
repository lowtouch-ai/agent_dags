from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from PyPDF2 import PdfReader
from ollama import Client
import io
import csv
import json
import re

#  Google Drive Shared Drive and Folder Config
SHARED_DRIVE_ID = Variable.get("SHARED_DRIVE_ID")
FOLDER_ID = Variable.get("FOLDER_ID")
MODEL_NAME = 'cvscan:0.3'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cv_agent_processing',
    default_args=default_args,
    description='Process one JD and multiple CVs via AgentOmatic and save structured result to Drive CSV',
    schedule_interval=None,
    catchup=False,
)

def get_drive_service():
    service_account_info = Variable.get("gdrive_credentials_json", deserialize_json=True)
    creds = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds)

def extract_text_from_pdf_bytes(pdf_bytes):
    reader = PdfReader(io.BytesIO(pdf_bytes))
    text = ''
    for page in reader.pages:
        text += page.extract_text() or ''
    return text.strip()

def extract_json_from_text(text):
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
    except Exception:
        return None
    return None

def call_agent(jd_text, cv_text, cv_file_name):
    agent_url = f"http://{Variable.get('AGENT_HOST', default_var='agentomatic:8000')}/"
    client = Client(host=agent_url)

    instructions = """
Act as a CV Evaluation Bot.

Given a Job Description and a Candidate CV, compare and evaluate the candidate.

Your response MUST be a valid JSON object only, and must contain ONLY the following fields:

{
  "Name": "string",
  "Email": "string",
  "Phone Number": "string",
  "Overall Score": "string or number",
  "Must-Have Criteria": "string",
  "Nice-to-Have Criteria": "string",
  "Other Criteria": "string",
  "Notes": "string",
  "Remarks": "string"
}

❗Do not add explanation, markdown, or formatting. Respond with ONLY a pure JSON object.
"""

    messages = [
        {"role": "user", "content": f"{instructions}\n\nJob Description:\n{jd_text}"},
        {"role": "user", "content": f"Candidate CV:\n{cv_text}"}
    ]

    response = client.chat(
        model=MODEL_NAME,
        messages=messages,
        stream=False
    )
    return response['message']['content']

def upload_to_drive(service, content_bytes, filename, folder_id, mimetype):
    media = MediaIoBaseUpload(io.BytesIO(content_bytes), mimetype=mimetype)

    try:
        service.files().create(
            body={'name': filename, 'parents': [folder_id]},
            media_body=media,
            supportsAllDrives=True,
            fields='id'
        ).execute()
        print(f"✅ Uploaded file: {filename}")
    except Exception as e:
        print(f"❌ Failed to upload file {filename}: {e}")
        raise

def generate_markdown_table(data, headers):
    lines = []
    lines.append('| ' + ' | '.join(headers) + ' |')
    lines.append('| ' + ' | '.join(['---'] * len(headers)) + ' |')
    for row in data:
        lines.append('| ' + ' | '.join(str(row.get(h, "")).replace('\n', ' ').replace('|', '\\|') for h in headers) + ' |')
    return '\n'.join(lines)

def process_and_score(ti, **kwargs):
    service = get_drive_service()
    results = service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        fields="files(id, name)"
    ).execute()

    files = results.get('files', [])
    jd_text = None
    cvs = []

    for file in files:
        file_id = file['id']
        file_name = file['name'].lower()
        pdf_bytes = service.files().get_media(fileId=file_id).execute()
        extracted_text = extract_text_from_pdf_bytes(pdf_bytes)

        if 'jd' in file_name or 'job description' in file_name:
            jd_text = extracted_text
            print(f"✅ Found JD: {file['name']}")
        else:
            cvs.append({
                'name': file['name'],
                'text': extracted_text
            })

    if not jd_text:
        raise ValueError("❗ No JD found in Drive folder.")

    structured_results = []
    for cv in cvs:
        print(f"⚙️ Processing CV: {cv['name']}")
        agent_output = call_agent(jd_text, cv['text'], cv['name'])
        parsed = extract_json_from_text(agent_output)
        if parsed:
            structured_results.append(parsed)
        else:
            print(f"❌ JSON parsing failed for {cv['name']}")
            structured_results.append({
                "Name": cv['name'],
                "Email": "",
                "Phone Number": "",
                "Overall Score": "",
                "Must-Have Criteria": "",
                "Nice-to-Have Criteria": "",
                "Other Criteria": "",
                "Notes": "Parsing failed",
                "Remarks": agent_output[:1000]
            })

    # Push Markdown formatted summary to XCom
    fieldnames = [
        "Name", "Email", "Phone Number", "Overall Score",
        "Must-Have Criteria", "Nice-to-Have Criteria", "Other Criteria",
        "Notes", "Remarks"
    ]
    markdown_output = generate_markdown_table(structured_results, fieldnames)
    ti.xcom_push(key='md_result', value=markdown_output)

    # Create and upload CSV
    timestamp_str = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    dynamic_filename = f"cv_results_{timestamp_str}.csv"

    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in structured_results:
        writer.writerow(row)

    upload_to_drive(
        service=service,
        content_bytes=csv_buffer.getvalue().encode('utf-8'),
        filename=dynamic_filename,
        folder_id=FOLDER_ID,
        mimetype='text/csv'
    )
    print(f"✅ Uploaded new structured CSV '{dynamic_filename}' with {len(structured_results)} results to Drive.")

with dag:
    process_and_score_task = PythonOperator(
        task_id='process_and_score_cv_against_jd',
        python_callable=process_and_score
    )
