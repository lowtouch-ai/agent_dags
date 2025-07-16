from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import csv
import re
from PyPDF2 import PdfReader
from ollama import Client

SHARED_DRIVE_ID = '0AO6Pw6zAUDLJUk9PVA'
FOLDER_ID = '1sqk2IONrPJHtNruCMzAyYqOd3igXJmND'
MODEL_NAME = 'recruitment-agent:0.3'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cv_agent_processing',
    default_args=default_args,
    description='Process JD and CVs, parse structured output, upload CSV to Drive',
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
    return " ".join(page.extract_text() or '' for page in reader.pages).strip()

def call_agent(jd_text, cv_text):
    agent_url = f"http://{Variable.get('AGENT_HOST', default_var='agentomatic:8000')}/"
    client = Client(host=agent_url)
    messages = [
        {"role": "user", "content": f"Job Description:\n{jd_text}"},
        {"role": "user", "content": f"Candidate CV:\n{cv_text}"}
    ]
    response = client.chat(
        model=MODEL_NAME,
        messages=messages,
        stream=False
    )
    return response['message']['content']

def parse_agent_output(text):
    patterns = {
        "Name": r"Name\s*[:\-]\s*(.*)",
        "Email": r"Email\s*[:\-]\s*([^\s,;]+@[^"]+)",
        "Phone Number": r"Phone\s*Number\s*[:\-]?[\s]*(\+?\d[\d\s\-().]{6,})",
        "Overall Score": r"Overall\s*Score\s*[:\-]?[\s]*(\d{1,3})",
        "Must-Have Criteria": r"Must[-\s]*Have\s*Criteria\s*[:\-]?[\s]*(.*)",
        "Nice-to-Have Criteria": r"Nice[-\s]*to[-\s]*Have\s*Criteria\s*[:\-]?[\s]*(.*)",
        "Other Criteria": r"Other\s*Criteria\s*[:\-]?[\s]*(.*)",
        "Notes": r"Notes\s*[:\-]?[\s]*(.*)",
        "Remarks": r"Remarks\s*[:\-]?[\s]*(.*)",
    }
    result = {k: '' for k in patterns}
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            result[key] = match.group(1).strip()
    return result

def upload_to_drive(service, content_bytes, filename, folder_id, mimetype):
    media = MediaIoBaseUpload(io.BytesIO(content_bytes), mimetype=mimetype)
    service.files().create(
        body={'name': filename, 'parents': [folder_id]},
        media_body=media,
        supportsAllDrives=True,
        fields='id'
    ).execute()
    print(f"✅ Uploaded file: {filename}")

def process_and_score(ti, **kwargs):
    service = get_drive_service()
    files = service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        fields="files(id, name)"
    ).execute().get('files', [])

    jd_text = None
    cvs = []
    for file in files:
        file_id, file_name = file['id'], file['name'].lower()
        pdf_bytes = service.files().get_media(fileId=file_id).execute()
        text = extract_text_from_pdf_bytes(pdf_bytes)
        if 'jd' in file_name or 'job description' in file_name:
            jd_text = text
            print(f"✅ Found JD: {file['name']}")
        else:
            cvs.append({'name': file['name'], 'text': text})

    if not jd_text:
        raise ValueError("❗ No JD found in Drive folder.")

    structured_results = []
    for cv in cvs:
        print(f"⚙️ Processing CV: {cv['name']}")
        response_text = call_agent(jd_text, cv['text'])
        parsed = parse_agent_output(response_text)
        parsed['CV File'] = cv['name']
        structured_results.append(parsed)

    csv_buffer = io.StringIO()
    fieldnames = ["Name", "Email", "Phone Number", "Overall Score",
                  "Must-Have Criteria", "Nice-to-Have Criteria", "Other Criteria",
                  "Notes", "Remarks", "CV File"]
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(structured_results)

    filename = f"cv_results_{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}.csv"
    upload_to_drive(
        service=service,
        content_bytes=csv_buffer.getvalue().encode('utf-8'),
        filename=filename,
        folder_id=FOLDER_ID,
        mimetype='text/csv'
    )

with dag:
    process_and_score_task = PythonOperator(
        task_id='process_and_score_cv_against_jd',
        python_callable=process_and_score
    )
