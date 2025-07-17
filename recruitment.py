from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import pandas as pd
import re
from PyPDF2 import PdfReader
from ollama import Client

#  Drive Shared Drive and Folder Config
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
    description='Process one JD and multiple CVs via AgentOmatic and save result to Drive CSV',
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

def call_agent(jd_text, cv_text, cv_file_name):
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

def parse_agent_response(agent_text):
    # Extract CV Score
    score_match = re.search(r'CV Score:\s*(\d+/\d+)', agent_text)
    overall_score = score_match.group(1) if score_match else ''

    # Extract the markdown table
    table_lines = []
    in_table = False
    for line in agent_text.splitlines():
        if '|' in line:
            in_table = True
            table_lines.append(line.strip())
        elif in_table:
            break  # stop collecting once table ends

    if len(table_lines) < 3:
        return []

    header = [h.strip() for h in table_lines[0].strip('|').split('|')]
    row = [r.strip() for r in table_lines[2].strip('|').split('|')]

    data = dict(zip(header, row))
    return {
        'Name': data.get('Name', ''),
        'Email': data.get('Email', ''),
        'Phone Number': data.get('Phone Number', ''),
        'Overall Score': overall_score,
        'Must-Have Criteria': data.get('Must-Have Skills', ''),
        'Nice-to-Have Criteria': data.get('Nice-to-Have Skills', ''),
        'Other Criteria': data.get('Other Criteria', ''),
        'Remarks': data.get('Remarks', '').replace('<br>', '; ').strip(),
        'Notes': data.get('Notes', '').replace('<br>', '; ').strip(),
    }

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
        agent_result = call_agent(jd_text, cv['text'], cv['name'])
        parsed = parse_agent_response(agent_result)
        parsed['cv_file'] = cv['name']
        structured_results.append(parsed)

    df = pd.DataFrame(structured_results)

    timestamp_str = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    dynamic_filename = f"cv_results_{timestamp_str}.csv"

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    upload_to_drive(
        service=service,
        content_bytes=csv_buffer.getvalue().encode('utf-8'),
        filename=dynamic_filename,
        folder_id=FOLDER_ID,
        mimetype='text/csv'
    )
    print(f"✅ Uploaded new CSV '{dynamic_filename}' with {len(df)} rows to Drive.")

with dag:
    process_and_score_task = PythonOperator(
        task_id='process_and_score_cv_against_jd',
        python_callable=process_and_score
    )
