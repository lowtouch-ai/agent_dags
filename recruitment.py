from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
import io
from PyPDF2 import PdfReader

# Constants
FOLDER_ID = 'https://drive.google.com/drive/folders/1cFl0s4IkZi-pPhZ4mRoAFm_gj9wEF8g1'  # Replace with your actual Google Drive folder ID
CREDENTIALS_PATH = Variable.get("credentials")  # Loaded from Airflow Variable

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cv_drive_processing',
    default_args=default_args,
    description='Extract CVs and JD from Google Drive and parse content',
    schedule_interval=None,
    catchup=False
)

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH,
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=creds)

def extract_text_from_pdf_bytes(pdf_bytes):
    reader = PdfReader(io.BytesIO(pdf_bytes))
    text = ''
    for page in reader.pages:
        text += page.extract_text() or ''
    return text.strip()

def process_drive_files():
    service = get_drive_service()

    # Query all PDF files in the folder
    results = service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='application/pdf' and trashed = false",
        fields="files(id, name)",
    ).execute()

    files = results.get('files', [])
    if not files:
        print("No files found in the Drive folder.")
        return

    jd_text = None
    cv_texts = []

    for file in files:
        file_id = file['id']
        file_name = file['name'].lower()

        # Read PDF content as bytes (not saving locally)
        pdf_bytes = service.files().get_media(fileId=file_id).execute()
        text_content = extract_text_from_pdf_bytes(pdf_bytes)

        if 'jd' in file_name or 'job description' in file_name:
            print(f"✅ Identified JD file: {file['name']}")
            jd_text = text_content
        else:
            print(f"📄 Processed CV: {file['name']}")
            cv_texts.append({
                'file_name': file['name'],
                'text': text_content
            })

    # Output sample
    print("\n📌 JD Preview:")
    print(jd_text[:500] if jd_text else "No JD file detected.")

    print("\n📌 CV Previews:")
    for cv in cv_texts:
        print(f"- {cv['file_name']} → {cv['text'][:200]}")

with dag:
    process_files_task = PythonOperator(
        task_id='process_drive_files',
        python_callable=process_drive_files
    )
