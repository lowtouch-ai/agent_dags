from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import imaplib
import email
from email_header import decode_header
import pdfplumber
import odoorpc
import os
import smtplib
from email.mime.text import MIMEText
import logging
from dotenv import load_dotenv

default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'email_on_future':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

ODOO_URL=os.getenv("ODOO_URL", "https://odoo-dev-odoo-14-0.lowtouchcloud.io/")
ODOO_DB=os.getenv("ODOO_DB", "bitnami_odoo")
ODOO_USERNAME=os.getenv("ODOO_USERNAME")
ODOO_PASSWORD=os.getenv("ODOO_API_KEY")

def decode_email_subject(subject):
    decoded_subject = decode_header(subject)[0][0]
    if isinstance(decoded_subject, bytes):
        return decoded_subject.decode()
    return decoded_subject


def fetch_emails(**kwargs):
    imap_server="",
    email_user="",
    email_pass="",

    imap=imaplib.IMAP4_SSL(imap_server)
    imap.login(email_user,email_pass),
    imap.select('INBOX')

    _,message_numbers=imap.search(None,'(UNSEEN)')
    invoices=[]

    for num in message_numbers[0].split():
    _,msg_data=imap.fetch(num,'(RFC822)')
    email_body=msg_data[0][1]
    msg=email.message_from_bytes(email_body)
    subject=decode_email_subject(msg['subject'])
    sender=msg['from']

    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_maintype()=='multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue
            if part.get_filename() and part.get_filename().endwith('.pdf'):
                pdf_data=part.get_payload(decode=True)
                invoices.append({
                    'subject':subject,
                    'sender':sender,
                    'pdf_data':pdf_data,
                    'message_id':num
                })
    imap.logout()
    kwargs['ti'].xcom_push(key='invoices',value=invoices)
#connect to email and locate pdf
    

def convert_pdf_to_text(**kwargs):
    invoices=kwargs['ti'].xcom_pull(key='invoices',task_ids='fetch_emails')
    processed_invoices=[]

    for invoice in invoices:
        pdf_path=f'/tmp/invoice_{invoice['message_id'].decode()}.pdf'
        with open(pdf_path,'wb') as f:
            f.write(invoice['pdf_data'])
        with pdfplumber.open(pdf_path) as pdf:
            text=""
            for page in pdf.pages:
                text+=page.extract_text() or ""

        processed_invoices.append({
            'subject':invoice['subject'],
            'sender':invoice['sender'],
            'text':text,
            'message_id':invoice['message_id']
        })
        os.remove(pdf.path)
    #parse pdf and extract the data in the pdf
    

def validate_and_create_bill(**kwargs):
    #validate fields in invoice
    processed_invoices = kwargs['ti'].xcom_pull(key='processed_invoices', task_ids='convert_pdf_to_text')
    results = []
    
    # Connect to Odoo
    odoo = odoorpc.ODOO(ODOO_URL)
    odoo.login(ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD)
    
    for invoice in processed_invoices:
        # Mock AI agent validation
        invoice_data = mock_ai_validation(invoice['text'])
        vendor_name = invoice_data.get('vendor_name')
        invoice_number = invoice_data.get('invoice_number')
        amount = invoice_data.get('amount')
        is_valid = invoice_data.get('is_valid', False)
        
        # Fraud prevention: Check if vendor exists
        partner = odoo.env['res.partner'].search([('name', '=', vendor_name)])
        is_new_vendor = not bool(partner)
        
        # Check if PO exists (mocked)
        has_po = check_purchase_order(invoice_number)
        
        # Fraud prevention: Require approval for new vendors or non-PO bills
        if is_new_vendor or not has_po:
            log_audit_event(vendor_name, invoice_number, "Approval required")
            state = 'draft'
        else:
            state = 'posted' if is_valid else 'draft'
        
        # Create bill in Odoo
        bill_vals = {
            'partner_id': partner[0] if partner else create_new_vendor(vendor_name, odoo),
            'invoice_date': datetime.now().strftime('%Y-%m-%d'),
            'ref': invoice_number,
            'amount_total': amount,
            'state': state,
        }
        bill_id = odoo.env['account.move'].create(bill_vals)
        
        results.append({
            'message_id': invoice['message_id'],
            'result': f"Bill {'posted' if state == 'posted' else 'created as draft'} for {vendor_name}, Invoice: {invoice_number}, Amount: {amount}",
            'bill_id': bill_id,
            'sender': invoice['sender']
        })
    
    kwargs['ti'].xcom_push(key='results', value=results)

def mock_ai_validation(text):
    # Mock AI validation logic
    return {
        'vendor_name': 'Sample Vendor',
        'invoice_number': 'INV12345',
        'amount': 1000.00,
        'is_valid': True  # Simplified; real logic would parse text
    }

def create_new_vendor(vendor_name, odoo):
    partner_id = odoo.env['res.partner'].create({
        'name': vendor_name,
        'supplier_rank': 1
    })
    log_audit_event(vendor_name, None, "New vendor created")
    return partner_id

def check_purchase_order(invoice_number):
    # Mock PO check
    return True

def log_audit_event(vendor_name, invoice_number, event):
    logging.info(f"Audit: {event} - Vendor: {vendor_name}, Invoice: {invoice_number}")

def send_reply_email(**kwargs):
    results = kwargs['ti'].xcom_pull(key='results', task_ids='validate_and_create_bill')
    
    email_user = ""
    email_pass = ""
    smtp_server = ""
    smtp_port = 587
    
    for result in results:
        msg = MIMEText(result['result'])
        msg['Subject'] = f"Invoice Processing Result for {result['bill_id']}"
        msg['From'] = email_user
        msg['To'] = result['sender']
        
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.send_message(msg)

def query_bills(**kwargs):
    # Mock conversational UI query (e.g., "Show XYZ’s bills")
    vendor_name = kwargs.get('vendor_name', 'Sample Vendor')  # From chat input
    odoo = odoorpc.ODOO('localhost', port=8069)
    odoo.login(ODOO_DB,ODOO_USERNAME,ODOO_PASSWORD)
    
    partner = odoo.env['res.partner'].search([('name', '=', vendor_name)])
    if partner:
        bills = odoo.env['account.move'].search([('partner_id', '=', partner[0]), ('move_type', '=', 'in_invoice')])
        bill_details = []
        for bill in bills:
            bill_data = odoo.env['account.move'].browse(bill)
            bill_details.append({
                'id': bill,
                'invoice_number': bill_data.ref,
                'amount': bill_data.amount_total,
                'state': bill_data.state
            })
        logging.info(f"Queried bills for {vendor_name}: {bill_details}")
    else:
        logging.info(f"No vendor found: {vendor_name}")

with DAG(
    'invoice_processing_dag',
    default_args=default_args,
    description='Process PDF invoices and create Odoo bills',
    schedule_interval="manual",
    start_date=datetime(2025, 5, 15),
    catchup=False,
) as dag:
    
    fetch_emails_task = PythonOperator(
        task_id='fetch_emails',
        python_callable=fetch_emails,
    )
    
    convert_pdf_task = PythonOperator(
        task_id='convert_pdf_to_text',
        python_callable=convert_pdf_to_text,
    )
    
    validate_and_create_task = PythonOperator(
        task_id='validate_and_create_bill',
        python_callable=validate_and_create_bill,
    )
    
    send_reply_task = PythonOperator(
        task_id='send_reply_email',
        python_callable=send_reply_email,
    )
    
    query_bills_task = PythonOperator(
        task_id='query_bills',
        python_callable=query_bills,
        op_kwargs={'vendor_name': 'Sample Vendor'}  # Mocked; would come from chat
    )
    
    fetch_emails_task >> convert_pdf_task >> validate_and_create_task >> send_reply_task
    validate_and_create_task >> query_bills_task

    

