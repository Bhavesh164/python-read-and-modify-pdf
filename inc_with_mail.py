import pandas as pd
import fitz  # PyMuPDF
import re
import os
import zipfile
import datetime
import shutil
import threading
import queue
import boto3
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

def replace_text_in_pdf(pdf_path, replacements, output_pdf):
    """Replace placeholders in a PDF by redacting old text and inserting new text at the exact position."""
    doc = fitz.open(pdf_path)
    
    for page in doc:
        for key, value in replacements.items():
            text_instances = page.search_for(key)
            for inst in text_instances:
                # Redact the placeholder text
                page.add_redact_annot(inst, text="", fill=(1, 1, 1))  # White fill
                page.apply_redactions()
                
                # Calculate the baseline position of the placeholder text
                baseline_x = inst.x0  # Left edge of the placeholder
                baseline_y = inst.y1 - 4  # Adjust for baseline alignment (4 is a small offset for better alignment)
                
                # Special handling for [Employee Type] if needed
                if key == "[Employee Type]":
                    baseline_y = inst.y1 - 2  # Adjust offset specifically for this placeholder
                
                # Insert the new text at the same position
                page.insert_text(
                    (baseline_x, baseline_y),
                    value,
                    fontsize=10,  # Adjust to match your template's font size
                    color=(0, 0, 0),
                    fontname="helv",  # Adjust to match your template's font if known
                )
    
    doc.save(output_pdf)
    doc.close()

def send_email_worker(email_queue, sender_email, aws_region):
    """Worker function to send emails from a queue using AWS SES."""
    # Create SES client
    ses_client = boto3.client('ses', region_name=aws_region)
    
    while True:
        try:
            email_data = email_queue.get(block=False)
            if email_data is None:  # Sentinel value to stop the worker
                break
                
            recipient_email, subject, body, attachment_path = email_data
            
            # Create email message
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = recipient_email
            msg['Date'] = formatdate(localtime=True)
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach the PDF
            with open(attachment_path, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
                msg.attach(part)
            
            # Send email through SES
            try:
                response = ses_client.send_raw_email(
                    Source=sender_email,
                    Destinations=[recipient_email],
                    RawMessage={'Data': msg.as_string()}
                )
                print(f"Email sent to {recipient_email}, SES MessageId: {response['MessageId']}")
            except ClientError as e:
                print(f"Failed to send email to {recipient_email}: {e.response['Error']['Message']}")
                
        except queue.Empty:
            # No more emails in queue
            break
            
        finally:
            email_queue.task_done()

def merge_employee_data_and_zip(excel_file, pdf_template, output_folder, sender_email=None, aws_region=None, zip_name=None, batch_size=50):
    """
    Read employee data from Excel, replace placeholders in PDF document,
    create a zip file containing all documents, and send individual PDFs via email.
    """
    os.makedirs(output_folder, exist_ok=True)
    df = pd.read_excel(excel_file)
    
    # Check if Email Id column exists for email sending
    send_emails = sender_email is not None and aws_region is not None and 'Email Id' in df.columns
    
    # Placeholder mapping
    placeholder_mapping = {
        '[Employee ID]': 'Emp ID',
        '[Name]': 'Name',
        '[Employee Department]': 'Department',
        '[Employee Title]': 'Employee Title',
        '[Employee Type]': 'Employee Type',
        '[Bonus in INR]': '2024 Bonus',
        '[Basic in INR]': 'Basic Salary',
        '[HRA in INR]': 'HRA',
        '[Other Allowance in INR]': 'Other Allowences',  # Fixed typo
        '[Provident Fund in INR]': 'Provident Fund',
        '[Company Deposit in INR]': 'Company Deposit',
        '[Total Fixed in INR]': 'Total Fixed',
        '[Target in INR]': 'Bonus 2025 (At Target)',
        '[Total CTC in INR]': 'Total CTC'
    }
    
    current_date = datetime.datetime.now().strftime("%B %d, %Y")
    docs_folder = os.path.join(output_folder, "temp_pdfs")
    os.makedirs(docs_folder, exist_ok=True)
    
    if zip_name is None:
        today = datetime.datetime.now().strftime("%Y%m%d")
        zip_name = f"employee_documents_{today}.zip"
    zip_path = os.path.join(output_folder, zip_name)
    
    total_employees = len(df)
    
    # Create a queue for email tasks if email sending is enabled
    email_queue = queue.Queue() if send_emails else None
    
    # Start email worker threads if email sending is enabled
    email_workers = []
    if send_emails:
        num_email_workers = 5  # Number of concurrent email sending threads
        for _ in range(num_email_workers):
            worker = threading.Thread(
                target=send_email_worker,
                args=(email_queue, sender_email, aws_region),
                daemon=True
            )
            worker.start()
            email_workers.append(worker)
    
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for start_idx in range(0, total_employees, batch_size):
            end_idx = min(start_idx + batch_size, total_employees)
            batch = df.iloc[start_idx:end_idx]
            print(f"Processing batch {start_idx//batch_size + 1}: employees {start_idx+1} to {end_idx}")
            
            for _, row in batch.iterrows():
                replacements = {'[Date]': current_date}
                for pdf_placeholder, excel_col in placeholder_mapping.items():
                    if excel_col in df.columns:
                        value = row[excel_col]
                        if pd.notna(value) and value != "":
                            replacements[pdf_placeholder] = str(value)
                        else:
                            replacements[pdf_placeholder] = "N/A"  # Handle missing data
                
                emp_id = str(row['Emp ID'])
                safe_emp_id = re.sub(r'[^\w\s-]', '', emp_id).strip().replace(' ', '_')
                emp_name = str(row['Name'])
                safe_emp_name = re.sub(r'[^\w\s-]', '', emp_name).strip().replace(' ', '_')
                file_name = safe_emp_id+"_"+safe_emp_name
                pdf_output_path = os.path.join(docs_folder, f"{file_name}.pdf")
                
                # Create the PDF
                replace_text_in_pdf(pdf_template, replacements, pdf_output_path)
                
                # Add to zip file
                zipf.write(pdf_output_path, arcname=f"{file_name}.pdf")
                
                # Queue email task if email sending is enabled and email is available
                if send_emails:
                    email = row.get('Email Id')
                    if pd.notna(email) and email != "":
                        # Hardcoded subject
                        subject = "Appraisal Letter"
                        
                        # Email body with name from Excel
                        body = f"Dear {row['Name']},\nPlease find attachment for your apprisal letter."
                        
                        # Add to email queue
                        email_queue.put((email, subject, body, pdf_output_path))
                
                print(f"  Processed employee ID: {emp_id}")
    
    # If email sending is enabled, wait for the queue to process (with a timeout)
    if send_emails:
        # Start a thread to wait for queue completion with timeout
        def wait_with_timeout(queue, timeout):
            try:
                queue.join()
            except Exception as e:
                print(f"Error while waiting for email queue: {str(e)}")
        
        completion_thread = threading.Thread(
            target=wait_with_timeout,
            args=(email_queue, 25),  # 25 second timeout to leave 5 seconds margin
            daemon=True
        )
        completion_thread.start()
        completion_thread.join(timeout=25)  # Wait up to 25 seconds
    
    # Clean up temporary files
    if os.path.exists(docs_folder):
        shutil.rmtree(docs_folder)
    
    print(f"Processed {total_employees} employees. All documents saved in single zip file: {zip_path}")
    if send_emails:
        print("Email sending continues in the background.")
    
    return zip_path

if __name__ == "__main__":
    excel_file = "employee_data.xlsx"
    pdf_template = "template.pdf"
    output_folder = "output"
    
    # AWS SES configuration (for email sending)
    sender_email = "hr@yourcompany.com"  # Must be verified in SES
    aws_region = "us-east-1"  # Your AWS region for SES
    
    # Pass email parameters (omit these to skip email sending)
    zip_file = merge_employee_data_and_zip(
        excel_file, 
        pdf_template, 
        output_folder,
        sender_email=sender_email,
        aws_region=aws_region
    )
    
    print(f"ZIP file created at: {zip_file}")
