from fastapi import FastAPI, Depends, HTTPException, File, UploadFile, Form, status, Request, BackgroundTasks
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import pandas as pd
import fitz  # PyMuPDF
import re
import os
import zipfile
import datetime
import shutil
import secrets
from typing import Dict, Optional
import jwt
from datetime import timedelta
from passlib.context import CryptContext
import concurrent.futures
import smtplib
import os
from email.message import EmailMessage
import queue
import threading

# Initialize FastAPI app
app = FastAPI(title="PDF Document Processor", description="API for processing employee documents")

# Setup templates and static files
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Security configurations
SECRET_KEY = secrets.token_hex(32)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Define the upload directory
UPLOAD_DIR = "uploads"
OUTPUT_DIR = "output"
TEMPLATES_DIR = "templates"
STATIC_DIR = "static"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TEMPLATES_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)

# Create HTML templates
login_html = """
<!DOCTYPE html>
<html>
<head>
    <title>PDF Document Processor - Login</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            background-color: #f4f4f4;
            margin: 0;
            padding: 0;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
        }
        .container {
            background-color: white;
            padding: 30px;
            border-radius: 5px;
            box-shadow: 0px 0px 10px rgba(0,0,0,0.1);
            width: 350px;
        }
        h1 {
            text-align: center;
            color: #333;
        }
        .form-group {
            margin-bottom: 20px;
        }
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
        }
        input[type="text"],
        input[type="password"] {
            width: 100%;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 4px;
            box-sizing: border-box;
        }
        button {
            background-color: #4CAF50;
            color: white;
            padding: 12px 20px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            width: 100%;
            font-size: 16px;
        }
        button:hover {
            background-color: #45a049;
        }
        .error-message {
            color: red;
            margin-bottom: 15px;
            text-align: center;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Login</h1>
        {% if error %}
        <div class="error-message">{{ error }}</div>
        {% endif %}
        <form action="/login" method="post">
            <div class="form-group">
                <label for="username">Username:</label>
                <input type="text" id="username" name="username" required>
            </div>
            <div class="form-group">
                <label for="password">Password:</label>
                <input type="password" id="password" name="password" required>
            </div>
            <button type="submit">Login</button>
        </form>
    </div>
</body>
</html>
"""

upload_html = """
<!DOCTYPE html>
<html>
<head>
    <title>PDF Document Processor - Upload Files</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            background-color: #f4f4f4;
            margin: 0;
            padding: 0;
        }
        .container {
            max-width: 800px;
            margin: 50px auto;
            background-color: white;
            padding: 30px;
            border-radius: 5px;
            box-shadow: 0px 0px 10px rgba(0,0,0,0.1);
        }
        h1 {
            text-align: center;
            color: #333;
            margin-bottom: 30px;
        }
        .form-group {
            margin-bottom: 20px;
        }
        label {
            display: block;
            margin-bottom: 8px;
            font-weight: bold;
        }
        .file-input {
            display: block;
            width: 100%;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 4px;
            box-sizing: border-box;
        }
        button {
            background-color: #4CAF50;
            color: white;
            padding: 12px 20px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
        }
        button:hover {
            background-color: #45a049;
        }
        .logout-btn {
            background-color: #f44336;
            float: right;
        }
        .logout-btn:hover {
            background-color: #d32f2f;
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }
        .error-message {
            color: red;
            margin-bottom: 15px;
        }
        .success-message {
            color: green;
            margin-bottom: 15px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>PDF Document Processor</h1>
            <a href="/logout"><button class="logout-btn">Logout</button></a>
        </div>

        {% if error %}
        <div class="error-message">{{ error }}</div>
        {% endif %}

        {% if success %}
        <div class="success-message">{{ success }}</div>
        {% endif %}

        <form action="/upload" method="post" enctype="multipart/form-data">
            <div class="form-group">
                <label for="excel_file">Excel Data File:</label>
                <input type="file" id="excel_file" name="excel_file" class="file-input" accept=".xls,.xlsx" required>
            </div>
            <button type="submit">Process Documents</button>
        </form>
    </div>
</body>
</html>
"""

# Define user model
class User(BaseModel):
    username: str
    disabled: Optional[bool] = None

class UserInDB(User):
    hashed_password: str

# Demo users database (in production, use a real database)
fake_users_db = {
    "admin": {
        "username": "admin",
        "hashed_password": pwd_context.hash("adminpassword"),
        "disabled": False,
    }
}

# Token model
class Token(BaseModel):
    access_token: str
    token_type: str

# OAuth2 Bearer token scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

# Authentication functions
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)
    return None

def authenticate_user(fake_db, username: str, password: str):
    user = get_user(fake_db, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.datetime.now() + expires_delta
    else:
        expire = datetime.datetime.now() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(request: Request, token: str = Depends(oauth2_scheme)):
    if token is None:
        # Check for token in cookies
        session_token = request.cookies.get("access_token")
        if not session_token:
            return None
        token = session_token

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            return None
    except jwt.PyJWTError:
        return None

    user = get_user(fake_users_db, username)
    if user is None:
        return None

    return user

async def get_current_active_user(current_user: User = Depends(get_current_user)):
    if current_user is None or current_user.disabled:
        return None
    return current_user

# PDF processing functions
def replace_text_in_pdf(pdf_path, replacements, output_pdf, texts_to_remove, dynamic_column_value, bonus_column_value, bonus_column_value2):
    """Replace placeholders in a PDF by redacting old text and inserting new text at the exact position."""
    doc = fitz.open(pdf_path)

    # Handle special replacements based on dynamic_column_value
    if dynamic_column_value == 'sdr':
        replacements['[-]'] = '=>'
        replacements['[--]'] = ''
    elif dynamic_column_value == 'comments':
        replacements['[--]'] = '=>'
        replacements['[-]'] = ''
    elif dynamic_column_value == 'both':
        replacements['[--]'] = '=>'
        replacements['[-]'] = '=>'
    else:
        replacements['[--]'] = ''
        replacements['[-]'] = ''

    # Handle bonus values
    if bonus_column_value is None or replacements.get('[Bonus in INR]', '') == '':
        replacements['[Bonus in INR]'] = ''
        replacements['II'] = 'I'
        replacements['I.'] = ''
        replacements['Your 2024 Bonus is'] = ''

    if bonus_column_value2 is None or replacements.get('[Target in INR]', '') == '':
        replacements['=> Bonus (at Target)3'] = ''
        replacements['[Target in INR]'] = ''

    # Combine replacements with texts_to_remove
    all_replacements = {**replacements, **texts_to_remove}

    for page in doc:
        for key, value in all_replacements.items():
            text_instances = page.search_for(key)

            for inst in text_instances:
                # First redact the original text
                page.add_redact_annot(inst, text="", fill=(1, 1, 1))
                page.apply_redactions()

                # Then insert the new text with specified font properties
                if value:
                    baseline_x = inst.x0
                    baseline_y = inst.y1 - 4  # Default offset

                    # Adjust baseline for Employee Type
                    if key == "[Employee Type]":
                        baseline_y = inst.y1 - 2

                    # Special handling for 'II' replacement
                    if key == 'II.':
                        page.insert_text(
                            point=(baseline_x, baseline_y - 2),
                            text=value,
                            fontsize=11,
                            fontname="helv-b",  # Bold Helvetica
                            color=(0, 0, 0)
                        )
                    else:
                        page.insert_text(
                            point=(baseline_x, baseline_y),
                            text=value,
                            fontsize=10,
                            fontname="helv",  # Regular Helvetica
                            color=(0, 0, 0)
                        )

    doc.save(output_pdf)
    doc.close()

def format_indian_currency(value):
    """Format a numeric value in Indian currency style with INR prefix."""
    try:
        # Handle empty or invalid values
        if pd.isna(value) or str(value).strip().lower() in ['nan', '', 'na', 'n/a']:
            return ""

        # Convert to float, handling potential string inputs
        numeric_value = float(str(value).replace(',', '').strip())

        # Special handling for zero
        if numeric_value == 0:
            return "INR 0.00"

        # Convert to string with two decimal places
        formatted = f"{numeric_value:,.2f}"

        # Return early if formatting fails
        if '.' not in formatted:
            return ""

        whole_number = formatted.split('.')[0].replace(',', '')
        decimal_part = formatted.split('.')[1]

        # Handle numbers less than 1000
        if len(whole_number) <= 3:
            return f"INR {whole_number}.{decimal_part}"

        # Format according to Indian numbering system
        last_three = whole_number[-3:]
        remaining = whole_number[:-3]

        groups = []
        while remaining:
            groups.append(remaining[-2:] if len(remaining) >= 2 else remaining)
            remaining = remaining[:-2]

        formatted_whole = last_three
        if groups:
            formatted_whole = ','.join(groups[::-1]) + ',' + formatted_whole

        return f"INR {formatted_whole}.{decimal_part}"

    except (ValueError, TypeError, IndexError, AttributeError):
        return ""

def process_record(row_dict, pdf_template, docs_folder, current_date, placeholder_mapping):
    """Helper function to process a single record with Indian currency formatting."""
    replacements = {'[Date]': current_date}

    # Get SDR and Comments values
    sdr_value = str(row_dict.get('For SDR only', '')).strip()
    comments_value = str(row_dict.get('Comments (Optional)', '')).strip()

    # Handle bonus values safely
    bonus_value = str(row_dict.get('2024 Bonus', '')).strip()
    bonus_value_2 = str(row_dict.get('Bonus 2025 (At Target)', '')).strip()

    # Initialize values
    texts_to_remove = {}
    dynamic_column_value = 'both'
    bonus_column_value = 'fill'
    bonus_column_value2 = 'fill'

    # Handle SDR and Comments logic
    if sdr_value.lower() not in ['nan', '', 'na', 'n/a'] and not pd.isna(row_dict.get('For SDR only')):
        dynamic_column_value = 'sdr'
        texts_to_remove = {
            "[For SDRs only]": str(sdr_value),
            "[Any other employee-specific details that need to be covered in Appraisal Letter]": ""
        }
    elif comments_value.lower() not in ['nan', '', 'na', 'n/a'] and not pd.isna(row_dict.get('Comments (Optional)')):
        dynamic_column_value = 'comments'
        texts_to_remove = {
            "[Any other employee-specific details that need to be covered in Appraisal Letter]": str(comments_value)
        }
    else:
        dynamic_column_value = 'none'
        texts_to_remove = {
            "[Any other employee-specific details that need to be covered in Appraisal Letter]": ""
        }

    # Handle bonus values
    if bonus_value.lower() in ['nan', '', 'na', 'n/a'] or pd.isna(row_dict.get('2024 Bonus')):
        bonus_column_value = None

    if bonus_value_2.lower() in ['nan', '', 'na', 'n/a'] or pd.isna(row_dict.get('Bonus 2025 (At Target)')):
        bonus_column_value2 = None

    # Process all replacements
    for pdf_placeholder, excel_col in placeholder_mapping.items():
        value = row_dict.get(excel_col, "N/A")

        # Special handling for currency columns
        currency_columns = [
            '2024 Bonus', 'Basic Salary', 'HRA', 'Other Allowences',
            'Provident Fund', 'Company Deposit', 'Total Fixed',
            'Bonus 2025 (At Target)', 'Total CTC'
        ]

        if excel_col in currency_columns:
            formatted_value = format_indian_currency(value)
            replacements[pdf_placeholder] = formatted_value
        else:
            if pd.notna(value) and str(value).strip() != "":
                replacements[pdf_placeholder] = str(value)
            else:
                replacements[pdf_placeholder] = ""

    # Generate output file name
    emp_id = str(row_dict.get('Emp ID', ''))
    safe_emp_id = re.sub(r'[^\w\s-]', '', emp_id).strip().replace(' ', '_')
    emp_name = str(row_dict.get('Name', ''))
    safe_emp_name = re.sub(r'[^\w\s-]', '', emp_name).strip().replace(' ', '_')
    file_name = safe_emp_id + "_" + safe_emp_name
    pdf_output_path = os.path.join(docs_folder, f"{file_name}.pdf")

    replace_text_in_pdf(pdf_template, replacements, pdf_output_path, texts_to_remove, dynamic_column_value, bonus_column_value, bonus_column_value2)
    return pdf_output_path, f"{file_name}.pdf"

def send_office365_email(recipient_email, pdf_path, emp_name):
    """Worker function to send a single email"""
    print(f"\nAttempting to send email to {recipient_email}")
    try:
        msg = EmailMessage()
        msg["From"] = "hrd@algoworks.com"
        msg["To"] = recipient_email
        msg["Subject"] = "Appraisal Letter"
        msg.set_content(f"Dear {emp_name},\nPlease find attachment for your appraisal letter.")

        # Attach PDF
        print(f"Attaching PDF: {pdf_path}")
        with open(pdf_path, "rb") as f:
            file_data = f.read()
            file_name = os.path.basename(pdf_path)
            msg.add_attachment(file_data, maintype="application", subtype="octet-stream", filename=file_name)

        # Send Email with debug enabled
        print("Connecting to SMTP server...")
        with smtplib.SMTP("smtp.office365.com", 587) as server:
            server.set_debuglevel(1)  # Enable debug output
            print("Starting TLS...")
            server.starttls()
            print("Logging in...")
            server.login("hrd@algoworks.com", "netscape@1")
            print("Sending message...")
            server.send_message(msg)
            print(f"✓ SUCCESS: Email sent to {recipient_email}")
            return True
    except FileNotFoundError:
        print(f"✗ ERROR: PDF file not found: {pdf_path}")
        return False
    except smtplib.SMTPAuthenticationError:
        print("✗ ERROR: SMTP Authentication failed. Check username and password.")
        return False
    except smtplib.SMTPException as e:
        print(f"✗ ERROR: SMTP error occurred: {str(e)}")
        return False
    except Exception as e:
        print(f"✗ ERROR: Failed to send email to {recipient_email}: {str(e)}")
        return False

def email_worker(email_queue):
    """Worker function to process email queue"""
    print("\nEmail worker started...")
    emails_processed = 0
    emails_succeeded = 0
    emails_failed = 0

    while True:
        try:
            email_data = email_queue.get(timeout=30)  # Wait up to 30 seconds for new items
            if email_data is None:  # Sentinel value to stop worker
                print("Received stop signal, finishing up...")
                break

            recipient_email, pdf_path, emp_name = email_data
            emails_processed += 1
            print(f"\nProcessing email {emails_processed}...")

            if send_office365_email(recipient_email, pdf_path, emp_name):
                emails_succeeded += 1
            else:
                emails_failed += 1

            email_queue.task_done()

        except queue.Empty:
            print("Email queue empty, worker finishing...")
            break
        except Exception as e:
            print(f"✗ ERROR in email worker: {str(e)}")
            emails_failed += 1
            email_queue.task_done()

    print(f"\nEmail worker finished:")
    print(f"Total emails processed: {emails_processed}")
    print(f"Successful: {emails_succeeded}")
    print(f"Failed: {emails_failed}")

# Define the mapping between PDF placeholders and Excel columns
placeholder_mapping = {
    '[Employee ID]': 'Emp ID',
    '[Name]': 'Name',
    '[Employee Department]': 'Department',
    '[Employee Title]': 'Employee Title',
    '[Employee Type]': 'Employee Type',
    '[Bonus in INR]': '2024 Bonus',
    '[Basic in INR]': 'Basic Salary',
    '[HRA in INR]': 'HRA',
    '[Other Allowance in INR]': 'Other Allowences',
    '[Provident Fund in INR]': 'Provident Fund',
    '[Company Deposit in INR]': 'Company Deposit',
    '[Total Fixed in INR]': 'Total Fixed',
    '[Target in INR]': 'Bonus 2025 (At Target)',
    '[Total CTC in INR]': 'Total CTC',
    '[For SDRs only]': 'For SDR only',
    '[Any other employee-specific details that need to be covered in Appraisal Letter]': 'Comments (Optional)'
}

def merge_employee_data_and_zip(excel_file_path, pdf_template, output_folder, zip_name=None):
    """Main function to process Excel and create ZIP"""
    print("\nStarting merge and zip process...")
    os.makedirs(output_folder, exist_ok=True)
    df = pd.read_excel(excel_file_path)

    if zip_name is None:
        today = datetime.datetime.now().strftime("%Y%m%d")
        zip_name = f"employee_documents_{today}.zip"
    zip_path = os.path.join(output_folder, zip_name)

    docs_folder = os.path.join(output_folder, "temp_pdfs")
    os.makedirs(docs_folder, exist_ok=True)

    # Keep track of files to email
    email_tasks = []
    generated_pdfs = []

    try:
        # First, generate all PDFs and create ZIP
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for _, row in df.iterrows():
                # Process single record
                pdf_output_path, arcname = process_record(
                    row.to_dict(),
                    pdf_template,
                    docs_folder,
                    datetime.datetime.now().strftime("%B %d, %Y"),
                    placeholder_mapping
                )

                # Add to ZIP
                zipf.write(pdf_output_path, arcname=arcname)
                generated_pdfs.append(pdf_output_path)

                # Store email task if Email Id exists
                email = row.get('Email Id')
                if pd.notna(email) and email.strip():
                    email_tasks.append((email.strip(), pdf_output_path, row['Name']))

        # Now that all PDFs are generated, start email process
        if email_tasks:
            # Create email queue and start worker thread
            email_queue = queue.Queue()
            print("\nStarting email worker thread...")
            email_thread = threading.Thread(
                target=email_worker,
                args=(email_queue,),
                daemon=True
            )
            email_thread.start()

            # Queue all email tasks
            for email_task in email_tasks:
                print(f"\nQueuing email for: {email_task[0]}")
                email_queue.put(email_task)

            # Signal email worker to stop
            print("Adding stop signal to email queue...")
            email_queue.put(None)

            # Wait for email worker to finish (with timeout)
            print("Waiting for email worker to finish...")
            email_thread.join(timeout=120)  # Increased timeout to 2 minutes

    except Exception as e:
        print(f"Error during processing: {str(e)}")
        raise
    finally:
        # Clean up PDF files only after emails are sent
        for pdf_file in generated_pdfs:
            if os.path.exists(pdf_file):
                try:
                    os.remove(pdf_file)
                except Exception as e:
                    print(f"Warning: Could not delete temporary file {pdf_file}: {str(e)}")

        # Clean up temp folder
        try:
            if os.path.exists(docs_folder):
                shutil.rmtree(docs_folder)
        except Exception as e:
            print(f"Warning: Could not delete temporary folder {docs_folder}: {str(e)}")

    print("\nZIP file created successfully.")
    return zip_path

# Save HTML templates
def create_template_files():
    with open(os.path.join(TEMPLATES_DIR, "login.html"), "w") as f:
        f.write(login_html)
    with open(os.path.join(TEMPLATES_DIR, "upload.html"), "w") as f:
        f.write(upload_html)

# Routes for web interface
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, current_user: User = Depends(get_current_active_user)):
    if current_user is not None:
        return templates.TemplateResponse("upload.html", {"request": request})
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    user = authenticate_user(fake_users_db, username, password)
    if not user:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Invalid username or password"}
        )

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )

    response = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )
    return response

@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    response.delete_cookie(key="access_token")
    return response

@app.post("/upload")
async def upload_files(
    request: Request,
    background_tasks: BackgroundTasks,  # Add background tasks dependency
    excel_file: UploadFile = File(...),
    current_user: User = Depends(get_current_active_user)
):
    if current_user is None:
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    # Check file extensions
    if not excel_file.filename.endswith(('.xls', '.xlsx')):
        return templates.TemplateResponse(
            "upload.html",
            {
                "request": request,
                "error": "Only Excel files (.xls, .xlsx) are accepted"
            }
        )

    # Save uploaded file temporarily
    excel_path = os.path.join(UPLOAD_DIR, excel_file.filename)

    try:
        with open(excel_path, "wb") as f:
            f.write(await excel_file.read())

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_filename = f"employee_documents_{timestamp}.zip"
        zip_path = merge_employee_data_and_zip(
            excel_path,
            'template.pdf',
            OUTPUT_DIR,
            zip_name=zip_filename
        )

        # Schedule deletion of the ZIP file after sending response
        background_tasks.add_task(os.remove, zip_path)

        return FileResponse(
            path=zip_path,
            filename=zip_filename,
            media_type="application/zip"
        )

    except Exception as e:
        return templates.TemplateResponse(
            "upload.html",
            {
                "request": request,
                "error": f"Error processing files: {str(e)}"
            }
        )
    finally:
        if os.path.exists(excel_path):
            os.remove(excel_path)

# Register the startup event handler and create templates when app starts
app.add_event_handler("startup", create_template_files)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)
