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
def replace_text_in_pdf(pdf_path, replacements, output_pdf,texts_to_remove, dynamic_column_value, bonus_column_value, bonus_column_value2):
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

    # Handle bonus value
    if bonus_column_value is None:
        replacements['[Bonus in INR]'] = ''
        replacements['II.'] = 'I.'
        replacements['I.'] = ''
        replacements['Your 2024 Bonus is'] = ''
    if bonus_column_value2 is None:
        replacements['=> Bonus (at Target)3'] = ''
        replacements['[Target in INR]'] = ''

    for page in doc:
        for key, value in replacements.items():
            text_instances = page.search_for(key)
            for inst in text_instances:
                # Redact the placeholder text
                page.add_redact_annot(inst, text="", fill=(1, 1, 1))  # White fill
                page.apply_redactions()

                # Calculate the baseline position of the placeholder text
                baseline_x = inst.x0  # Left edge of the placeholder
                baseline_y = inst.y1 - 4  # Adjust for baseline alignment

                # Special handling for [Employee Type] if needed
                if key == "[Employee Type]":
                    baseline_y = inst.y1 - 2  # Specific adjustment

                # Insert the new text at the same position
                page.insert_text(
                    (baseline_x, baseline_y),
                    value,
                    fontsize=10,  # Adjust to match your template's font size
                    color=(0, 0, 0),
                    fontname="helv",  # Adjust to match your template's font if known
                )

            # Get all text blocks on the page
            blocks = page.get_text("blocks")
            if texts_to_remove:
                # Handle specific text replacements
                for block in blocks:
                    block_text = block[4].strip()  # Get the text content of the block
                    for text, replacement in texts_to_remove.items():
                        # Check if the block exactly matches the text to remove/replace
                        if block_text == text:
                            # Create rectangle from block coordinates
                            rect = fitz.Rect(block[:4])
                            # Redact the original text
                            page.add_redact_annot(rect, text="", fill=(1, 1, 1))
                            page.apply_redactions()

                            # If there's a replacement text (not empty string), insert it
                            if replacement:
                                baseline_x = rect.x0
                                baseline_y = rect.y1 - 4  # Adjust baseline for text alignment
                                page.insert_text(
                                    (baseline_x, baseline_y),
                                    replacement,
                                    fontsize=10,
                                    color=(0, 0, 0),
                                    fontname="helv"
                                )


    doc.save(output_pdf)
    doc.close()

def format_indian_currency(value):
    """
    Format a numeric value in Indian currency style with INR prefix.
    Examples: 1,000; 10,000; 1,00,000; 10,00,000; 1,00,00,000

    Args:
        value (float or int or str): Numeric value to format

    Returns:
        str: Formatted currency string with INR prefix
    """
    try:
        # Convert to float, handling potential string inputs
        numeric_value = float(value)

        # Special handling for zero
        if numeric_value == 0:
            return "INR 0.00"

        # Convert to string with two decimal places
        whole_number = f"{numeric_value:,.2f}".split('.')[0].replace(',', '')
        decimal_part = f"{numeric_value:,.2f}".split('.')[1]

        # Handle numbers less than 1000
        if len(whole_number) <= 3:
            return f"INR {whole_number}.{decimal_part}"

        # Format according to Indian numbering system
        # First, get the last 3 digits
        last_three = whole_number[-3:]
        # Get remaining digits
        remaining = whole_number[:-3]

        # Group remaining digits by 2 from right
        groups = []
        while remaining:
            groups.append(remaining[-2:] if len(remaining) >= 2 else remaining)
            remaining = remaining[:-2]

        # Combine all parts
        formatted_whole = last_three
        if groups:
            formatted_whole = ','.join(groups[::-1]) + ',' + formatted_whole

        return f"INR {formatted_whole}.{decimal_part}"

    except (ValueError, TypeError):
        return "INR N/A"

def process_record(row_dict, pdf_template, docs_folder, current_date, placeholder_mapping):
    """
    Helper function to process a single record with Indian currency formatting.
    """
    replacements = {'[Date]': current_date}

    # Get SDR and Comments values
    sdr_value = str(row_dict.get('For SDR only', '')).strip()
    comments_value = str(row_dict.get('Comments (Optional)', '')).strip()
    bonus_value = str(row_dict.get('2024 Bonus', '')).strip()
    bonus_value_2 = str(row_dict.get('Bonus 2025 (At Target)', '')).strip()

    print(f"SDR Value: '{sdr_value}'")  # Debug print
    print(f"Comments Value: '{comments_value}'")  # Debug print

    # Initialize texts_to_remove dictionary
    texts_to_remove = {}
    dynamic_column_value = 'both'
    bonus_column_value = 'fill'
    bonus_column_value2 = 'fill'

    # Check if SDR value exists and is not empty/nan
    if sdr_value.lower() not in ['nan', '', 'na', 'n/a'] and not pd.isna(row_dict.get('For SDR only')) and \
         (comments_value.lower() in ['nan', '', 'na', 'n/a'] or pd.isna(row_dict.get('Comments (Optional)'))):
        dynamic_column_value = 'sdr'
        texts_to_remove = {
            "[For SDRs only]": str(sdr_value),
            "[Any other employee-specific details that need to be covered in Appraisal Letter]": ""
        }
    # Check if Comments value exists but SDR is empty
    elif (comments_value.lower() not in ['nan', '', 'na', 'n/a'] and not pd.isna(row_dict.get('Comments (Optional)'))) and \
         (sdr_value.lower() in ['nan', '', 'na', 'n/a'] or pd.isna(row_dict.get('For SDR only'))):
        dynamic_column_value = 'comments'
        texts_to_remove = {
            "[Any other employee-specific details that need to be covered in Appraisal Letter]": str(comments_value)
        }
    # If both SDR and Comments are empty/nan
    elif (sdr_value.lower() in ['nan', '', 'na', 'n/a'] or pd.isna(row_dict.get('For SDR only'))) and \
         (comments_value.lower() in ['nan', '', 'na', 'n/a'] or pd.isna(row_dict.get('Comments (Optional)'))):
        dynamic_column_value = 'none'
        texts_to_remove = {
            "[Any other employee-specific details that need to be covered in Appraisal Letter]": ""
        }

    # If bonus 2024 are empty/nan
    if (bonus_value.lower() in ['nan', '', 'na', 'n/a'] or pd.isna(row_dict.get('2024 Bonus'))):
        bonus_column_value = None
    # If bonus 2025 at target are empty/nan
    if (bonus_value_2.lower() in ['nan', '', 'na', 'n/a'] or pd.isna(row_dict.get('Bonus 2025 (At Target)'))):
        bonus_column_value2 = None




    for pdf_placeholder, excel_col in placeholder_mapping.items():
        value = row_dict.get(excel_col, "N/A")

        # Special handling for numeric/currency columns
        currency_columns = [
            '2024 Bonus', 'Basic Salary', 'HRA', 'Other Allowences',
            'Provident Fund', 'Company Deposit', 'Total Fixed',
            'Bonus 2025 (At Target)', 'Total CTC'
        ]

        if excel_col in currency_columns:
            replacements[pdf_placeholder] = format_indian_currency(value)
        else:
            if pd.notna(value) and value != "":
                replacements[pdf_placeholder] = str(value)
            else:
                replacements[pdf_placeholder] = "" # empty the placeholder values

    emp_id = str(row_dict.get('Emp ID', ''))
    safe_emp_id = re.sub(r'[^\w\s-]', '', emp_id).strip().replace(' ', '_')
    emp_name = str(row_dict.get('Name', ''))
    safe_emp_name = re.sub(r'[^\w\s-]', '', emp_name).strip().replace(' ', '_')
    file_name = safe_emp_id + "_" + safe_emp_name
    pdf_output_path = os.path.join(docs_folder, f"{file_name}.pdf")

    replace_text_in_pdf(pdf_template, replacements, pdf_output_path, texts_to_remove, dynamic_column_value, bonus_column_value, bonus_column_value2)
    return pdf_output_path, f"{file_name}.pdf"

def merge_employee_data_and_zip(excel_file_path, pdf_template, output_folder, zip_name=None):
    """
    Read employee data from Excel, replace placeholders in PDF document,
    and create a single zip file containing all documents.
    This version processes each record concurrently.
    """
    os.makedirs(output_folder, exist_ok=True)
    df = pd.read_excel(excel_file_path)

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
        '[Other Allowance in INR]': 'Other Allowences',  # Fixed typo
        '[Provident Fund in INR]': 'Provident Fund',
        '[Company Deposit in INR]': 'Company Deposit',
        '[Total Fixed in INR]': 'Total Fixed',
        '[Target in INR]': 'Bonus 2025 (At Target)',
        '[Total CTC in INR]': 'Total CTC',
        '[For SDRs only]': 'For SDR only',
        '[Any other employee-specific details that need to be covered in Appraisal Letter]': 'Comments (Optional)'
    }

    # Validate required columns
    required_columns = list(placeholder_mapping.values())
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Excel file is missing required columns: {', '.join(missing_columns)}")

    current_date = datetime.datetime.now().strftime("%B %d, %Y")
    docs_folder = os.path.join(output_folder, "temp_pdfs")
    os.makedirs(docs_folder, exist_ok=True)

    if zip_name is None:
        today = datetime.datetime.now().strftime("%Y%m%d")
        zip_name = f"employee_documents_{today}.zip"
    zip_path = os.path.join(output_folder, zip_name)

    # Use a ProcessPoolExecutor to process each record concurrently
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = []
            for _, row in df.iterrows():
                # Pass the row as a dictionary to the helper function
                futures.append(executor.submit(process_record, row.to_dict(), pdf_template, docs_folder, current_date, placeholder_mapping))
            for future in concurrent.futures.as_completed(futures):
                pdf_output_path, arcname = future.result()
                zipf.write(pdf_output_path, arcname=arcname)
                os.remove(pdf_output_path)

    if os.path.exists(docs_folder):
        shutil.rmtree(docs_folder)
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
    uvicorn.run(app, host="0.0.0.0", port=8000)
