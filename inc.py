import pandas as pd
import fitz  # PyMuPDF
import re
import os
import zipfile
import datetime
import shutil

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

def merge_employee_data_and_zip(excel_file, pdf_template, output_folder, zip_name=None, batch_size=50):
    """
    Read employee data from Excel, replace placeholders in PDF document,
    and create a single zip file containing all documents.
    """
    os.makedirs(output_folder, exist_ok=True)
    df = pd.read_excel(excel_file)
    
    # Verify these column names match your Excel file's actual column names
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
                
                replace_text_in_pdf(pdf_template, replacements, pdf_output_path)
                zipf.write(pdf_output_path, arcname=f"{file_name}.pdf")
                os.remove(pdf_output_path)
                print(f"  Processed employee ID: {emp_id}")
    
    if os.path.exists(docs_folder):
        shutil.rmtree(docs_folder)
    print(f"Processed {total_employees} employees. All documents saved in single zip file: {zip_path}")
    return zip_path

if __name__ == "__main__":
    excel_file = "employee_data.xlsx"
    pdf_template = "template.pdf"
    output_folder = "output"
    zip_file = merge_employee_data_and_zip(excel_file, pdf_template, output_folder)
    print(f"ZIP file created at: {zip_file}")
