import pandas as pd
from docx import Document
import re
import os
import zipfile
import datetime
import shutil
from copy import deepcopy

def merge_employee_data_and_zip(excel_file, word_template, output_folder, zip_name=None, batch_size=50):
    """
    Read employee data from Excel, replace repeated placeholders in Word document,
    and create a single zip file containing all documents with improved performance.

    Args:
        excel_file (str): Path to Excel file with employee data
        word_template (str): Path to Word template with placeholders
        output_folder (str): Folder to save generated documents and final zip
        zip_name (str, optional): Name for the zip file. If None, uses current date
        batch_size (int, optional): Number of documents to process in each batch
    """
    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Read Excel data
    df = pd.read_excel(excel_file)

    # Define the mapping between Excel columns and Word placeholders
    placeholder_mapping = {
        'Emp ID': '[Employee ID]',
        'Name': '[Name]',
        'Department': '[Employee Department]',
        'Employee Title': '[Employee Title]',
        'Employee Type': '[Employee Type]',
        '2024 Bonus': '[Bonus in INR]',
        'Basic Salary': '[Basic in INR]',
        'HRA': '[HRA in INR]',
        'Other Allowences': '[Other Allowance in INR]',
        'Provident Fund': '[Provident Fund in INR]',
        'Company Deposit': '[Company Deposit in INR]',
        'Total Fixed': '[Total Fixed in INR]',
        'Bonus 2025 (At Target)': '[Target in INR]',
        'Total CTC': '[Total CTC in INR]'
    }

    # Current date in the format "February 26, 2025"
    current_date = datetime.datetime.now().strftime("%B %d, %Y")

    # Create a temp folder for documents
    docs_folder = os.path.join(output_folder, "temp_docs")
    os.makedirs(docs_folder, exist_ok=True)

    # Load the template document once
    template_doc = Document(word_template)

    # Create ZIP file
    if zip_name is None:
        today = datetime.datetime.now().strftime("%Y%m%d")
        zip_name = f"employee_documents_{today}.zip"
    
    zip_path = os.path.join(output_folder, zip_name)
    
    # Helper function to replace placeholders in paragraphs and table cells
    def replace_placeholders(element, replacements):
        for key, value in replacements.items():
            if key in element.text:
                element.text = element.text.replace(key, value)
    
    # Process in batches
    total_employees = len(df)
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for start_idx in range(0, total_employees, batch_size):
            end_idx = min(start_idx + batch_size, total_employees)
            batch = df.iloc[start_idx:end_idx]
            
            print(f"Processing batch {start_idx//batch_size + 1}: employees {start_idx+1} to {end_idx}")
            
            # Process each employee in the batch
            for _, row in batch.iterrows():
                # Create a deep copy of the template for each employee
                doc = deepcopy(template_doc)
                
                # Prepare all replacements at once
                replacements = {
                    '[Date]': current_date
                }
                
                # Add employee-specific replacements
                for excel_col, word_placeholder in placeholder_mapping.items():
                    if excel_col in df.columns:
                        value = row[excel_col]
                        if pd.notna(value) and value != "":
                            replacements[word_placeholder] = str(value)
                
                # Apply replacements to paragraphs
                for paragraph in doc.paragraphs:
                    replace_placeholders(paragraph, replacements)
                
                # Apply replacements to tables
                for table in doc.tables:
                    for table_row in table.rows:
                        for cell in table_row.cells:
                            # Process cell text directly
                            for p in cell.paragraphs:
                                replace_placeholders(p, replacements)
                
                # Get employee ID for naming files
                emp_id = str(row['Emp ID'])
                safe_emp_id = re.sub(r'[^\w\s-]', '', emp_id).strip().replace(' ', '_')
                
                # Save document directly to a temporary file
                doc_path = os.path.join(docs_folder, f"{safe_emp_id}.docx")
                doc.save(doc_path)
                
                # Add to zip file immediately
                zipf.write(doc_path, arcname=f"{safe_emp_id}.docx")
                
                # Remove the temporary file right away
                os.remove(doc_path)
                
                print(f"  Processed employee ID: {emp_id}")
    
    # Cleanup the temp folder
    if os.path.exists(docs_folder):
        shutil.rmtree(docs_folder)
    
    print(f"Processed {total_employees} employees. All documents saved in single zip file: {zip_path}")
    return zip_path

# Example usage
if __name__ == "__main__":
    excel_file = "employee_data.xlsx"  # Path to your Excel file
    word_template = "template.docx"  # Path to your Word template
    output_folder = "output"  # Folder to save the final zip

    # Create the documents and zip file
    zip_file = merge_employee_data_and_zip(excel_file, word_template, output_folder)
    print(f"ZIP file created at: {zip_file}")
