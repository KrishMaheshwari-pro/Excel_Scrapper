# NAV Extraction and Mapping Tool

This project automates the extraction and normalization of NAV (Net Asset Value) data from bulk AMC Excel files on GOOGLE COLAB.  
It is designed to standardize data into a single CSV file for website uploads and generate a report of unmatched schemes for manual corrections.



## 🚀 Features
- Extracts NAV-Date pairs from bulk AMC Excel sheets inside a ZIP.
- Skips malformed/unsupported AMCs (defined in code).
- Matches NAV data with **Scheme Codes** using a mapping Excel file.
- Handles multiple NAV-Date blocks in one Excel.
- Detects and excludes "benchmark" columns automatically.
- Produces two clean CSV outputs:
  - `final_revised_output.csv` → Ready-to-upload NAV data.
  - `unmatched_schemes.csv` → Schemes that failed to match, with diagnostic columns.



## 📂 Inputs
The program requires three main inputs:
1. **ZIP file** → Contains AMC folders with Excel files.  
2. **Baseline NAV CSV** → Template in which final data should be appended.  
3. **Mapping Excel(s)** → One or more `.xlsx` files containing:
   - `Folder Name` (AMC name, matching ZIP folder names)  
   - `Scheme Name`  
   - `Scheme Code`  

> ⚠️ Note: Some AMCs (e.g., `abc`, `xyz`, `pqr`) are excluded automatically because of inconsistent formats.


## 📊 Outputs
1. **`final_revised_output.csv`**  
   - Standardized NAV data (SchemeCode, Date, NAV).  
   - Format ready for direct website upload.  

2. **`unmatched_schemes.csv`**  
   - A diagnostic report listing schemes that were not matched.  
   - Contains:
     - `Excluded Folder` → If AMC was intentionally skipped.  
     - `Folder Not Available` → If AMC folder not found in ZIP.  
     - Remaining unmatched schemes → Require manual fixing.  


## ⚙️ How It Works
1. Upload three files in Google Colab:
   - ZIP of Excels  
   - Baseline CSV  
   - Mapping Excel(s)  
2. Code extracts Excel data, normalizes headers, and removes benchmark columns.  
3. Attempts to match NAV data blocks with Scheme Codes:
   - First by folder name → then by scheme name → then by sheet text → fallback to folder name.  
4. Produces output CSVs for matched and unmatched schemes.  

---

## 📝 User Guide
- Prepare mapping Excel correctly:
  - Folder Name must **exactly match** ZIP folder name.  
  - Scheme Names should be trimmed/normalized (avoid unnecessary words).  
- If NAV is extracted but Scheme Code isn’t matched:
  - Code falls back to folder name.  
  - Manually edit unmatched CSV to assign correct Scheme Code.  
- Use unmatched_schemes.csv to diagnose missing mappings:
  - **Excluded Folder** → Skipped AMC.  
  - **Folder Not Available** → Folder name mismatch.  
  - Remaining → Scheme name spelling issues or multiple NAV blocks inside one Excel.  



## 📦 Example
Repository includes:
- `example_zip/` → Sample AMC Excel ZIP.  
- `NAV_file.csv` → Example baseline NAV file.  
- `mapping_example.xlsx` → Example mapping sheet.  
- `USER GUIDE.docx` → Detailed user guide.  


## 🔧 Requirements
- Python 3.x  
- Google Colab / Jupyter Notebook  
- Libraries: `pandas`, `openpyxl`, `zipfile`, `collections`  

## 🖊️ Author
Developed during internship project for NAV data automation.  
