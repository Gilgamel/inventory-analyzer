# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Streamlit** web application that performs ABC classification analysis on Lingxing ERP inventory data. It integrates with Google Sheets for warehouse mapping and exports multi-country Excel reports.

## Running the Application

```bash
# Install dependencies
pip install -r requirements.txt

# Run the Streamlit app
streamlit run inventory_analyzer.py
```

For development with devcontainer, the app automatically runs on port 8501.

## Architecture

The application follows a monolithic single-file structure (`inventory_analyzer.py`). Key sections:

1. **Column Mapping** (lines 24-57): Chinese-to-English column name translation for Lingxing ERP fields
2. **Age Band Definitions** (lines 60-71): Inventory aging bands (0-60, 61-90, 91-180, 181-365, 365+ days)
3. **Google Sheets Connection** (lines 74-99): Warehouse-to-country mapping via service account auth
4. **Data Processing Functions**: Column renaming, numeric conversion, age band calculations
5. **ABC Classification**: Modified Pareto logic (80/95% thresholds) at Brand and SKU levels
6. **Excel Export**: Multi-sheet output with per-country reports

## Key Dependencies

- **streamlit**: Web UI framework
- **pandas/numpy**: Data processing
- **gspread/google-auth**: Google Sheets API integration
- **openpyxl**: Excel file operations

## Configuration

Google Sheets credentials are stored in `.streamlit/secrets.toml` (not committed). The app expects:
- `st.secrets["gcp"]` with service account credentials
- A shared Google Sheet with warehouse mapping (Warehouse, Country columns)

## Processing Pipeline

1. Load warehouse mapping from Google Sheets
2. Upload Lingxing ERP Excel file
3. LEFT JOIN inventory to warehouse mapping
4. Rename Chinese columns to English
5. Calculate inventory value by age bands
6. Apply ABC classification (Brand level + SKU level)
7. Export per-country Excel reports
