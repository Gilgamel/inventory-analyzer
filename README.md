# Inventory ABC Classification Analyzer

A Streamlit application designed specifically for Lingxing ERP (领星ERP) inventory data.

This tool performs automated ABC classification, inventory aging analysis, and multi-country reporting. It integrates with a Google Sheets warehouse mapping table and supports one-click Excel export.

---

## Purpose

This project is built for inventory reports exported from Lingxing ERP, especially those containing:

- SKU
- Brand (品牌)
- Warehouse (仓库)
- Total inventory
- Aging quantity and cost fields (e.g., 0~30库龄成本)

Chinese column names are automatically mapped to standardized English field names.

---

## Processing Logic

### 1. Load Warehouse Mapping (Google Sheets)

A static warehouse-region mapping table is loaded from Google Sheets.

Required fields:
- Warehouse
- Country

Optional fields:
- Warehouse Location
- Type
- Description

---

### 2. Upload Lingxing ERP Inventory File

- Excel format (.xlsx / .xls)
- Must contain Warehouse (仓库 or Warehouse)

---

### 3. LEFT JOIN

Inventory.Warehouse = Mapping.Warehouse

This adds:
- Country
- Warehouse Location
- Type
- Description

Warehouse names are normalized (trimmed and converted to uppercase) before matching.

---

### 4. Data Preprocessing

- Rename Chinese columns to English
- Convert numeric columns
- Calculate inventory value by age bands:

Age bands:
- 0–60 days
- 61–90 days
- 91–180 days
- 181–365 days
- 365+ days

Each band calculates:
- Total quantity
- Total inventory value

---

### 5. Modified ABC Classification

Classification is based on cumulative inventory value.

Rules:
- A Class: cumulative ≤ 80%
- B Class: cumulative ≤ 95%
- C Class: remaining items

Special logic:
If an item crosses the 80% or 95% threshold, it is included in the previous class.

ABC classification is calculated:
- At Brand level
- At SKU level (within each Brand)

---

### 6. Output Reports (Per Country)

For each country:

1. Age Summary
2. Brand ABC Classification
3. SKU ABC Classification

All reports are exported into a single Excel file:
- Multiple sheets
- Percentage formatting
- Auto-adjusted column widths

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/inventory-abc-analyzer.git
cd inventory-abc-analyzer
```


### 2. Install Dependencies
```bash
pip install -r requirements.txt
```


### 3. Share Google Sheet Permission

1. Enable the Google Sheets API in Google Cloud Console  
2. Create a Service Account  
3. Share the Warehouse Region Google Sheet with the Service Account email  


### 4. Create App on Streamlit Cloud

1. Login streamlit cloud
2. Click "create app"
3. Select the corresponding repository on GitHub


### 5. Edit Secret on Streamlit Cloud
1. App - Setting - Secrets


