# 📊 Inventory ABC Classification Analyzer

A Streamlit application designed specifically for **Lingxing ERP (领星ERP) inventory data**.  

It performs automated **ABC classification**, **inventory aging analysis**, and **multi-country reporting**, with warehouse-country mapping via Google Sheets and one-click Excel export.

---

## 🎯 Purpose

This tool is built for inventory reports exported from **Lingxing ERP**, especially those containing:

- SKU
- Brand
- Warehouse (仓库)
- Total inventory
- Aging quantity & cost fields (e.g., 0~30库龄成本)

Chinese column names are automatically mapped to standardized English field names.

---

## 🔄 Processing Logic

The system follows this workflow:

### 1️⃣ Load Warehouse Mapping (Google Sheets)
- Static mapping table
- Required fields:
  - `Warehouse`
  - `Country`
- Optional:
  - Warehouse Location
  - Type
  - Description

---

### 2️⃣ Upload Lingxing ERP Inventory File
- Excel format (`.xlsx` / `.xls`)
- Must contain warehouse field (`仓库` or `Warehouse`)

---

### 3️⃣ LEFT JOIN
