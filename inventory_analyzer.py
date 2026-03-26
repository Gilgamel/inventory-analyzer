import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import numpy as np
import json
from io import BytesIO
from openpyxl import load_workbook
from openpyxl.styles import NamedStyle
from openpyxl.utils.dataframe import dataframe_to_rows
from inventory_snapshot import load_snapshots, get_snapshot_dataframe, save_snapshot

# Page configuration
st.set_page_config(
    page_title="Inventory ABC Classification Analyzer",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("📊 Inventory ABC Classification Analyzer")
st.markdown("---")

# ========== 1. Column name mapping (Your original column names) ==========
COLUMN_MAPPING = {
    '品名': 'Product_Name',
    'SKU': 'SKU',
    '仓库': 'Warehouse',
    '数据层级': 'Data_Level',
    '分类': 'Category',
    '品牌': 'Brand',
    '总库存': 'Total_Inventory',
    '可用量': 'Available_Qty',
    '预留量/锁定量': 'Reserved_Qty',
    '次品量': 'Defect_Qty',
    '待检待上架量': 'Pending_Inspection',
    '调拨在途': 'Transfer_Transit',
    'FBA标发在途数量': 'FBA_Transit',
    'FBA计划入库数量': 'FBA_Planned',
    '待到货量': 'Expected_Receipt',
    '预计库存': 'Projected_Inventory',
    '0~30库龄数量': 'Age_0_30_Qty',
    '0~30库龄成本': 'Age_0_30_Cost',
    '31~60库龄数量': 'Age_31_60_Qty',
    '31~60库龄成本': 'Age_31_60_Cost',
    '61~90库龄数量': 'Age_61_90_Qty',
    '61~90库龄成本': 'Age_61_90_Cost',
    '91~180库龄数量': 'Age_91_180_Qty',
    '91~180库龄成本': 'Age_91_180_Cost',
    '181~270库龄数量': 'Age_181_270_Qty',
    '181~270库龄成本': 'Age_181_270_Cost',
    '271~330库龄数量': 'Age_271_330_Qty',
    '271~330库龄成本': 'Age_271_330_Cost',
    '331~365库龄数量': 'Age_331_365_Qty',
    '331~365库龄成本': 'Age_331_365_Cost',
    '365以上库龄数量': 'Age_365_Plus_Qty',
    '365以上库龄成本': 'Age_365_Plus_Cost'
}

# ========== 2. Age band definitions ==========
AGE_BANDS = [
    {'name': '0-60 days', 'qty_cols': ['Age_0_30_Qty', 'Age_31_60_Qty'], 
     'cost_cols': ['Age_0_30_Cost', 'Age_31_60_Cost']},
    {'name': '61-90 days', 'qty_cols': ['Age_61_90_Qty'], 
     'cost_cols': ['Age_61_90_Cost']},
    {'name': '91-180 days', 'qty_cols': ['Age_91_180_Qty'], 
     'cost_cols': ['Age_91_180_Cost']},
    {'name': '181-365 days', 'qty_cols': ['Age_181_270_Qty', 'Age_271_330_Qty', 'Age_331_365_Qty'], 
     'cost_cols': ['Age_181_270_Cost', 'Age_271_330_Cost', 'Age_331_365_Cost']},
    {'name': '365+ days', 'qty_cols': ['Age_365_Plus_Qty'],
     'cost_cols': ['Age_365_Plus_Cost']}
]

# ========== 3. MAXPOWER SKU Owner Mapping ==========
with open('maxpower_owner_mapping.json', 'r', encoding='utf-8') as f:
    MAXPOWER_SKU_MAPPING = json.load(f)  # lowercase sku -> owner

def assign_owner(df):
    """
    Assign owner based on brand and SKU:
    - brand = 'MAXPOWER' and SKU found in mapping -> VTM (from JSON)
    - brand = 'MAXPOWER' and SKU not found -> VTC
    - brand != 'MAXPOWER' (including empty/NaN) -> VTM
    """
    def get_owner(row):
        brand = str(row.get('Brand', '')).strip()
        sku = str(row.get('SKU', '')).strip().lower()

        if brand == 'MAXPOWER':
            if sku in MAXPOWER_SKU_MAPPING:
                return MAXPOWER_SKU_MAPPING[sku]
            else:
                return 'VTC'
        else:
            return 'VTM'

    df = df.copy()
    df['Owner'] = df.apply(get_owner, axis=1)
    return df

# ========== 4. Google Sheets connection function ==========
@st.cache_resource
def connect_to_gsheet(retries=3, delay=2):
    """
    Connect to Google Sheets with retry logic
    """
    credentials_dict = {
        "type": st.secrets["gcp"]["type"],
        "project_id": st.secrets["gcp"]["project_id"],
        "private_key_id": st.secrets["gcp"]["private_key_id"],
        "private_key": st.secrets["gcp"]["private_key"],
        "client_email": st.secrets["gcp"]["client_email"],
        "client_id": st.secrets["gcp"]["client_id"],
        "auth_uri": st.secrets["gcp"]["auth_uri"],
        "token_uri": st.secrets["gcp"]["token_uri"]
    }

    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive',
             'https://www.googleapis.com/auth/spreadsheets']
    credentials = Credentials.from_service_account_info(
        credentials_dict, scopes=scope)

    for attempt in range(retries):
        try:
            client = gspread.authorize(credentials)
            # Test connection
            client.openall()
            return client
        except Exception as e:
            if attempt < retries - 1:
                import time
                time.sleep(delay * (attempt + 1))  # Exponential backoff
                continue
            st.error(f"Failed to connect to Google Sheets after {retries} attempts: {str(e)}")
            return None
    return None

# ========== 5. Load static Warehouse Region mapping table ==========
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_warehouse_region_mapping():
    """
    Load static warehouse region mapping table from Google Sheets with JSON fallback
    Table structure: Warehouse, Country, Warehouse Location, Type, Description
    """
    def process_mapping_df(mapping_df):
        """Process and validate mapping dataframe"""
        if mapping_df is None or mapping_df.empty:
            return None

        # Standardize column names (remove extra spaces)
        mapping_df.columns = [str(col).strip() for col in mapping_df.columns]

        # Create column mapping based on your actual column names
        column_mapping = {}
        for col in mapping_df.columns:
            col_lower = col.lower()
            if 'warehouse' in col_lower and 'location' not in col_lower:
                column_mapping[col] = 'Warehouse'
            elif 'country' in col_lower:
                column_mapping[col] = 'Country'
            elif 'warehouse location' in col_lower or 'location' in col_lower:
                column_mapping[col] = 'Warehouse_Location'
            elif 'type' in col_lower:
                column_mapping[col] = 'Type'
            elif 'description' in col_lower:
                column_mapping[col] = 'Description'

        # Rename columns
        if column_mapping:
            mapping_df = mapping_df.rename(columns=column_mapping)

        # Ensure required columns exist
        required_cols = ['Warehouse', 'Country']
        missing_cols = [col for col in required_cols if col not in mapping_df.columns]

        if missing_cols:
            st.error(f"Missing required columns in mapping table: {missing_cols}")
            st.write("Available columns:", list(mapping_df.columns))
            return None

        return mapping_df

    def load_from_json():
        """Load mapping from local JSON file"""
        try:
            with open('warehouse_region_mapping.json', 'r', encoding='utf-8') as f:
                records = json.load(f)
            mapping_df = pd.DataFrame(records)
            st.info("Loaded warehouse mapping from local JSON file (Google Sheets unavailable)")
            return mapping_df
        except FileNotFoundError:
            st.error("Local warehouse mapping file not found. Please update warehouse_region_mapping.json")
            return None
        except Exception as e:
            st.error(f"Failed to load warehouse mapping from JSON: {str(e)}")
            return None

    try:
        client = connect_to_gsheet()
        if client is None:
            # Fallback to JSON
            st.warning("Google Sheets unavailable, trying local JSON fallback...")
            mapping_df = load_from_json()
            if mapping_df is not None:
                mapping_df = process_mapping_df(mapping_df)
                if mapping_df is not None:
                    with st.expander("View Warehouse Mapping Table"):
                        st.dataframe(mapping_df.head())
                        st.write(f"Total records: {len(mapping_df)}")
                        st.write(f"Country distribution: {mapping_df['Country'].value_counts().to_dict()}")
            return mapping_df

        # Get mapping sheet ID from secrets
        mapping_sheet_id = st.secrets["sheets"]["warehouse_region_sheet_id"]

        # Open mapping sheet with retry
        sheet = client.open_by_key(mapping_sheet_id)

        # Get first worksheet
        worksheet = sheet.sheet1

        # Get all records
        records = worksheet.get_all_records()

        if not records:
            st.warning("Warehouse mapping table is empty, trying JSON fallback...")
            mapping_df = load_from_json()
            mapping_df = process_mapping_df(mapping_df)
            if mapping_df is not None:
                with st.expander("View Warehouse Mapping Table"):
                    st.dataframe(mapping_df.head())
                    st.write(f"Total records: {len(mapping_df)}")
                    st.write(f"Country distribution: {mapping_df['Country'].value_counts().to_dict()}")
            return mapping_df

        # Convert to DataFrame
        mapping_df = pd.DataFrame(records)

        # Display original column names for debugging
        st.write("Original column names in mapping table:", list(mapping_df.columns))

        mapping_df = process_mapping_df(mapping_df)

        if mapping_df is not None:
            # Show preview
            with st.expander("View Warehouse Mapping Table"):
                st.dataframe(mapping_df.head())
                st.write(f"Total records: {len(mapping_df)}")
                st.write(f"Country distribution: {mapping_df['Country'].value_counts().to_dict()}")
                if 'Warehouse_Location' in mapping_df.columns:
                    st.write(f"Warehouse Location distribution: {mapping_df['Warehouse_Location'].value_counts().to_dict()}")

        return mapping_df

    except Exception as e:
        st.warning(f"Failed to load warehouse mapping from Google Sheets: {str(e)}, trying JSON fallback...")
        mapping_df = load_from_json()
        if mapping_df is not None:
            mapping_df = process_mapping_df(mapping_df)
            if mapping_df is not None:
                with st.expander("View Warehouse Mapping Table"):
                    st.dataframe(mapping_df.head())
                    st.write(f"Total records: {len(mapping_df)}")
                    st.write(f"Country distribution: {mapping_df['Country'].value_counts().to_dict()}")
        return mapping_df

# ========== 5. JOIN inventory data with warehouse mapping table ==========
def join_with_warehouse_region(inventory_df, mapping_df):
    """
    JOIN inventory data with warehouse region table
    Match based on Warehouse column
    """
    if mapping_df is None or len(mapping_df) == 0:
        st.error("Warehouse mapping table is empty, cannot perform JOIN")
        return inventory_df
    
    # Find warehouse column in inventory data
    warehouse_col_inventory = None
    
    # Check if Warehouse column already exists
    if 'Warehouse' in inventory_df.columns:
        warehouse_col_inventory = 'Warehouse'
    else:
        # Try to find column containing "仓库" or "warehouse"
        for col in inventory_df.columns:
            if '仓库' in col or 'warehouse' in col.lower():
                warehouse_col_inventory = col
                break
    
    if warehouse_col_inventory is None:
        st.error("Cannot find warehouse column in inventory data")
        return inventory_df
    
    # Ensure mapping table has Warehouse column
    if 'Warehouse' not in mapping_df.columns:
        st.error("Mapping table missing Warehouse column")
        return inventory_df
    
    st.info(f"""
    **JOIN Information:**
    - Left table (inventory): {warehouse_col_inventory}
    - Right table (mapping): Warehouse
    - JOIN type: LEFT JOIN
    """)
    
    # Prepare data
    inventory_join = inventory_df.copy()
    mapping_join = mapping_df.copy()
    
    # Convert warehouse column to string and strip spaces for matching
    inventory_join['_join_key'] = inventory_join[warehouse_col_inventory].astype(str).str.strip().str.upper()
    mapping_join['_join_key'] = mapping_join['Warehouse'].astype(str).str.strip().str.upper()
    
    # Select needed columns - include all available columns from mapping
    mapping_cols = ['_join_key', 'Country']
    if 'Warehouse_Location' in mapping_join.columns:
        mapping_cols.append('Warehouse_Location')
    if 'Type' in mapping_join.columns:
        mapping_cols.append('Type')
    if 'Description' in mapping_join.columns:
        mapping_cols.append('Description')
    
    # Perform LEFT JOIN
    merged_df = pd.merge(
        inventory_join,
        mapping_join[mapping_cols],
        on='_join_key',
        how='left'
    )
    
    # Remove temporary column
    merged_df = merged_df.drop('_join_key', axis=1)
    
    # Calculate match statistics using Country column
    total_rows = len(merged_df)
    matched_rows = merged_df['Country'].notna().sum()
    match_rate = (matched_rows / total_rows * 100) if total_rows > 0 else 0
    
    # Find unmatched warehouses
    unmatched_warehouses = merged_df[merged_df['Country'].isna()][warehouse_col_inventory].unique()
    
    st.success(f"""
    ✅ JOIN completed!
    - Total records: {total_rows}
    - Successfully matched: {matched_rows} ({match_rate:.1f}%)
    - Unmatched: {total_rows - matched_rows}
    """)
    
    if len(unmatched_warehouses) > 0:
        st.warning(f"""
        ⚠️ Following warehouses not found in mapping table:
        {', '.join([str(w) for w in unmatched_warehouses[:10]])}
        {', etc' if len(unmatched_warehouses) > 10 else ''}
        """)
    
    # Show country distribution after JOIN
    if 'Country' in merged_df.columns:
        country_counts = merged_df['Country'].value_counts()
        st.info(f"Country distribution: {dict(country_counts)}")
    
    return merged_df

# ========== 6. Data preprocessing function ==========
def preprocess_data(df):
    """
    Data preprocessing: rename columns
    """
    # Create copy to avoid modifying original data
    df_copy = df.copy()
    
    # Rename columns
    for chinese_name, english_name in COLUMN_MAPPING.items():
        if chinese_name in df_copy.columns:
            df_copy = df_copy.rename(columns={chinese_name: english_name})
    
    # Ensure numeric columns are numeric type
    numeric_cols = ['Total_Inventory', 'Available_Qty', 'Reserved_Qty', 'Defect_Qty',
                    'Pending_Inspection', 'Transfer_Transit', 'FBA_Transit', 
                    'FBA_Planned', 'Expected_Receipt', 'Projected_Inventory']
    
    # Add all age-related columns
    for band in AGE_BANDS:
        numeric_cols.extend(band['qty_cols'])
        numeric_cols.extend(band['cost_cols'])
    
    for col in numeric_cols:
        if col in df_copy.columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)
    
    return df_copy

# ========== 7. Calculate inventory value by age band ==========
def calculate_age_band_values(df):
    """
    Calculate inventory value for each SKU by age band
    """
    result = df.copy()
    
    # Calculate total value for each age band
    for band in AGE_BANDS:
        band_name = band['name']
        # Calculate total cost for this age band
        cost_cols = [col for col in band['cost_cols'] if col in df.columns]
        if cost_cols:
            result[f'{band_name}_Value'] = result[cost_cols].sum(axis=1)
        else:
            result[f'{band_name}_Value'] = 0
        
        # Calculate total quantity for this age band
        qty_cols = [col for col in band['qty_cols'] if col in df.columns]
        if qty_cols:
            result[f'{band_name}_Qty'] = result[qty_cols].sum(axis=1)
        else:
            result[f'{band_name}_Qty'] = 0
    
    # Calculate total inventory value and quantity from all age bands
    value_cols = [f"{band['name']}_Value" for band in AGE_BANDS if f"{band['name']}_Value" in result.columns]
    if value_cols:
        result['Total_Value'] = result[value_cols].sum(axis=1)
    else:
        result['Total_Value'] = 0

    # Calculate total quantity from all age bands (for consistent reporting)
    qty_cols = [f"{band['name']}_Qty" for band in AGE_BANDS if f"{band['name']}_Qty" in result.columns]
    if qty_cols:
        result['Total_Band_Qty'] = result[qty_cols].sum(axis=1)
    else:
        result['Total_Band_Qty'] = 0

    return result

# ========== 8. Filter data by age band ==========
def filter_by_age_band(df, age_band_name):
    """
    Filter dataframe to include only SKUs with value or qty in the selected age band
    If age_band_name is None or 'All Data', return original dataframe
    """
    if age_band_name is None or age_band_name == 'All Data':
        return df

    # Find the age band
    selected_band = None
    for band in AGE_BANDS:
        if band['name'] == age_band_name:
            selected_band = band
            break

    if selected_band is None:
        return df

    # Get the value and qty columns for this age band
    value_col = f"{selected_band['name']}_Value"
    qty_col = f"{selected_band['name']}_Qty"

    # Filter to keep SKUs with value OR qty in this age band (to match Age Summary)
    if value_col in df.columns and qty_col in df.columns:
        filtered_df = df[(df[value_col] > 0) | (df[qty_col] > 0)].copy()
        return filtered_df
    elif value_col in df.columns:
        filtered_df = df[df[value_col] > 0].copy()
        return filtered_df
    else:
        return df

# ========== 9. Modified ABC classification function ==========
def abc_classification(df, value_col, group_col=None):
    """
    ABC classification function - Modified version
    When cumulative percentage crosses 0.8 or 0.95 threshold, 
    the crossing item is included in the previous class
    """
    if df.empty or value_col not in df.columns:
        return df
    
    if group_col and group_col in df.columns:
        result_dfs = []
        for group, group_df in df.groupby(group_col, dropna=False):
            if len(group_df) > 0:
                # Sort by value descending
                sorted_df = group_df.sort_values(value_col, ascending=False).copy()
                total = sorted_df[value_col].sum()
                
                if total > 0:
                    # Calculate value percentage
                    sorted_df['value_pct'] = sorted_df[value_col] / total
                    
                    # Calculate cumulative percentage
                    cum_pct = 0
                    cum_pct_list = []
                    
                    for pct in sorted_df['value_pct']:
                        cum_pct += pct
                        cum_pct_list.append(cum_pct)
                    
                    sorted_df['cum_pct'] = cum_pct_list
                    
                    # Initialize classification column
                    sorted_df['abc_class'] = 'C'  # Default to C class
                    
                    # Identify A class: cumulative <= 0.8 OR crosses from <0.8 to >0.8
                    a_mask = pd.Series(False, index=sorted_df.index)
                    prev_cum = 0
                    
                    for idx, cum in zip(sorted_df.index, cum_pct_list):
                        if cum <= 0.8 or (prev_cum < 0.8 and cum > 0.8):
                            a_mask[idx] = True
                        prev_cum = cum
                    
                    sorted_df.loc[a_mask, 'abc_class'] = 'A'
                    
                    # Identify B class: after A class, cumulative <= 0.95 OR crosses from <0.95 to >0.95
                    b_mask = pd.Series(False, index=sorted_df.index)
                    prev_cum = 0
                    
                    for idx, cum in zip(sorted_df.index, cum_pct_list):
                        if cum <= 0.95 or (prev_cum < 0.95 and cum > 0.95):
                            # If not already A class
                            if not a_mask[idx]:
                                b_mask[idx] = True
                        prev_cum = cum
                    
                    # B class should not overlap with A class
                    b_mask = b_mask & ~a_mask
                    sorted_df.loc[b_mask, 'abc_class'] = 'B'
                    
                else:
                    sorted_df['value_pct'] = 0
                    sorted_df['cum_pct'] = 0
                    sorted_df['abc_class'] = 'C'
                
                result_dfs.append(sorted_df)
        
        return pd.concat(result_dfs, ignore_index=True) if result_dfs else df
    
    else:
        # Overall classification (no grouping)
        sorted_df = df.sort_values(value_col, ascending=False).copy()
        total = sorted_df[value_col].sum()
        
        if total > 0:
            # Calculate value percentage
            sorted_df['value_pct'] = sorted_df[value_col] / total
            
            # Calculate cumulative percentage
            cum_pct = 0
            cum_pct_list = []
            
            for pct in sorted_df['value_pct']:
                cum_pct += pct
                cum_pct_list.append(cum_pct)
            
            sorted_df['cum_pct'] = cum_pct_list
            
            # Initialize classification column
            sorted_df['abc_class'] = 'C'  # Default to C class
            
            # Identify A class: cumulative <= 0.8 OR crosses from <0.8 to >0.8
            a_mask = pd.Series(False, index=sorted_df.index)
            prev_cum = 0
            
            for idx, cum in zip(sorted_df.index, cum_pct_list):
                if cum <= 0.8 or (prev_cum < 0.8 and cum > 0.8):
                    a_mask[idx] = True
                prev_cum = cum
            
            sorted_df.loc[a_mask, 'abc_class'] = 'A'
            
            # Identify B class: after A class, cumulative <= 0.95 OR crosses from <0.95 to >0.95
            b_mask = pd.Series(False, index=sorted_df.index)
            prev_cum = 0
            
            for idx, cum in zip(sorted_df.index, cum_pct_list):
                if cum <= 0.95 or (prev_cum < 0.95 and cum > 0.95):
                    # If not already A class
                    if not a_mask[idx]:
                        b_mask[idx] = True
                prev_cum = cum
            
            # B class should not overlap with A class
            b_mask = b_mask & ~a_mask
            sorted_df.loc[b_mask, 'abc_class'] = 'B'
            
        else:
            sorted_df['value_pct'] = 0
            sorted_df['cum_pct'] = 0
            sorted_df['abc_class'] = 'C'
        
        return sorted_df

# ========== 10. Generate Report 1: Age Summary ==========
def generate_age_summary(df, country):
    """
    Generate age summary report
    Value % is returned as decimal (e.g., 0.15 for 15%)
    """
    if 'Country' not in df.columns:
        return pd.DataFrame()
    
    country_df = df[df['Country'] == country].copy()
    
    if len(country_df) == 0:
        return pd.DataFrame()
    
    age_summary = []
    for band in AGE_BANDS:
        band_name = band['name']
        value_col = f'{band_name}_Value'
        qty_col = f'{band_name}_Qty'
        
        if value_col in country_df.columns:
            total_value = country_df[value_col].sum()
            total_qty = country_df[qty_col].sum() if qty_col in country_df.columns else 0
            
            age_summary.append({
                'Age Band': band_name,
                'Inventory Qty': total_qty,
                'Inventory Value': total_value
            })
    
    if not age_summary:
        return pd.DataFrame()
    
    summary_df = pd.DataFrame(age_summary)
    total_value = summary_df['Inventory Value'].sum()
    # Return as decimal (not multiplied by 100) for consistency with other reports
    summary_df['Value %'] = (summary_df['Inventory Value'] / total_value).round(4)
    
    return summary_df

# ========== 11. Generate Report 2: Brand ABC Classification ==========
def generate_brand_abc(df, country, age_band=None):
    """
    Generate brand ABC classification report
    Value % and Cumulative % are returned as decimals
    Can filter by age band if specified
    """
    if 'Country' not in df.columns or 'Brand' not in df.columns:
        return pd.DataFrame()
    
    # Filter by country
    country_df = df[df['Country'] == country].copy()
    
    # Filter by age band if specified
    if age_band and age_band != 'All Data':
        filtered_df = filter_by_age_band(country_df, age_band)
    else:
        filtered_df = country_df
    
    if len(filtered_df) == 0:
        return pd.DataFrame()
    
    # 根据 age_band 选择正确的数量列和价值列
    # 使用 Total_Band_Qty (所有波段Qty总和) 以确保与 Age Summary 一致
    qty_col = f'{age_band}_Qty' if age_band and age_band != 'All Data' else 'Total_Band_Qty'
    # 使用对应波段的 Value 列来汇总
    value_col = f'{age_band}_Value' if age_band and age_band != 'All Data' else 'Total_Value'

    brand_summary = filtered_df.groupby('Brand', dropna=False).agg({
        value_col: 'sum',
        'SKU': 'count',
        qty_col: 'sum'
    }).rename(columns={
        'SKU': 'SKU Count',
        qty_col: 'Inventory Qty',
        value_col: 'Total_Value'  # 重命名为 Total_Value 供后续使用
    }).reset_index()

    # 过滤掉 Total_Value 和 Inventory Qty 都为 0 的品牌（既没数量也没金额）
    brand_summary = brand_summary[~((brand_summary['Total_Value'] == 0) & (brand_summary['Inventory Qty'] == 0))]
    
    if len(brand_summary) == 0:
        return pd.DataFrame()
    
    # Use modified ABC classification function
    brand_abc = abc_classification(brand_summary, 'Total_Value')
    
    brand_abc = brand_abc.rename(columns={
        'Brand': 'Brand',
        'Total_Value': 'Inventory Value',
        'value_pct': 'Value %',
        'cum_pct': 'Cumulative %',
        'abc_class': 'Brand Class'
    })
    
    # Define column order to match Streamlit display
    column_order = ['Brand', 'SKU Count', 'Inventory Qty', 'Inventory Value', 'Value %', 'Cumulative %', 'Brand Class']
    brand_abc = brand_abc[[col for col in column_order if col in brand_abc.columns]]
    
    return brand_abc

# ========== 12. Generate Report 3: SKU ABC Classification ==========
def generate_sku_abc(df, country, age_band=None):
    """
    Generate SKU ABC classification report
    Sort by Brand Class from A to Z, then by Inventory Value from high to low within each Brand Class
    Value % and Cumulative % are returned as decimals
    Can filter by age band if specified
    """
    if 'Country' not in df.columns:
        return pd.DataFrame()
    
    # Filter by country
    country_df = df[df['Country'] == country].copy()
    
    # Filter by age band if specified
    if age_band and age_band != 'All Data':
        filtered_df = filter_by_age_band(country_df, age_band)
    else:
        filtered_df = country_df
    
    if len(filtered_df) == 0:
        return pd.DataFrame()
    
    # 根据 age_band 选择正确的数量列
    # 使用 Total_Band_Qty (所有波段Qty总和) 以确保与 Age Summary 一致
    qty_col = f'{age_band}_Qty' if age_band and age_band != 'All Data' else 'Total_Band_Qty'
    # 使用对应波段的 Value 列来汇总
    value_col = f'{age_band}_Value' if age_band and age_band != 'All Data' else 'Total_Value'
    sku_cols = ['Brand', 'SKU', 'Product_Name', value_col, qty_col]
    available_cols = [col for col in sku_cols if col in filtered_df.columns]

    if not available_cols:
        return pd.DataFrame()

    sku_data = filtered_df[available_cols].copy()
    # 重命名 value_col 为 Total_Value 供后续使用
    sku_data = sku_data.rename(columns={value_col: 'Total_Value'})
    # 过滤掉 Total_Value 和对应的数量列都为 0 的 SKU（既没数量也没金额）
    sku_data = sku_data[~((sku_data['Total_Value'] == 0) & (sku_data[qty_col] == 0))]

    if len(sku_data) == 0:
        return pd.DataFrame()

    # 按 Brand + SKU 合并数据（汇总所有仓库的同一 SKU）
    # 检查 Brand 列是否存在
    if 'Brand' not in sku_data.columns or 'SKU' not in sku_data.columns:
        return pd.DataFrame()

    groupby_cols = ['Brand', 'SKU']
    if 'Product_Name' in sku_data.columns:
        groupby_cols.append('Product_Name')

    sku_data = sku_data.groupby(groupby_cols, dropna=False).agg({
        'Total_Value': 'sum',
        qty_col: 'sum'
    }).reset_index()
    
    # Get brand classification (using the same age band for consistency)
    brand_abc = generate_brand_abc(df, country, age_band)
    if len(brand_abc) > 0 and 'Brand' in brand_abc.columns:
        brand_class_map = dict(zip(brand_abc['Brand'], brand_abc['Brand Class']))
        sku_data['Brand Class'] = sku_data['Brand'].map(brand_class_map)
    else:
        sku_data['Brand Class'] = 'Unclassified'
    
    # Use modified ABC classification function for SKU-level classification
    sku_abc = abc_classification(sku_data, 'Total_Value', group_col='Brand')
    
    # 重命名列（groupby 后 qty_col 变成了聚合后的列名）
    rename_dict = {
        'Brand': 'Brand',
        'SKU': 'SKU',
        'Total_Value': 'Inventory Value',
        'value_pct': 'Value %',
        'cum_pct': 'Cumulative %',
        'abc_class': 'SKU Class'
    }
    # 检查 qty_col 是否在列中，如果不在尝试其他可能的列名
    if qty_col in sku_abc.columns:
        rename_dict[qty_col] = 'Inventory Qty'
    elif 'Total_Band_Qty' in sku_abc.columns:
        rename_dict['Total_Band_Qty'] = 'Inventory Qty'
    if 'Product_Name' in sku_abc.columns:
        rename_dict['Product_Name'] = 'Product Name'

    sku_abc = sku_abc.rename(columns=rename_dict)
    
    # Define custom sort order for Brand Class
    brand_class_order = {'A': 0, 'B': 1, 'C': 2, 'Unclassified': 3}
    
    # Create sort key for Brand Class
    sku_abc['brand_sort'] = sku_abc['Brand Class'].map(brand_class_order)
    
    # Sort by Brand Class first (ascending), then by Inventory Value (descending)
    sku_abc = sku_abc.sort_values(['brand_sort', 'Inventory Value'], ascending=[True, False])
    
    # Remove temporary sort column
    sku_abc = sku_abc.drop('brand_sort', axis=1)
    
    # Define column order to match Streamlit display
    display_cols = ['Brand Class', 'Brand', 'SKU', 'Product Name', 'Inventory Qty', 'Inventory Value', 'Value %', 'Cumulative %', 'SKU Class']
    sku_abc = sku_abc[[col for col in display_cols if col in sku_abc.columns]]
    
    return sku_abc

# ========== 13. Function to create Excel download with percentage formatting ==========
def create_excel_download(all_reports):
    """
    Create an Excel file with multiple sheets from all reports
    Ensure Value % and Cumulative % columns are formatted as percentages
    All percentage values should be in decimal format (0.xx) for Excel formatting
    """
    output = BytesIO()
    
    # First, write all data to Excel
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in all_reports.items():
            if not df.empty:
                # Clean sheet name (Excel sheet names have length limit of 31 characters)
                clean_name = sheet_name.replace('_', ' ')[:31]
                
                # Create a copy of the dataframe for Excel
                df_excel = df.copy()
                
                # Ensure percentage columns are float (they should already be in decimal format)
                for col in df_excel.columns:
                    if col in ['Value %', 'Cumulative %']:
                        df_excel[col] = pd.to_numeric(df_excel[col], errors='coerce')
                
                # Write to Excel
                df_excel.to_excel(writer, sheet_name=clean_name, index=False)
                
                # Get the workbook and worksheet to apply formatting
                workbook = writer.book
                worksheet = writer.sheets[clean_name]
                
                # Apply percentage formatting to Value % and Cumulative % columns
                for col_idx, col_name in enumerate(df_excel.columns, 1):  # Excel columns are 1-indexed
                    if col_name in ['Value %', 'Cumulative %']:
                        for row_idx in range(2, len(df_excel) + 2):  # Start from row 2 (skip header)
                            cell = worksheet.cell(row=row_idx, column=col_idx)
                            cell.number_format = '0.00%'
                
                # Auto-adjust column widths
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)  # Cap width at 50
                    worksheet.column_dimensions[column_letter].width = adjusted_width

    output.seek(0)
    return output

# ========== 15. SKU Comparison Functions ==========
def compare_inventory(new_df: pd.DataFrame, baseline_df: pd.DataFrame, debug: bool = False) -> pd.DataFrame:
    """
    Compare new inventory with baseline snapshot.
    Aggregates by SKU first, then compares totals.

    Args:
        new_df: New inventory DataFrame (processed)
        baseline_df: Baseline snapshot DataFrame
        debug: If True, include debug column

    Returns:
        DataFrame with comparison columns added (one row per SKU)
    """
    debug_info = []

    # Normalize SKU for matching
    new_df = new_df.copy()
    baseline_df = baseline_df.copy()
    new_df['_sku_key'] = new_df['SKU'].astype(str).str.strip().str.lower()
    baseline_df['_sku_key'] = baseline_df['SKU'].astype(str).str.strip().str.lower()

    # Aggregate both by SKU (sum all Available_Qty regardless of warehouse/country)
    baseline_agg = baseline_df.groupby('_sku_key', dropna=False).agg({
        'Available_Qty': 'sum'
    }).reset_index()

    new_agg = new_df.groupby('_sku_key', dropna=False).agg({
        'Available_Qty': 'sum'
    }).reset_index()

    debug_info.append(f"Baseline aggregated SKUs: {len(baseline_agg)}")
    debug_info.append(f"New aggregated SKUs: {len(new_agg)}")

    # Get Brand and Country from first occurrence
    brand_df = new_df.groupby('_sku_key', dropna=False).first()['Brand'].reset_index()
    brand_df['_sku_key'] = brand_df['_sku_key'].astype(str).str.strip().str.lower()
    brand_lookup = dict(zip(brand_df['_sku_key'], brand_df['Brand']))

    country_df = new_df.groupby('_sku_key', dropna=False).first()['Country'].reset_index()
    country_df['_sku_key'] = country_df['_sku_key'].astype(str).str.strip().str.lower()
    country_lookup = dict(zip(country_df['_sku_key'], country_df['Country']))

    # Create lookup for baseline
    baseline_lookup = dict(zip(baseline_agg['_sku_key'], baseline_agg['Available_Qty']))

    # Find problematic rows (Old > New but Recent_Sold = 0)
    problem_rows = []

    # Calculate comparison
    results = []
    for _, row in new_agg.iterrows():
        sku_key = row['_sku_key']
        new_available = float(row['Available_Qty']) if pd.notna(row['Available_Qty']) else 0.0
        brand = brand_lookup.get(sku_key, '')
        country = country_lookup.get(sku_key, 'Unknown')

        if sku_key in baseline_lookup:
            old_available = float(baseline_lookup[sku_key]) if pd.notna(baseline_lookup[sku_key]) else 0.0

            if new_available < old_available:
                recent_sold = old_available - new_available
                status = "Sold"
            elif new_available > old_available:
                recent_sold = 0
                status = "Flagged"
            else:
                recent_sold = 0
                status = "Zero Sales"

            # Check for problems
            if new_available < old_available and recent_sold == 0:
                problem_rows.append({
                    'SKU': sku_key,
                    'Available_New': new_available,
                    'Available_Old': old_available,
                    'Recent_Sales': recent_sold,
                    'Status': status
                })
        else:
            old_available = None
            recent_sold = 0
            status = "New"

        result_row = {
            'SKU': sku_key,
            'Country': country if pd.notna(country) else 'Unknown',
            'Brand': brand,
            'Available_New': int(new_available),
            'Available_Old': int(old_available) if old_available is not None else None,
            'Recent_Sales': int(recent_sold),
            'Comparison_Status': status
        }
        results.append(result_row)

    result_df = pd.DataFrame(results)

    # Debug info
    debug_info.append(f"Results: {len(result_df)} rows")
    status_counts = result_df['Comparison_Status'].value_counts()
    debug_info.append(f"Status summary: {dict(status_counts)}")

    result_df['_debug_info'] = str(debug_info)

    return result_df


def add_sold_skus_from_baseline(new_df: pd.DataFrame, baseline_df: pd.DataFrame,
                                  country: str, owner_filter: list = None) -> pd.DataFrame:
    """
    Add SKUs that exist in baseline but not in new data (sold/lost items).

    Args:
        new_df: New inventory DataFrame (processed)
        baseline_df: Baseline snapshot DataFrame
        country: Country to filter
        owner_filter: List of owners to include

    Returns:
        DataFrame with sold/lost SKUs added
    """
    # Get SKUs in new data (lowercase)
    new_skus = set()
    for _, row in new_df.iterrows():
        sku = str(row.get('SKU', '')).strip().lower()
        new_skus.add(sku)

    # Find SKUs in baseline not in new data
    sold_rows = []
    for _, row in baseline_df.iterrows():
        sku = str(row.get('SKU', '')).strip().lower()
        if sku not in new_skus:
            # This SKU was in baseline but not in new data - sold/lost
            sold_row = row.to_dict()
            old_qty = float(row.get('Available_Qty', 0)) if pd.notna(row.get('Available_Qty')) else 0.0
            sold_row['Available_New'] = 0
            sold_row['Available_Old'] = old_qty
            sold_row['Recent_Sales'] = int(old_qty)  # All old quantity was sold
            sold_row['Comparison_Status'] = 'Sold'
            sold_rows.append(sold_row)

    if not sold_rows:
        return pd.DataFrame()

    sold_df = pd.DataFrame(sold_rows)

    # Filter by country if present
    if 'Country' in sold_df.columns:
        sold_df = sold_df[sold_df['Country'] == country]

    # Filter by owner if specified
    if owner_filter and 'Owner' in sold_df.columns:
        sold_df = sold_df[sold_df['Owner'].isin(owner_filter)]

    return sold_df


def generate_sku_comparison(new_df: pd.DataFrame, baseline_df: pd.DataFrame,
                            country: str, owner_filter: list = None) -> tuple:
    """
    Generate SKU comparison report.

    Args:
        new_df: New inventory DataFrame (processed)
        baseline_df: Baseline snapshot DataFrame
        country: Country to filter
        owner_filter: List of owners to include

    Returns:
        Tuple of (DataFrame with SKU comparison data, debug_info list)
    """
    # Filter by country and owner before comparing
    filter_df = new_df.copy()
    if 'Country' in filter_df.columns:
        filter_df = filter_df[filter_df['Country'] == country]
    if owner_filter and 'Owner' in filter_df.columns:
        filter_df = filter_df[filter_df['Owner'].isin(owner_filter)]

    # Compare with baseline (debug mode)
    compared_df = compare_inventory(filter_df, baseline_df, debug=True)

    # Extract debug info
    debug_info = []
    if '_debug_info' in compared_df.columns:
        debug_info = eval(compared_df['_debug_info'].iloc[0]) if len(compared_df) > 0 else []
        compared_df = compared_df.drop(columns=['_debug_info'])

    # Add sold/lost SKUs from baseline
    sold_df = add_sold_skus_from_baseline(filter_df, baseline_df, country, owner_filter)

    # Select columns for display
    display_cols = ['SKU', 'Country', 'Brand', 'Available_New', 'Available_Old',
                    'Recent_Sales', 'Comparison_Status']
    available_cols = [col for col in display_cols if col in compared_df.columns]
    result_df = compared_df[available_cols].copy()

    # Add sold SKUs if any
    if not sold_df.empty:
        sold_display_cols = ['SKU', 'Country', 'Brand', 'Available_New', 'Available_Old',
                             'Recent_Sales', 'Comparison_Status']
        sold_available = [col for col in sold_display_cols if col in sold_df.columns]
        sold_subset = sold_df[sold_available].copy()
        result_df = pd.concat([result_df, sold_subset], ignore_index=True)

    return result_df, debug_info

# ========== 14. Function to demonstrate ABC classification logic ==========
def demonstrate_abc_logic():
    """
    Demonstrate the modified ABC classification logic
    """
    st.subheader("📊 ABC Classification Logic Demonstration")
    
    # Create example data
    example_data = pd.DataFrame({
        'Item': ['Item1', 'Item2', 'Item3', 'Item4', 'Item5', 'Item6'],
        'Value': [400, 300, 200, 50, 30, 20]
    })
    
    st.write("Example data:")
    st.dataframe(example_data)
    
    # Apply ABC classification
    result = abc_classification(example_data, 'Value')
    
    st.write("Classification result (note that Item3 crossing 80% threshold is classified as A):")
    st.dataframe(result.style.format({
        'value_pct': '{:.2%}',
        'cum_pct': '{:.2%}'
    }))
    
    st.info("""
    **Logic explanation:**
    - Item1 (40%): cumulative 40% → A class
    - Item2 (30%): cumulative 70% → A class
    - Item3 (20%): cumulative 90% → **A class** (crosses from 70% to 90%, exceeding 80% threshold)
    - Item4 (5%): cumulative 95% → B class
    - Item5 (3%): cumulative 98% → C class
    - Item6 (2%): cumulative 100% → C class
    """)

# ========== 15. Main program ==========
def main():
    st.sidebar.header("⚙️ Analyzer Information")
    
    # Display system architecture
    with st.sidebar:
        st.markdown("""
        ### 📋 Data Flow
        1. **Load static mapping table** (Google Sheets)
           - Warehouse
           - Country (used for country classification)
           - Warehouse Location
           - Type
           - Description
        
        2. **Upload inventory data**
           - Make sure it contains "Warehouse" column
        
        3. **JOIN operation**
           - Inventory.Warehouse = Mapping.Warehouse
           - Add Country, Warehouse Location, Type, Description
        
        4. **Filter by Age Band** (optional)
           - Select specific age band for analysis
        
        5. **Analysis by country**
           - Using ABC classification logic
           - Items crossing thresholds included in previous class
        
        6. **Download results**
           - One-click download all reports as Excel
           - Value % and Cumulative % displayed as percentages
        """)
        
        st.markdown("---")
        
        # Add ABC logic demonstration button
        if st.button("📊 View ABC Classification Demo"):
            demonstrate_abc_logic()
        
        # Test connection button
        if st.button("🔄 Test Google Sheets Connection"):
            client = connect_to_gsheet()
            if client:
                st.success("✅ Connection successful")
            else:
                st.error("❌ Connection failed")
    
    # Main content area
    st.subheader("📤 Upload Inventory Data File")
    inventory_file = st.file_uploader(
        "Please upload inventory report in Excel format",
        type=['xlsx', 'xls'],
        help="Upload inventory report containing SKU, Brand, Warehouse, Age information"
    )
    
    if inventory_file:
        try:
            # Read inventory data
            df = pd.read_excel(inventory_file)
            
            with st.expander("View Raw Data Preview"):
                st.dataframe(df.head())
                st.write(f"Total rows: {len(df)}")
                st.write(f"Column names: {list(df.columns)}")

            # ===== Step 1.5: Select Baseline Snapshot for Comparison =====
            st.subheader("📸 Step 1.5: Select Baseline Snapshot (Optional)")

            # Check if Gist is configured
            gist_configured = hasattr(st.secrets, "gist") and "gist_token" in st.secrets["gist"] and "gist_id" in st.secrets["gist"]

            if gist_configured:
                gist_token = st.secrets["gist"]["gist_token"]
                gist_id = st.secrets["gist"]["gist_id"]

                with st.spinner("Loading snapshots from Gist..."):
                    snapshots, debug_info = load_snapshots(gist_token, gist_id)

                # Show debug info in expander
                with st.expander("🔧 Gist Debug Info"):
                    for line in debug_info:
                        st.write(line)

                if snapshots:
                    # Create options for dropdown
                    snapshot_options = ["None (Skip Comparison)"]
                    snapshot_dates = [None]
                    for snap in snapshots:
                        date = snap.get("date", "Unknown")
                        saved_at = snap.get("saved_at", "")
                        if saved_at:
                            try:
                                saved_dt = datetime.fromisoformat(saved_at.replace("Z", "+00:00"))
                                saved_str = saved_dt.strftime("%Y-%m-%d %H:%M")
                            except:
                                saved_str = saved_at[:16] if len(saved_at) >= 16 else saved_at
                            snapshot_options.append(f"{date} (saved: {saved_str})")
                        else:
                            snapshot_options.append(date)
                        snapshot_dates.append(snap)

                    selected_snapshot_idx = st.selectbox(
                        "Compare with baseline snapshot:",
                        options=range(len(snapshot_options)),
                        format_func=lambda x: snapshot_options[x],
                        index=0,
                        help="Select a historical snapshot to compare against. This will show Recent Sales and status flags."
                    )

                    if selected_snapshot_idx > 0:
                        st.session_state['selected_baseline'] = snapshot_dates[selected_snapshot_idx]
                        st.success(f"Selected baseline: {snapshot_options[selected_snapshot_idx]}")
                    else:
                        st.session_state['selected_baseline'] = None
                        st.info("Comparison skipped - current upload will be saved as a new snapshot.")
                else:
                    st.session_state['selected_baseline'] = None
                    st.warning("No historical snapshots found. This upload will be saved as the baseline for future comparisons.")
            else:
                st.session_state['selected_baseline'] = None
                st.markdown("""
                **Gist not configured** - To enable snapshot comparison, add to your Streamlit secrets:
                ```toml
                [gist]
                gist_token = "your-github-personal-access-token"
                gist_id = "your-gist-id"
                ```
                """)

            # ===== Step 1: Load static warehouse mapping table =====
            st.subheader("🗺️ Step 1: Load Warehouse Region Mapping Table")
            mapping_df = load_warehouse_region_mapping()
            
            if mapping_df is None:
                st.error("Unable to load warehouse mapping table, please check Google Sheets configuration")
                st.stop()
            
            # ===== Step 2: JOIN operation =====
            st.subheader("🔗 Step 2: JOIN Inventory Data with Warehouse Mapping")
            df_with_region = join_with_warehouse_region(df, mapping_df)
            
            # ===== Step 3: Data preprocessing =====
            st.subheader("🔄 Step 3: Data Preprocessing")
            df_processed = preprocess_data(df_with_region)
            
            # ===== Step 4: Calculate age band values =====
            st.subheader("💰 Step 4: Calculate Inventory Value")
            df_with_values = calculate_age_band_values(df_processed)

            # ===== Step 4.5: Assign Owner =====
            st.subheader("👤 Step 5: Assign Owner")
            df_with_values = assign_owner(df_with_values)
            owner_counts = df_with_values['Owner'].value_counts()
            st.info(f"Owner distribution: {dict(owner_counts)}")

            # ===== Step 6: Age Band Selection =====
            st.subheader("📅 Step 5: Select Age Band for Analysis")
            
            # Create age band options
            age_band_options = ['All Data'] + [band['name'] for band in AGE_BANDS]
            selected_age_band = st.selectbox(
                "Select Age Band (optional):",
                options=age_band_options,
                index=0,
                help="Select a specific age band to analyze only that inventory. 'All Data' includes all age bands."
            )
            
            if selected_age_band != 'All Data':
                # Show filter information
                filtered_count = len(filter_by_age_band(df_with_values, selected_age_band))
                st.info(f"Filtering to show only SKUs with {selected_age_band} inventory. Found {filtered_count} SKUs with this age band.")

            # Owner multi-select filter
            owner_options = sorted(df_with_values['Owner'].unique().tolist())
            selected_owners = st.multiselect(
                "Filter by Owner:",
                options=owner_options,
                default=owner_options,
                help="Select one or more owners to filter the data. All owners selected by default."
            )

            # ===== Step 6: Analysis by country =====
            st.subheader("📊 Step 6: Generate Analysis Reports")
            
            # Get unique countries
            if 'Country' not in df_with_values.columns:
                st.error("Unable to get country information, JOIN may have failed")
                st.stop()
            
            countries = df_with_values['Country'].unique()
            countries = [c for c in countries if pd.notna(c)]  # Filter NaN only, keep empty string

            # Sort countries: US, US Local, CA, CA Local, CN, then others (empty string = VTM 北美仓)
            def sort_key(c):
                if c == 'US':
                    return (0, '')
                elif c == 'US Local':
                    return (1, '')
                elif c == 'CA':
                    return (2, '')
                elif c == 'CA Local':
                    return (3, '')
                elif c == 'CN':
                    return (4, '')
                else:
                    return (5, str(c))
            countries = sorted(countries, key=sort_key)
            
            if len(countries) == 0:
                st.error("No valid country data")
                st.stop()
            
            st.success(f"Found {len(countries)} countries: {', '.join(countries)}")
            
            # Dictionary to store all reports for download
            all_reports = {}

            # Apply owner filter to the main dataframe for report generation
            if selected_owners:
                df_for_reports = df_with_values[df_with_values['Owner'].isin(selected_owners)]
            else:
                df_for_reports = df_with_values

            tabs = st.tabs([f"{c}" if c in ['US', 'CA', 'CN', 'US Local', 'CA Local'] else f"VTM 北美仓" for c in countries])

            for tab, country in zip(tabs, countries):
                with tab:
                    # Get country data and apply owner filter
                    country_data = df_for_reports[df_for_reports['Country'] == country]

                    # Apply age band filter for display info
                    filter_desc = ""
                    if selected_owners and set(selected_owners) != set(owner_options):
                        filter_desc += f" owner={','.join(selected_owners)}"
                    if selected_age_band != 'All Data':
                        filtered_country_data = filter_by_age_band(country_data, selected_age_band)
                        filter_desc += f" {selected_age_band}"
                        st.markdown(f"### {country} Inventory Analysis ({len(filtered_country_data)} records{filter_desc})")
                    else:
                        st.markdown(f"### {country} Inventory Analysis ({len(country_data)} records{filter_desc})")
                    
                    st.markdown(f"**Inventory value is calculated in RMB**")
                    
                    # Display warehouse type and location distribution if available
                    col1, col2 = st.columns(2)
                    with col1:
                        if 'Type' in country_data.columns:
                            type_counts = country_data['Type'].value_counts()
                            st.info(f"Warehouse type distribution: {dict(type_counts)}")
                    with col2:
                        if 'Warehouse_Location' in country_data.columns:
                            location_counts = country_data['Warehouse_Location'].value_counts()
                            st.info(f"Warehouse location distribution: {dict(location_counts)}")
                    
                    # Report 1: Age Summary (always shows all age bands, regardless of filter)
                    st.markdown("#### Report 1: Age Summary")
                    age_summary = generate_age_summary(df_for_reports, country)
                    
                    if not age_summary.empty:
                        st.dataframe(
                            age_summary.style.format({
                                'Inventory Qty': '{:,.0f}',
                                'Inventory Value': '￥{:,.2f}',
                                'Value %': '{:.2%}'
                            }),
                            use_container_width=False
                        )
                        # Store in dictionary for download
                        report_key = f"{country}_Age_Summary"
                        all_reports[report_key] = age_summary
                    
                    # Report 2: Brand ABC (with age band filter)
                    st.markdown("#### Report 2: Brand ABC Classification")
                    brand_abc = generate_brand_abc(df_for_reports, country, selected_age_band)
                    
                    if not brand_abc.empty:
                        # Define column order for display
                        column_order = ['Brand', 'SKU Count', 'Inventory Qty', 'Inventory Value', 'Value %', 'Cumulative %', 'Brand Class']
                        display_columns = [col for col in column_order if col in brand_abc.columns]
                        
                        st.dataframe(
                            brand_abc[display_columns].style.format({
                                'Inventory Qty': '{:,.0f}',
                                'Inventory Value': '￥{:,.2f}',
                                'SKU Count': '{:,.0f}',
                                'Value %': '{:.2%}',
                                'Cumulative %': '{:.2%}'
                            }),
                            use_container_width=True
                        )
                        
                        # Add age band info to report name for download
                        if selected_age_band != 'All Data':
                            report_key = f"{country}_Brand_ABC_{selected_age_band.replace(' ', '_')}"
                        else:
                            report_key = f"{country}_Brand_ABC"
                        all_reports[report_key] = brand_abc
                    
                    # Report 3: SKU ABC (with age band filter)
                    st.markdown("#### Report 3: SKU ABC Classification")
                    sku_abc = generate_sku_abc(df_for_reports, country, selected_age_band)
                    
                    if not sku_abc.empty:
                        display_cols = ['Brand Class', 'Brand', 'SKU', 'Product Name', 'Inventory Qty', 'Inventory Value', 'Value %', 'Cumulative %', 'SKU Class']
                        available_cols = [col for col in display_cols if col in sku_abc.columns]
                        
                        st.dataframe(
                            sku_abc[available_cols].head(100).style.format({
                                'Inventory Qty': '{:,.0f}',
                                'Inventory Value': '￥{:,.2f}',
                                'Value %': '{:.2%}',
                                'Cumulative %': '{:.2%}'
                            }),
                            use_container_width=True
                        )
                        st.caption(f"Showing first 100 rows, total {len(sku_abc)} rows")
                        
                        # Add age band info to report name for download
                        if selected_age_band != 'All Data':
                            report_key = f"{country}_SKU_ABC_{selected_age_band.replace(' ', '_')}"
                        else:
                            report_key = f"{country}_SKU_ABC"
                        all_reports[report_key] = sku_abc

                    # Report 4: SKU Comparison (if baseline snapshot is selected)
                    if st.session_state.get('selected_baseline') is not None:
                        st.markdown("#### Report 4: SKU Comparison")
                        baseline_snapshot = st.session_state['selected_baseline']
                        baseline_df = get_snapshot_dataframe(baseline_snapshot)

                        if not baseline_df.empty:
                            sku_comparison, debug_info = generate_sku_comparison(
                                df_for_reports, baseline_df, country,
                                owner_filter=selected_owners if selected_owners else None
                            )

                            if not sku_comparison.empty:
                                # Display stats
                                sold_count = len(sku_comparison[sku_comparison['Comparison_Status'] == 'Sold'])
                                flagged_count = len(sku_comparison[sku_comparison['Comparison_Status'] == 'Flagged'])
                                zero_sales_count = len(sku_comparison[sku_comparison['Comparison_Status'] == 'Zero Sales'])
                                new_count = len(sku_comparison[sku_comparison['Comparison_Status'] == 'New'])

                                stat_cols = st.columns(4)
                                with stat_cols[0]:
                                    st.metric("Sold", sold_count)
                                with stat_cols[1]:
                                    st.metric("Flagged", flagged_count, delta="⚠️" if flagged_count > 0 else None)
                                with stat_cols[2]:
                                    st.metric("Zero Sales", zero_sales_count)
                                with stat_cols[3]:
                                    st.metric("New", new_count, delta="🆕" if new_count > 0 else None)

                                # Status filter
                                filter_options = ["All", "Sold", "Flagged", "Zero Sales", "New"]
                                selected_filter = st.selectbox(
                                    "Filter by status:",
                                    options=filter_options,
                                    index=0,
                                    key=f"comparison_filter_{country}"
                                )

                                # Apply filter
                                if selected_filter == "All":
                                    filtered_df = sku_comparison
                                else:
                                    filtered_df = sku_comparison[sku_comparison['Comparison_Status'] == selected_filter]

                                # Show debug info
                                with st.expander("🔧 Debug Info"):
                                    for line in debug_info:
                                        st.write(line)
                                    st.write(f"Total rows: {len(sku_comparison)}")

                                if not filtered_df.empty:
                                    st.dataframe(
                                        filtered_df.style.format({
                                            'Available_New': '{:,.0f}',
                                            'Available_Old': '{:,.0f}',
                                            'Recent_Sales': '{:,.0f}'
                                        }, na_rep='-'),
                                        use_container_width=True
                                    )
                                    st.caption(f"Showing {len(filtered_df)} of {len(sku_comparison)} SKUs")
                                else:
                                    st.info(f"No SKUs with status '{selected_filter}'")

                                # Store full comparison for download
                                report_key = f"{country}_SKU_Comparison"
                                all_reports[report_key] = sku_comparison
                            else:
                                st.info("No comparison data available for this country.")
                        else:
                            st.warning("Baseline snapshot data is empty.")

            # ===== Step 7: Generate all reports for download =====
            # Generate complete set of reports for all age bands
            all_download_reports = {}

            for country in countries:
                # Age Summary (always included)
                age_summary = generate_age_summary(df_for_reports, country)
                if not age_summary.empty:
                    all_download_reports[f"Age Summary - {country}"] = age_summary

                # Generate Brand ABC and SKU ABC for All Data
                brand_abc_all = generate_brand_abc(df_for_reports, country, 'All Data')
                if not brand_abc_all.empty:
                    all_download_reports[f"Brand ABC (All Data) - {country}"] = brand_abc_all

                sku_abc_all = generate_sku_abc(df_for_reports, country, 'All Data')
                if not sku_abc_all.empty:
                    all_download_reports[f"SKU ABC (All Data) - {country}"] = sku_abc_all

                # Generate Brand ABC and SKU ABC for each age band
                for band in AGE_BANDS:
                    band_name = band['name']
                    brand_abc_band = generate_brand_abc(df_for_reports, country, band_name)
                    if not brand_abc_band.empty:
                        all_download_reports[f"Brand ABC ({band_name}) - {country}"] = brand_abc_band

                    sku_abc_band = generate_sku_abc(df_for_reports, country, band_name)
                    if not sku_abc_band.empty:
                        all_download_reports[f"SKU ABC ({band_name}) - {country}"] = sku_abc_band

                # Generate SKU Comparison if baseline snapshot is selected
                if st.session_state.get('selected_baseline') is not None:
                    baseline_snapshot = st.session_state['selected_baseline']
                    baseline_df = get_snapshot_dataframe(baseline_snapshot)
                    if not baseline_df.empty:
                        sku_comparison, _ = generate_sku_comparison(
                            df_for_reports, baseline_df, country,
                            owner_filter=selected_owners if selected_owners else None
                        )
                        if not sku_comparison.empty:
                            all_download_reports[f"SKU Comparison - {country}"] = sku_comparison

            # ===== Step 8: Download section with options =====
            if all_download_reports:
                st.markdown("---")
                st.subheader("📥 Step 8: Download Reports")

                # Country and Age Band multi-selectors
                col_filter1, col_filter2 = st.columns(2)

                # Countries to exclude by default
                exclude_countries = ["US Local", "CA Local"]

                with col_filter1:
                    # Country multi-selector (no "All" option - select all by default)
                    # Default: select all countries except US Local and CA Local
                    selected_countries = st.multiselect(
                        "Select Country:",
                        options=countries,
                        default=[c for c in countries if c not in exclude_countries],
                        help="Select one or more countries (default excludes US Local and CA Local)"
                    )

                with col_filter2:
                    # Age Band multi-selector (no "All Data" option - select all by default)
                    age_band_names = [band['name'] for band in AGE_BANDS]

                    selected_age_bands = st.multiselect(
                        "Select Age Band:",
                        options=age_band_names,
                        default=age_band_names,
                        help="Select one or more age bands"
                    )

                # Build filtered options based on selection
                filtered_options = []

                # Helper function to check if country should be included
                def should_include_country(country):
                    if not selected_countries:
                        # If nothing selected, include all except excluded
                        return country not in exclude_countries
                    return country in selected_countries

                # Helper function to check if age band should be included
                def should_include_age_band(age_band):
                    if not selected_age_bands:
                        return True
                    return age_band in selected_age_bands

                # Add Age Summary options
                for country in countries:
                    if should_include_country(country):
                        if not selected_age_bands or len(selected_age_bands) == len(age_band_names):
                            filtered_options.append(f"Age Summary - {country}")

                # Add Brand/SKU ABC (All Data) options (only if all age bands are selected or none selected)
                all_age_bands_selected = len(selected_age_bands) == len(age_band_names) or not selected_age_bands

                for country in countries:
                    if should_include_country(country):
                        if all_age_bands_selected:
                            filtered_options.append(f"Brand ABC (All Data) - {country}")
                            filtered_options.append(f"SKU ABC (All Data) - {country}")

                # Add Brand/SKU ABC per age band options
                for band in AGE_BANDS:
                    band_name = band['name']
                    if should_include_age_band(band_name):
                        for country in countries:
                            if should_include_country(country):
                                filtered_options.append(f"Brand ABC ({band_name}) - {country}")
                                filtered_options.append(f"SKU ABC ({band_name}) - {country}")

                # Multi-select for reports to download (pre-selected based on filters)
                selected_reports = st.multiselect(
                    "Select reports to download:",
                    options=filtered_options,
                    default=filtered_options,
                    help="Choose which reports to include in the Excel download"
                )

                # Add hint about default selection
                st.caption("💡 By default, all data is downloaded except US Local and CA Local.")

                if selected_reports:
                    # Filter reports based on selection
                    filtered_reports = {}
                    for report_name in selected_reports:
                        if report_name in all_download_reports:
                            filtered_reports[report_name] = all_download_reports[report_name]

                    if filtered_reports:
                        col1, col2, col3 = st.columns([2,1,2])
                        with col2:
                            # Generate Excel file for download
                            excel_file = create_excel_download(filtered_reports)

                            # Create download button
                            today = datetime.now()
                            filename = f"{today.strftime('%Y-%m-%d')} Inventory Analysis.xlsx"

                            st.download_button(
                                label=f"📥 Download {len(filtered_reports)} Selected Reports",
                                data=excel_file,
                                file_name=filename,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                type="primary",
                                use_container_width=True
                            )

                            st.success(f"✅ {len(filtered_reports)} reports ready for download")

            # ===== Step 9: Save Snapshot to Gist =====
            if gist_configured:
                st.markdown("---")
                st.subheader("📸 Step 9: Save Current Snapshot")

                snapshot_date = datetime.now().strftime("%Y-%m-%d")

                col1, col2 = st.columns([3, 1])
                with col1:
                    snapshot_date_input = st.text_input(
                        "Snapshot Date:",
                        value=snapshot_date,
                        help="Date identifier for this snapshot (YYYY-MM-DD)"
                    )

                with col2:
                    st.write("")  # Spacer
                    st.write("")

                    if st.button("💾 Save Snapshot", type="primary"):
                        with st.spinner("Saving snapshot to Gist..."):
                            success, debug_info = save_snapshot(df_processed, snapshot_date_input, gist_token, gist_id)

                        # Show debug info
                        with st.expander("🔧 Save Debug Info"):
                            for line in debug_info:
                                st.write(line)

                        if success:
                            st.success(f"✅ Snapshot saved successfully for date: {snapshot_date_input}")
                        else:
                            st.error("❌ Failed to save snapshot to Gist")

        except Exception as e:
            st.error(f"Error processing data: {str(e)}")
            st.exception(e)

if __name__ == "__main__":
    main()