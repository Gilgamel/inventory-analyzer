import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Inventory ABC Analysis System",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("📊 Inventory ABC Classification Analysis System")
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

# ========== 3. Google Sheets connection function ==========
@st.cache_resource
def connect_to_gsheet():
    """
    Connect to Google Sheets
    """
    try:
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
        client = gspread.authorize(credentials)
        
        return client
    except Exception as e:
        st.error(f"Failed to connect to Google Sheets: {str(e)}")
        return None

# ========== 4. Load static Warehouse Region mapping table ==========
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_warehouse_region_mapping():
    """
    Load static warehouse region mapping table from Google Sheets
    Table structure: Warehouse, Country, Warehouse Location, Type, Description
    """
    try:
        client = connect_to_gsheet()
        if client is None:
            return None
        
        # Get mapping sheet ID from secrets
        mapping_sheet_id = st.secrets["sheets"]["warehouse_region_sheet_id"]
        
        # Open mapping sheet
        sheet = client.open_by_key(mapping_sheet_id)
        
        # Get first worksheet
        worksheet = sheet.sheet1
        
        # Get all records
        records = worksheet.get_all_records()
        
        if not records:
            st.warning("Warehouse mapping table is empty")
            return None
        
        # Convert to DataFrame
        mapping_df = pd.DataFrame(records)
        
        # Display original column names for debugging
        st.write("Original column names in mapping table:", list(mapping_df.columns))
        
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
        
        # Show preview
        with st.expander("View Warehouse Mapping Table"):
            st.dataframe(mapping_df.head())
            st.write(f"Total records: {len(mapping_df)}")
            st.write(f"Country distribution: {mapping_df['Country'].value_counts().to_dict()}")
            if 'Warehouse_Location' in mapping_df.columns:
                st.write(f"Warehouse Location distribution: {mapping_df['Warehouse_Location'].value_counts().to_dict()}")
        
        return mapping_df
        
    except Exception as e:
        st.error(f"Failed to load warehouse mapping table: {str(e)}")
        return None

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
    
    # Calculate total inventory value
    value_cols = [f"{band['name']}_Value" for band in AGE_BANDS if f"{band['name']}_Value" in result.columns]
    if value_cols:
        result['Total_Value'] = result[value_cols].sum(axis=1)
    else:
        result['Total_Value'] = 0
    
    return result

# ========== 8. Modified ABC classification function ==========
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
        for group, group_df in df.groupby(group_col):
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

# ========== 9. Generate Report 1: Age Summary ==========
def generate_age_summary(df, country):
    """
    Generate age summary report
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
    summary_df['Value %'] = (summary_df['Inventory Value'] / total_value * 100).round(2)
    
    return summary_df

# ========== 10. Generate Report 2: Brand ABC Classification ==========
def generate_brand_abc(df, country):
    """
    Generate brand ABC classification report
    """
    if 'Country' not in df.columns or 'Brand' not in df.columns:
        return pd.DataFrame()
    
    country_df = df[df['Country'] == country].copy()
    
    if len(country_df) == 0:
        return pd.DataFrame()
    
    brand_summary = country_df.groupby('Brand').agg({
        'Total_Value': 'sum',
        'SKU': 'count',
        'Total_Inventory': 'sum'
    }).rename(columns={
        'SKU': 'SKU Count',
        'Total_Inventory': 'Total Qty'
    }).reset_index()
    
    brand_summary = brand_summary[brand_summary['Total_Value'] > 0]
    
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
    
    return brand_abc

# ========== 11. Generate Report 3: SKU ABC Classification ==========
def generate_sku_abc(df, country):
    """
    Generate SKU ABC classification report
    """
    if 'Country' not in df.columns:
        return pd.DataFrame()
    
    country_df = df[df['Country'] == country].copy()
    
    if len(country_df) == 0:
        return pd.DataFrame()
    
    # Prepare SKU-level data
    sku_cols = ['Brand', 'SKU', 'Product_Name', 'Total_Value', 'Total_Inventory']
    available_cols = [col for col in sku_cols if col in country_df.columns]
    
    if not available_cols:
        return pd.DataFrame()
    
    sku_data = country_df[available_cols].copy()
    sku_data = sku_data[sku_data['Total_Value'] > 0]
    
    if len(sku_data) == 0:
        return pd.DataFrame()
    
    # Get brand classification
    brand_abc = generate_brand_abc(df, country)
    if len(brand_abc) > 0 and 'Brand' in brand_abc.columns:
        brand_class_map = dict(zip(brand_abc['Brand'], brand_abc['Brand Class']))
        sku_data['Brand Class'] = sku_data['Brand'].map(brand_class_map)
    else:
        sku_data['Brand Class'] = 'Unclassified'
    
    # Use modified ABC classification function for SKU-level classification
    sku_abc = abc_classification(sku_data, 'Total_Value', group_col='Brand')
    
    sku_abc = sku_abc.rename(columns={
        'Brand': 'Brand',
        'SKU': 'SKU',
        'Product_Name': 'Product Name',
        'Total_Value': 'Inventory Value',
        'Total_Inventory': 'Inventory Qty',
        'value_pct': 'Value %',
        'cum_pct': 'Cumulative %',
        'abc_class': 'SKU Class'
    })
    
    return sku_abc

# ========== 12. Save to Google Sheets ==========
def save_to_gsheet(data_df, country, analysis_type):
    """
    Save data to corresponding country's Google Sheets history table
    """
    try:
        client = connect_to_gsheet()
        if client is None:
            return False
        
        # Add timestamp
        data_df = data_df.copy()
        data_df['Analysis Date'] = datetime.now().strftime('%Y-%m-%d')
        data_df['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Select history table based on country
        sheet_id_key = f"{country}_history_sheet_id"
        sheet_id = st.secrets["sheets"].get(sheet_id_key)
        
        if not sheet_id:
            st.error(f"No history table configured for {country}")
            return False
        
        # Open sheet
        sheet = client.open_by_key(sheet_id)
        
        # Create worksheet name: analysis_type_YYYYMM
        worksheet_name = f"{analysis_type}_{datetime.now().strftime('%Y%m')}"
        
        try:
            # Try to get existing worksheet
            worksheet = sheet.worksheet(worksheet_name)
            # Clear existing content
            worksheet.clear()
        except:
            # If doesn't exist, create new
            worksheet = sheet.add_worksheet(title=worksheet_name, rows=1000, cols=30)
        
        # Convert to list format
        headers = data_df.columns.tolist()
        records = data_df.values.tolist()
        
        # Write headers
        worksheet.append_row(headers)
        
        # Write data (batch write)
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            worksheet.append_rows(batch, value_input_option='USER_ENTERED')
        
        st.success(f"✅ {country} {analysis_type} data saved to history table")
        return True
        
    except Exception as e:
        st.error(f"Save failed: {str(e)}")
        return False

# ========== 13. Function to demonstrate ABC classification logic ==========
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

# ========== 14. Main program ==========
def main():
    st.sidebar.header("⚙️ System Information")
    
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
           - Contains "Warehouse" column
        
        3. **JOIN operation**
           - Inventory.Warehouse = Mapping.Warehouse
           - Add Country, Warehouse Location, Type, Description
        
        4. **Analysis by country**
           - Using modified ABC classification logic
           - Items crossing thresholds included in previous class
        
        5. **Save history**
           - Save to respective country history tables
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
            
            # ===== Step 5: Analysis by country =====
            st.subheader("📊 Step 5: Generate Analysis Reports")
            
            # Get unique countries
            if 'Country' not in df_with_values.columns:
                st.error("Unable to get country information, JOIN may have failed")
                st.stop()
            
            countries = df_with_values['Country'].unique()
            countries = [c for c in countries if pd.notna(c)]  # Filter NaN
            
            if len(countries) == 0:
                st.error("No valid country data")
                st.stop()
            
            st.success(f"Found {len(countries)} countries: {', '.join(countries)}")
            
            # Create tabs for each country
            tabs = st.tabs([f"{c}" if c == 'US' else f"{c}" if c == 'CA' else f"{c}" for c in countries])
            
            for tab, country in zip(tabs, countries):
                with tab:
                    country_data = df_with_values[df_with_values['Country'] == country]
                    
                    st.markdown(f"### {country} Inventory Analysis ({len(country_data)} records)")
                    
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
                    
                    # Report 1: Age Summary
                    st.markdown("#### Report 1: Age Summary")
                    age_summary = generate_age_summary(df_with_values, country)
                    
                    if not age_summary.empty:
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.dataframe(
                                age_summary.style.format({
                                    'Inventory Qty': '{:,.0f}',
                                    'Inventory Value': '${:,.2f}',
                                    'Value %': '{:.1f}%'
                                }),
                                use_container_width=True
                            )
                        with col2:
                            if st.button(f"💾 Save Age Summary", key=f"save_age_{country}"):
                                save_to_gsheet(age_summary, country, 'age_summary')
                    
                    # Report 2: Brand ABC
                    st.markdown("#### Report 2: Brand ABC Classification")
                    brand_abc = generate_brand_abc(df_with_values, country)
                    
                    if not brand_abc.empty:
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.dataframe(
                                brand_abc.style.format({
                                    'Inventory Value': '${:,.2f}',
                                    'SKU Count': '{:,.0f}',
                                    'Total Qty': '{:,.0f}',
                                    'Value %': '{:.2%}',
                                    'Cumulative %': '{:.2%}'
                                }),
                                use_container_width=True
                            )
                        with col2:
                            if st.button(f"💾 Save Brand ABC", key=f"save_brand_{country}"):
                                save_to_gsheet(brand_abc, country, 'brand_abc')
                    
                    # Report 3: SKU ABC
                    st.markdown("#### Report 3: SKU ABC Classification")
                    sku_abc = generate_sku_abc(df_with_values, country)
                    
                    if not sku_abc.empty:
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            display_cols = ['Brand Class', 'Brand', 'SKU', 'Product Name', 'Inventory Qty', 'Inventory Value', 'Value %', 'Cumulative %', 'SKU Class']
                            available_cols = [col for col in display_cols if col in sku_abc.columns]
                            
                            st.dataframe(
                                sku_abc[available_cols].head(100).style.format({
                                    'Inventory Qty': '{:,.0f}',
                                    'Inventory Value': '${:,.2f}',
                                    'Value %': '{:.2%}',
                                    'Cumulative %': '{:.2%}'
                                }),
                                use_container_width=True
                            )
                            st.caption(f"Showing first 100 rows, total {len(sku_abc)} rows")
                        with col2:
                            if st.button(f"💾 Save SKU ABC", key=f"save_sku_{country}"):
                                save_to_gsheet(sku_abc.head(1000), country, 'sku_abc')
            
        except Exception as e:
            st.error(f"Error processing data: {str(e)}")
            st.exception(e)

if __name__ == "__main__":
    main()