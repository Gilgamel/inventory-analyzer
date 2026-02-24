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
@st.cache_data(ttl=3600)
def load_warehouse_region_mapping():
    """
    Load static warehouse region mapping table from Google Sheets
    Expected columns: Warehouse, Country, Type, Description
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
        
        # Display column names for debugging
        st.write("Columns in mapping table:", list(mapping_df.columns))
        
        # Clean column names (remove extra spaces)
        mapping_df.columns = [str(col).strip() for col in mapping_df.columns]
        
        # Define expected columns and their possible variations
        column_mapping = {}
        
        for col in mapping_df.columns:
            col_lower = col.lower()
            
            # Map Warehouse column
            if 'warehouse' in col_lower or '仓库' in col_lower:
                column_mapping[col] = 'Warehouse'
            
            # Map Country column
            elif 'country' in col_lower or '国家' in col_lower:
                column_mapping[col] = 'Country'
            
            # Map Type column
            elif 'type' in col_lower or '类型' in col_lower:
                column_mapping[col] = 'Type'
            
            # Map Description column
            elif 'description' in col_lower or '描述' in col_lower:
                column_mapping[col] = 'Description'
        
        # Rename columns based on mapping
        if column_mapping:
            mapping_df = mapping_df.rename(columns=column_mapping)
        
        # Ensure required columns exist
        required_cols = ['Warehouse', 'Country']
        missing_cols = [col for col in required_cols if col not in mapping_df.columns]
        
        if missing_cols:
            st.error(f"Missing required columns in mapping table: {missing_cols}")
            return None
        
        # Keep only necessary columns
        keep_cols = ['Warehouse', 'Country']
        if 'Type' in mapping_df.columns:
            keep_cols.append('Type')
        if 'Description' in mapping_df.columns:
            keep_cols.append('Description')
        
        mapping_df = mapping_df[keep_cols]
        
        # Remove rows with missing values
        mapping_df = mapping_df.dropna(subset=['Warehouse', 'Country'])
        
        # Remove duplicates
        mapping_df = mapping_df.drop_duplicates(subset=['Warehouse'])
        
        # Show preview
        with st.expander("View Warehouse Mapping Table"):
            st.dataframe(mapping_df.head())
            st.write(f"Total records: {len(mapping_df)}")
            st.write(f"Country distribution: {mapping_df['Country'].value_counts().to_dict()}")
        
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
    
    st.info(f"""
    **JOIN Information:**
    - Left table (inventory): {warehouse_col_inventory}
    - Right table (mapping): Warehouse
    """)
    
    # Prepare data for joining
    inventory_join = inventory_df.copy()
    mapping_join = mapping_df.copy()
    
    # Create join keys (standardize for matching)
    inventory_join['_join_key'] = inventory_join[warehouse_col_inventory].astype(str).str.strip().str.upper()
    mapping_join['_join_key'] = mapping_join['Warehouse'].astype(str).str.strip().str.upper()
    
    # Select columns from mapping table
    mapping_cols = ['_join_key', 'Country']
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
    
    # Calculate match statistics
    total_rows = len(merged_df)
    matched_rows = merged_df['Country'].notna().sum()
    match_rate = (matched_rows / total_rows * 100) if total_rows > 0 else 0
    
    st.success(f"""
    ✅ JOIN completed!
    - Total records: {total_rows}
    - Successfully matched: {matched_rows} ({match_rate:.1f}%)
    """)
    
    # Show unmatched warehouses
    if matched_rows < total_rows:
        unmatched = merged_df[merged_df['Country'].isna()][warehouse_col_inventory].unique()
        st.warning(f"Unmatched warehouses: {', '.join([str(w) for w in unmatched[:10]])}")
    
    # Show country distribution
    country_counts = merged_df['Country'].value_counts()
    st.info(f"Country distribution: {dict(country_counts)}")
    
    return merged_df

# ========== 6. Data preprocessing function ==========
def preprocess_data(df):
    """
    Data preprocessing: rename columns
    """
    df_copy = df.copy()
    
    # Rename Chinese columns to English
    for chinese_name, english_name in COLUMN_MAPPING.items():
        if chinese_name in df_copy.columns:
            df_copy = df_copy.rename(columns={chinese_name: english_name})
    
    # Convert numeric columns
    numeric_cols = ['Total_Inventory', 'Available_Qty', 'Reserved_Qty', 'Defect_Qty',
                    'Pending_Inspection', 'Transfer_Transit', 'FBA_Transit', 
                    'FBA_Planned', 'Expected_Receipt', 'Projected_Inventory']
    
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
    
    for band in AGE_BANDS:
        band_name = band['name']
        
        # Calculate cost
        cost_cols = [col for col in band['cost_cols'] if col in df.columns]
        if cost_cols:
            result[f'{band_name}_Value'] = result[cost_cols].sum(axis=1)
        else:
            result[f'{band_name}_Value'] = 0
        
        # Calculate quantity
        qty_cols = [col for col in band['qty_cols'] if col in df.columns]
        if qty_cols:
            result[f'{band_name}_Qty'] = result[qty_cols].sum(axis=1)
        else:
            result[f'{band_name}_Qty'] = 0
    
    # Calculate total value
    value_cols = [f"{band['name']}_Value" for band in AGE_BANDS if f"{band['name']}_Value" in result.columns]
    if value_cols:
        result['Total_Value'] = result[value_cols].sum(axis=1)
    else:
        result['Total_Value'] = 0
    
    return result

# ========== 8. ABC classification function ==========
def abc_classification(df, value_col, group_col=None):
    """
    ABC classification function - Modified version
    When cumulative percentage crosses threshold, include crossing item in previous class
    """
    if df.empty or value_col not in df.columns:
        return df
    
    if group_col and group_col in df.columns:
        result_dfs = []
        for group, group_df in df.groupby(group_col):
            if len(group_df) > 0:
                sorted_df = group_df.sort_values(value_col, ascending=False).copy()
                total = sorted_df[value_col].sum()
                
                if total > 0:
                    sorted_df['value_pct'] = sorted_df[value_col] / total
                    
                    # Calculate cumulative percentage
                    cum_pct = 0
                    cum_pct_list = []
                    for pct in sorted_df['value_pct']:
                        cum_pct += pct
                        cum_pct_list.append(cum_pct)
                    
                    sorted_df['cum_pct'] = cum_pct_list
                    sorted_df['abc_class'] = 'C'  # Default
                    
                    # A class: <= 0.8 OR crosses from <0.8 to >0.8
                    a_mask = pd.Series(False, index=sorted_df.index)
                    prev_cum = 0
                    for idx, cum in zip(sorted_df.index, cum_pct_list):
                        if cum <= 0.8 or (prev_cum < 0.8 and cum > 0.8):
                            a_mask[idx] = True
                        prev_cum = cum
                    
                    sorted_df.loc[a_mask, 'abc_class'] = 'A'
                    
                    # B class: after A, <= 0.95 OR crosses from <0.95 to >0.95
                    b_mask = pd.Series(False, index=sorted_df.index)
                    prev_cum = 0
                    for idx, cum in zip(sorted_df.index, cum_pct_list):
                        if cum <= 0.95 or (prev_cum < 0.95 and cum > 0.95):
                            if not a_mask[idx]:
                                b_mask[idx] = True
                        prev_cum = cum
                    
                    sorted_df.loc[b_mask & ~a_mask, 'abc_class'] = 'B'
                
                result_dfs.append(sorted_df)
        
        return pd.concat(result_dfs, ignore_index=True) if result_dfs else df
    
    else:
        sorted_df = df.sort_values(value_col, ascending=False).copy()
        total = sorted_df[value_col].sum()
        
        if total > 0:
            sorted_df['value_pct'] = sorted_df[value_col] / total
            
            cum_pct = 0
            cum_pct_list = []
            for pct in sorted_df['value_pct']:
                cum_pct += pct
                cum_pct_list.append(cum_pct)
            
            sorted_df['cum_pct'] = cum_pct_list
            sorted_df['abc_class'] = 'C'
            
            # A class
            a_mask = pd.Series(False, index=sorted_df.index)
            prev_cum = 0
            for idx, cum in zip(sorted_df.index, cum_pct_list):
                if cum <= 0.8 or (prev_cum < 0.8 and cum > 0.8):
                    a_mask[idx] = True
                prev_cum = cum
            
            sorted_df.loc[a_mask, 'abc_class'] = 'A'
            
            # B class
            b_mask = pd.Series(False, index=sorted_df.index)
            prev_cum = 0
            for idx, cum in zip(sorted_df.index, cum_pct_list):
                if cum <= 0.95 or (prev_cum < 0.95 and cum > 0.95):
                    if not a_mask[idx]:
                        b_mask[idx] = True
                prev_cum = cum
            
            sorted_df.loc[b_mask & ~a_mask, 'abc_class'] = 'B'
        
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
    
    # Prepare SKU data
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
    if len(brand_abc) > 0:
        brand_class_map = dict(zip(brand_abc['Brand'], brand_abc['Brand Class']))
        sku_data['Brand Class'] = sku_data['Brand'].map(brand_class_map)
    else:
        sku_data['Brand Class'] = 'Unclassified'
    
    # SKU-level classification
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
    Save data to Google Sheets history table
    """
    try:
        client = connect_to_gsheet()
        if client is None:
            return False
        
        data_df = data_df.copy()
        data_df['Analysis Date'] = datetime.now().strftime('%Y-%m-%d')
        data_df['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        sheet_id_key = f"{country}_history_sheet_id"
        sheet_id = st.secrets["sheets"].get(sheet_id_key)
        
        if not sheet_id:
            st.error(f"No history table configured for {country}")
            return False
        
        sheet = client.open_by_key(sheet_id)
        worksheet_name = f"{analysis_type}_{datetime.now().strftime('%Y%m')}"
        
        try:
            worksheet = sheet.worksheet(worksheet_name)
            worksheet.clear()
        except:
            worksheet = sheet.add_worksheet(title=worksheet_name, rows=1000, cols=30)
        
        headers = data_df.columns.tolist()
        records = data_df.values.tolist()
        
        worksheet.append_row(headers)
        
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            worksheet.append_rows(batch, value_input_option='USER_ENTERED')
        
        st.success(f"✅ {country} {analysis_type} data saved")
        return True
        
    except Exception as e:
        st.error(f"Save failed: {str(e)}")
        return False

# ========== 13. Main program ==========
def main():
    st.sidebar.header("⚙️ System Information")
    
    with st.sidebar:
        st.markdown("""
        ### 📋 Data Flow
        1. **Load mapping table** from Google Sheets
        2. **Upload inventory data**
        3. **JOIN on Warehouse**
        4. **Analysis by Country**
        5. **Save to history**
        """)
        
        if st.button("🔄 Test Connection"):
            client = connect_to_gsheet()
            if client:
                st.success("✅ Connection successful")
            else:
                st.error("❌ Connection failed")
    
    st.subheader("📤 Upload Inventory Data File")
    inventory_file = st.file_uploader(
        "Upload inventory report (Excel format)",
        type=['xlsx', 'xls']
    )
    
    if inventory_file:
        try:
            # Read inventory data
            df = pd.read_excel(inventory_file)
            
            with st.expander("View Raw Data"):
                st.dataframe(df.head())
                st.write(f"Total rows: {len(df)}")
            
            # Step 1: Load mapping table
            st.subheader("🗺️ Step 1: Load Mapping Table")
            mapping_df = load_warehouse_region_mapping()
            
            if mapping_df is None:
                st.stop()
            
            # Step 2: JOIN
            st.subheader("🔗 Step 2: JOIN Data")
            df_with_region = join_with_warehouse_region(df, mapping_df)
            
            # Step 3: Preprocess
            st.subheader("🔄 Step 3: Preprocess")
            df_processed = preprocess_data(df_with_region)
            
            # Step 4: Calculate values
            st.subheader("💰 Step 4: Calculate Values")
            df_with_values = calculate_age_band_values(df_processed)
            
            # Step 5: Analysis
            st.subheader("📊 Step 5: Analysis")
            
            if 'Country' not in df_with_values.columns:
                st.error("No Country column after JOIN")
                st.stop()
            
            countries = df_with_values['Country'].dropna().unique()
            
            if len(countries) == 0:
                st.error("No valid country data")
                st.stop()
            
            st.success(f"Found {len(countries)} countries: {', '.join(countries)}")
            
            # Create tabs for each country
            tabs = st.tabs([f"🌍 {c}" for c in countries])
            
            for tab, country in zip(tabs, countries):
                with tab:
                    st.markdown(f"### {country} Analysis")
                    
                    # Report 1: Age Summary
                    st.markdown("#### Report 1: Age Summary")
                    age_summary = generate_age_summary(df_with_values, country)
                    
                    if not age_summary.empty:
                        st.dataframe(
                            age_summary.style.format({
                                'Inventory Qty': '{:,.0f}',
                                'Inventory Value': '${:,.2f}',
                                'Value %': '{:.1f}%'
                            }),
                            use_container_width=True
                        )
                    
                    # Report 2: Brand ABC
                    st.markdown("#### Report 2: Brand ABC")
                    brand_abc = generate_brand_abc(df_with_values, country)
                    
                    if not brand_abc.empty:
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
                    
                    # Report 3: SKU ABC
                    st.markdown("#### Report 3: SKU ABC")
                    sku_abc = generate_sku_abc(df_with_values, country)
                    
                    if not sku_abc.empty:
                        display_cols = ['Brand Class', 'Brand', 'SKU', 'Product Name', 
                                      'Inventory Qty', 'Inventory Value', 'Value %', 
                                      'Cumulative %', 'SKU Class']
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
                        
        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.exception(e)

if __name__ == "__main__":
    main()