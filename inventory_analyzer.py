import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import numpy as np
from googleapiclient.discovery import build

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
def get_credentials():
    """
    Get Google credentials from secrets
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
        
        return credentials
    except Exception as e:
        st.error(f"Failed to get credentials: {str(e)}")
        return None

@st.cache_resource
def connect_to_gsheet():
    """
    Connect to Google Sheets using credentials
    """
    try:
        credentials = get_credentials()
        if credentials is None:
            return None
        
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        st.error(f"Failed to connect to Google Sheets: {str(e)}")
        return None

@st.cache_resource
def get_drive_service():
    """
    Get Google Drive service
    """
    try:
        credentials = get_credentials()
        if credentials is None:
            return None
        
        drive_service = build('drive', 'v3', credentials=credentials)
        return drive_service
    except Exception as e:
        st.error(f"Failed to create Drive service: {str(e)}")
        return None

# ========== 4. Get shared Drive ID ==========
def get_shared_drive_id(drive_service, drive_name="Inventory ABC Analyzer"):
    """
    Get the ID of a shared drive by name
    """
    try:
        # List all shared drives the service account has access to
        results = drive_service.drives().list(
            pageSize=100,
            fields="drives(id, name)"
        ).execute()
        
        drives = results.get('drives', [])
        
        for drive in drives:
            if drive['name'] == drive_name:
                st.success(f"Found shared drive: {drive_name}")
                return drive['id']
        
        st.error(f"Shared drive '{drive_name}' not found. Please create it and share with the service account.")
        return None
        
    except Exception as e:
        st.error(f"Error finding shared drive: {str(e)}")
        return None

# ========== 5. Get or create year folder in shared drive ==========
def get_or_create_year_folder_in_shared_drive(drive_service, shared_drive_id, year):
    """
    Get or create a year folder in the shared drive
    """
    try:
        # Search for existing year folder in shared drive
        query = f"name='{year}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = drive_service.files().list(
            q=query,
            spaces='drive',
            drives=[shared_drive_id],
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            fields='files(id, name)'
        ).execute()
        
        items = results.get('files', [])
        
        if items:
            # Folder exists
            folder_id = items[0]['id']
            st.info(f"Found existing year folder: {year}")
            return folder_id
        else:
            # Create new year folder in shared drive
            file_metadata = {
                'name': str(year),
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [shared_drive_id]
            }
            
            folder = drive_service.files().create(
                body=file_metadata,
                supportsAllDrives=True,
                fields='id'
            ).execute()
            
            folder_id = folder.get('id')
            st.success(f"Created new year folder: {year}")
            return folder_id
            
    except Exception as e:
        st.error(f"Error creating year folder: {str(e)}")
        return None

# ========== 6. Create Google Sheet in shared drive folder ==========
def create_sheet_in_shared_drive(drive_service, gsheet_client, folder_id, sheet_name):
    """
    Create a new Google Sheet in the specified folder within shared drive
    """
    try:
        # Create the spreadsheet
        spreadsheet = gsheet_client.create(sheet_name)
        file_id = spreadsheet.id
        
        # Move to folder in shared drive
        file_metadata = {
            'id': file_id,
            'parents': [folder_id]
        }
        
        drive_service.files().update(
            fileId=file_id,
            addParents=folder_id,
            supportsAllDrives=True,
            fields='id, parents'
        ).execute()
        
        return spreadsheet
    except Exception as e:
        st.error(f"Error creating sheet: {str(e)}")
        return None

# ========== 7. Load static Warehouse Region mapping table ==========
@st.cache_data(ttl=3600)
def load_warehouse_region_mapping():
    """
    Load static warehouse region mapping table from Google Sheets
    """
    try:
        client = connect_to_gsheet()
        if client is None:
            return None
        
        mapping_sheet_id = st.secrets["sheets"]["warehouse_region_sheet_id"]
        sheet = client.open_by_key(mapping_sheet_id)
        worksheet = sheet.sheet1
        records = worksheet.get_all_records()
        
        if not records:
            st.warning("Warehouse mapping table is empty")
            return None
        
        mapping_df = pd.DataFrame(records)
        mapping_df.columns = [str(col).strip() for col in mapping_df.columns]
        
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
        
        if column_mapping:
            mapping_df = mapping_df.rename(columns=column_mapping)
        
        required_cols = ['Warehouse', 'Country']
        missing_cols = [col for col in required_cols if col not in mapping_df.columns]
        
        if missing_cols:
            st.error(f"Missing required columns in mapping table: {missing_cols}")
            return None
        
        with st.expander("View Warehouse Mapping Table"):
            st.dataframe(mapping_df.head())
            st.write(f"Total records: {len(mapping_df)}")
            st.write(f"Country distribution: {mapping_df['Country'].value_counts().to_dict()}")
        
        return mapping_df
        
    except Exception as e:
        st.error(f"Failed to load warehouse mapping table: {str(e)}")
        return None

# ========== 8. JOIN inventory data with warehouse mapping table ==========
def join_with_warehouse_region(inventory_df, mapping_df):
    """
    JOIN inventory data with warehouse region table
    """
    if mapping_df is None or len(mapping_df) == 0:
        st.error("Warehouse mapping table is empty")
        return inventory_df
    
    warehouse_col_inventory = None
    
    if 'Warehouse' in inventory_df.columns:
        warehouse_col_inventory = 'Warehouse'
    else:
        for col in inventory_df.columns:
            if '仓库' in col or 'warehouse' in col.lower():
                warehouse_col_inventory = col
                break
    
    if warehouse_col_inventory is None:
        st.error("Cannot find warehouse column in inventory data")
        return inventory_df
    
    if 'Warehouse' not in mapping_df.columns:
        st.error("Mapping table missing Warehouse column")
        return inventory_df
    
    inventory_join = inventory_df.copy()
    mapping_join = mapping_df.copy()
    
    inventory_join['_join_key'] = inventory_join[warehouse_col_inventory].astype(str).str.strip().str.upper()
    mapping_join['_join_key'] = mapping_join['Warehouse'].astype(str).str.strip().str.upper()
    
    mapping_cols = ['_join_key', 'Country']
    if 'Warehouse_Location' in mapping_join.columns:
        mapping_cols.append('Warehouse_Location')
    if 'Type' in mapping_join.columns:
        mapping_cols.append('Type')
    if 'Description' in mapping_join.columns:
        mapping_cols.append('Description')
    
    merged_df = pd.merge(
        inventory_join,
        mapping_join[mapping_cols],
        on='_join_key',
        how='left'
    )
    
    merged_df = merged_df.drop('_join_key', axis=1)
    
    total_rows = len(merged_df)
    matched_rows = merged_df['Country'].notna().sum()
    match_rate = (matched_rows / total_rows * 100) if total_rows > 0 else 0
    
    st.success(f"""
    ✅ JOIN completed!
    - Total records: {total_rows}
    - Successfully matched: {matched_rows} ({match_rate:.1f}%)
    """)
    
    if 'Country' in merged_df.columns:
        country_counts = merged_df['Country'].value_counts()
        st.info(f"Country distribution: {dict(country_counts)}")
    
    return merged_df

# ========== 9. Data preprocessing function ==========
def preprocess_data(df):
    """
    Data preprocessing: rename columns
    """
    df_copy = df.copy()
    
    for chinese_name, english_name in COLUMN_MAPPING.items():
        if chinese_name in df_copy.columns:
            df_copy = df_copy.rename(columns={chinese_name: english_name})
    
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

# ========== 10. Calculate inventory value by age band ==========
def calculate_age_band_values(df):
    """
    Calculate inventory value for each SKU by age band
    """
    result = df.copy()
    
    for band in AGE_BANDS:
        band_name = band['name']
        cost_cols = [col for col in band['cost_cols'] if col in df.columns]
        if cost_cols:
            result[f'{band_name}_Value'] = result[cost_cols].sum(axis=1)
        else:
            result[f'{band_name}_Value'] = 0
        
        qty_cols = [col for col in band['qty_cols'] if col in df.columns]
        if qty_cols:
            result[f'{band_name}_Qty'] = result[qty_cols].sum(axis=1)
        else:
            result[f'{band_name}_Qty'] = 0
    
    value_cols = [f"{band['name']}_Value" for band in AGE_BANDS if f"{band['name']}_Value" in result.columns]
    if value_cols:
        result['Total_Value'] = result[value_cols].sum(axis=1)
    else:
        result['Total_Value'] = 0
    
    return result

# ========== 11. Modified ABC classification function ==========
def abc_classification(df, value_col, group_col=None):
    """
    ABC classification function - Modified version
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
                    
                    cum_pct = 0
                    cum_pct_list = []
                    
                    for pct in sorted_df['value_pct']:
                        cum_pct += pct
                        cum_pct_list.append(cum_pct)
                    
                    sorted_df['cum_pct'] = cum_pct_list
                    sorted_df['abc_class'] = 'C'
                    
                    a_mask = pd.Series(False, index=sorted_df.index)
                    prev_cum = 0
                    
                    for idx, cum in zip(sorted_df.index, cum_pct_list):
                        if cum <= 0.8 or (prev_cum < 0.8 and cum > 0.8):
                            a_mask[idx] = True
                        prev_cum = cum
                    
                    sorted_df.loc[a_mask, 'abc_class'] = 'A'
                    
                    b_mask = pd.Series(False, index=sorted_df.index)
                    prev_cum = 0
                    
                    for idx, cum in zip(sorted_df.index, cum_pct_list):
                        if cum <= 0.95 or (prev_cum < 0.95 and cum > 0.95):
                            if not a_mask[idx]:
                                b_mask[idx] = True
                        prev_cum = cum
                    
                    b_mask = b_mask & ~a_mask
                    sorted_df.loc[b_mask, 'abc_class'] = 'B'
                    
                else:
                    sorted_df['value_pct'] = 0
                    sorted_df['cum_pct'] = 0
                    sorted_df['abc_class'] = 'C'
                
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
            
            a_mask = pd.Series(False, index=sorted_df.index)
            prev_cum = 0
            
            for idx, cum in zip(sorted_df.index, cum_pct_list):
                if cum <= 0.8 or (prev_cum < 0.8 and cum > 0.8):
                    a_mask[idx] = True
                prev_cum = cum
            
            sorted_df.loc[a_mask, 'abc_class'] = 'A'
            
            b_mask = pd.Series(False, index=sorted_df.index)
            prev_cum = 0
            
            for idx, cum in zip(sorted_df.index, cum_pct_list):
                if cum <= 0.95 or (prev_cum < 0.95 and cum > 0.95):
                    if not a_mask[idx]:
                        b_mask[idx] = True
                prev_cum = cum
            
            b_mask = b_mask & ~a_mask
            sorted_df.loc[b_mask, 'abc_class'] = 'B'
            
        else:
            sorted_df['value_pct'] = 0
            sorted_df['cum_pct'] = 0
            sorted_df['abc_class'] = 'C'
        
        return sorted_df

# ========== 12. Generate Report 1: Age Summary ==========
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
    summary_df['Country'] = country
    summary_df['Report Type'] = 'Age Summary'
    
    return summary_df

# ========== 13. Generate Report 2: Brand ABC Classification ==========
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
        'Total_Inventory': 'Inventory Qty'
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
    
    column_order = ['Brand', 'Inventory Qty', 'Inventory Value', 'SKU Count', 'Value %', 'Cumulative %', 'Brand Class']
    brand_abc = brand_abc[[col for col in column_order if col in brand_abc.columns]]
    brand_abc['Country'] = country
    brand_abc['Report Type'] = 'Brand ABC'
    
    return brand_abc

# ========== 14. Generate Report 3: SKU ABC Classification ==========
def generate_sku_abc(df, country):
    """
    Generate SKU ABC classification report
    """
    if 'Country' not in df.columns:
        return pd.DataFrame()
    
    country_df = df[df['Country'] == country].copy()
    
    if len(country_df) == 0:
        return pd.DataFrame()
    
    sku_cols = ['Brand', 'SKU', 'Product_Name', 'Total_Value', 'Total_Inventory']
    available_cols = [col for col in sku_cols if col in country_df.columns]
    
    if not available_cols:
        return pd.DataFrame()
    
    sku_data = country_df[available_cols].copy()
    sku_data = sku_data[sku_data['Total_Value'] > 0]
    
    if len(sku_data) == 0:
        return pd.DataFrame()
    
    brand_abc = generate_brand_abc(df, country)
    if len(brand_abc) > 0 and 'Brand' in brand_abc.columns:
        brand_class_map = dict(zip(brand_abc['Brand'], brand_abc['Brand Class']))
        sku_data['Brand Class'] = sku_data['Brand'].map(brand_class_map)
    else:
        sku_data['Brand Class'] = 'Unclassified'
    
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
    
    brand_class_order = {'A': 0, 'B': 1, 'C': 2, 'Unclassified': 3}
    sku_abc['brand_sort'] = sku_abc['Brand Class'].map(brand_class_order)
    sku_abc = sku_abc.sort_values(['brand_sort', 'Inventory Value'], ascending=[True, False])
    sku_abc = sku_abc.drop('brand_sort', axis=1)
    
    display_cols = ['Brand Class', 'Brand', 'SKU', 'Product Name', 'Inventory Qty', 'Inventory Value', 'Value %', 'Cumulative %', 'SKU Class']
    sku_abc = sku_abc[[col for col in display_cols if col in sku_abc.columns]]
    sku_abc['Country'] = country
    sku_abc['Report Type'] = 'SKU ABC'
    
    return sku_abc

# ========== 15. Save all reports to shared drive ==========
def save_all_to_shared_drive(all_reports, sheet_name):
    """
    Save all reports to a single Google Sheet in the shared drive
    """
    try:
        gsheet_client = connect_to_gsheet()
        if gsheet_client is None:
            return False
        
        drive_service = get_drive_service()
        if drive_service is None:
            return False
        
        # Get the shared drive ID
        shared_drive_id = get_shared_drive_id(drive_service, "Inventory ABC Analyzer")
        if shared_drive_id is None:
            st.error("Cannot access shared drive. Please ensure:")
            st.error("1. The shared drive 'Inventory ABC Analyzer' exists")
            st.error("2. The service account has been added as a member")
            return False
        
        # Get current year
        current_year = str(datetime.now().year)
        
        # Get or create year folder in shared drive
        year_folder_id = get_or_create_year_folder_in_shared_drive(drive_service, shared_drive_id, current_year)
        if year_folder_id is None:
            return False
        
        # Create new spreadsheet in the year folder
        spreadsheet = create_sheet_in_shared_drive(drive_service, gsheet_client, year_folder_id, sheet_name)
        if spreadsheet is None:
            return False
        
        # Add each report as a worksheet
        worksheet_count = 0
        for report_name, report_df in all_reports.items():
            if not report_df.empty:
                try:
                    clean_name = report_name.replace('_', ' ')[:50]
                    
                    worksheet = spreadsheet.add_worksheet(title=clean_name, rows=len(report_df)+1, cols=len(report_df.columns))
                    
                    report_df = report_df.copy()
                    report_df['Analysis Date'] = datetime.now().strftime('%Y-%m-%d')
                    report_df['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    headers = report_df.columns.tolist()
                    records = report_df.values.tolist()
                    
                    worksheet.append_row(headers)
                    
                    batch_size = 100
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i+batch_size]
                        worksheet.append_rows(batch, value_input_option='USER_ENTERED')
                    
                    worksheet_count += 1
                    st.info(f"✅ Added worksheet: {clean_name} ({len(report_df)} rows)")
                    
                except Exception as e:
                    st.warning(f"Failed to add worksheet {report_name}: {str(e)}")
        
        # Remove default "Sheet1"
        try:
            default_sheet = spreadsheet.worksheet("Sheet1")
            if len(default_sheet.get_all_values()) <= 1:
                spreadsheet.del_worksheet(default_sheet)
        except:
            pass
        
        # Get the shared drive folder link
        folder_link = f"https://drive.google.com/drive/folders/{year_folder_id}"
        
        st.success(f"""
        ✅ All reports saved successfully to Shared Drive!
        - Shared Drive: Inventory ABC Analyzer
        - Year Folder: {current_year}
        - Spreadsheet: {sheet_name}
        - Total worksheets: {worksheet_count}
        """)
        
        st.markdown(f"📁 [Open Year Folder in Google Drive]({folder_link})")
        st.markdown(f"📊 [Open Spreadsheet in Google Sheets](https://docs.google.com/spreadsheets/d/{spreadsheet.id})")
        
        return True
        
    except Exception as e:
        st.error(f"Failed to save to shared drive: {str(e)}")
        return False

# ========== 16. Function to demonstrate ABC classification logic ==========
def demonstrate_abc_logic():
    """
    Demonstrate the modified ABC classification logic
    """
    st.subheader("📊 ABC Classification Logic Demonstration")
    
    example_data = pd.DataFrame({
        'Item': ['Item1', 'Item2', 'Item3', 'Item4', 'Item5', 'Item6'],
        'Value': [400, 300, 200, 50, 30, 20]
    })
    
    st.write("Example data:")
    st.dataframe(example_data)
    
    result = abc_classification(example_data, 'Value')
    
    st.write("Classification result:")
    st.dataframe(result.style.format({
        'value_pct': '{:.2%}',
        'cum_pct': '{:.2%}'
    }))
    
    st.info("""
    **Logic explanation:**
    - Items crossing 80% threshold are included in A class
    - Items crossing 95% threshold are included in B class
    """)

# ========== 17. Main program ==========
def main():
    st.sidebar.header("⚙️ System Information")
    
    with st.sidebar:
        st.markdown("""
        ### 📋 Data Flow
        1. **Load static mapping table** from Google Sheets
        2. **Upload inventory data**
        3. **JOIN with warehouse mapping**
        4. **Generate analysis reports**
        5. **Save to Shared Drive**
        """)
        
        st.markdown("---")
        
        if st.button("📊 View ABC Classification Demo"):
            demonstrate_abc_logic()
        
        if st.button("🔄 Test Google Sheets Connection"):
            client = connect_to_gsheet()
            if client:
                st.success("✅ Connection successful")
            else:
                st.error("❌ Connection failed")
    
    st.subheader("📤 Upload Inventory Data File")
    inventory_file = st.file_uploader(
        "Please upload inventory report in Excel format",
        type=['xlsx', 'xls']
    )
    
    if inventory_file:
        try:
            df = pd.read_excel(inventory_file)
            
            with st.expander("View Raw Data Preview"):
                st.dataframe(df.head())
                st.write(f"Total rows: {len(df)}")
            
            st.subheader("🗺️ Step 1: Load Warehouse Region Mapping Table")
            mapping_df = load_warehouse_region_mapping()
            
            if mapping_df is None:
                st.stop()
            
            st.subheader("🔗 Step 2: JOIN Inventory Data with Warehouse Mapping")
            df_with_region = join_with_warehouse_region(df, mapping_df)
            
            st.subheader("🔄 Step 3: Data Preprocessing")
            df_processed = preprocess_data(df_with_region)
            
            st.subheader("💰 Step 4: Calculate Inventory Value")
            df_with_values = calculate_age_band_values(df_processed)
            
            st.subheader("📊 Step 5: Generate Analysis Reports")
            
            if 'Country' not in df_with_values.columns:
                st.error("No country information found")
                st.stop()
            
            countries = df_with_values['Country'].unique()
            countries = [c for c in countries if pd.notna(c)]
            
            st.success(f"Found {len(countries)} countries: {', '.join(countries)}")
            
            all_reports = {}
            tabs = st.tabs([f"{c}" for c in countries])
            
            for tab, country in zip(tabs, countries):
                with tab:
                    country_data = df_with_values[df_with_values['Country'] == country]
                    st.markdown(f"### {country} Inventory Analysis ({len(country_data)} records)")
                    
                    # Report 1: Age Summary
                    st.markdown("#### Report 1: Age Summary")
                    age_summary = generate_age_summary(df_with_values, country)
                    if not age_summary.empty:
                        st.dataframe(age_summary, use_container_width=True)
                        all_reports[f"{country}_Age_Summary"] = age_summary
                    
                    # Report 2: Brand ABC
                    st.markdown("#### Report 2: Brand ABC")
                    brand_abc = generate_brand_abc(df_with_values, country)
                    if not brand_abc.empty:
                        st.dataframe(brand_abc, use_container_width=True)
                        all_reports[f"{country}_Brand_ABC"] = brand_abc
                    
                    # Report 3: SKU ABC
                    st.markdown("#### Report 3: SKU ABC")
                    sku_abc = generate_sku_abc(df_with_values, country)
                    if not sku_abc.empty:
                        st.dataframe(sku_abc.head(100), use_container_width=True)
                        all_reports[f"{country}_SKU_ABC"] = sku_abc
            
            if all_reports:
                st.markdown("---")
                st.subheader("☁️ Step 6: Save All Results to Shared Drive")
                
                if st.button("💾 Save All Result to Shared Drive", type="primary", use_container_width=True):
                    with st.spinner("Saving all reports to shared drive..."):
                        today = datetime.now()
                        sheet_name = f"{today.strftime('%Y-%m-%d')} Inventory Analysis"
                        save_all_to_shared_drive(all_reports, sheet_name)
            
        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.exception(e)

if __name__ == "__main__":
    main()