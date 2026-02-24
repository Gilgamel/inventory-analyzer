import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import numpy as np

# 页面配置
st.set_page_config(
    page_title="库存ABC分析系统",
    page_icon="📊",
    layout="wide"
)

# 标题
st.title("📊 库存ABC分类分析系统")
st.markdown("---")

# ========== 1. 列名映射（您的原始列名）==========
COLUMN_MAPPING = {
    '品名': 'Product_Name',
    'SKU': 'SKU',
    '仓库': 'Warehouse_Original',  # 先映射为临时列名，避免冲突
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

# ========== 2. 库龄区间定义 ==========
AGE_BANDS = [
    {'name': '0-60天', 'qty_cols': ['Age_0_30_Qty', 'Age_31_60_Qty'], 
     'cost_cols': ['Age_0_30_Cost', 'Age_31_60_Cost']},
    {'name': '61-90天', 'qty_cols': ['Age_61_90_Qty'], 
     'cost_cols': ['Age_61_90_Cost']},
    {'name': '91-180天', 'qty_cols': ['Age_91_180_Qty'], 
     'cost_cols': ['Age_91_180_Cost']},
    {'name': '181-365天', 'qty_cols': ['Age_181_270_Qty', 'Age_271_330_Qty', 'Age_331_365_Qty'], 
     'cost_cols': ['Age_181_270_Cost', 'Age_271_330_Cost', 'Age_331_365_Cost']},
    {'name': '365+天', 'qty_cols': ['Age_365_Plus_Qty'], 
     'cost_cols': ['Age_365_Plus_Cost']}
]

# ========== 3. Google Sheets连接函数 ==========
@st.cache_resource
def connect_to_gsheet():
    """
    连接到Google Sheets
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
        st.error(f"连接Google Sheets失败: {str(e)}")
        return None

# ========== 4. 加载静态 Warehouse Region 映射表 ==========
@st.cache_data(ttl=3600)
def load_warehouse_region_mapping():
    """
    从Google Sheets加载静态的仓库区域映射表
    表结构: Warehouse, Country, Type, Description, Country Code
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
            st.warning("仓库映射表为空")
            return None
        
        mapping_df = pd.DataFrame(records)
        
        # 标准化列名：去除空格
        mapping_df.columns = [str(col).strip() for col in mapping_df.columns]
        
        # 创建新的列名映射
        new_columns = {}
        for col in mapping_df.columns:
            col_lower = col.lower()
            if 'warehouse' in col_lower or '仓库' in col:
                new_columns[col] = 'Warehouse'
            elif 'country code' in col_lower or '国家代码' in col_lower:
                new_columns[col] = 'Country_Code'
            elif 'country' in col_lower and 'code' not in col_lower:
                new_columns[col] = 'Country_Name'
            elif 'type' in col_lower or '类型' in col_lower:
                new_columns[col] = 'Type'
            elif 'description' in col_lower or '描述' in col_lower:
                new_columns[col] = 'Description'
        
        if new_columns:
            mapping_df = mapping_df.rename(columns=new_columns)
        
        # 检查必要列
        if 'Warehouse' not in mapping_df.columns:
            st.error("仓库映射表中找不到Warehouse列")
            st.write("现有列名:", list(mapping_df.columns))
            return None
        
        # 查找Country Code列（可能是多种命名）
        country_code_col = None
        for col in mapping_df.columns:
            if col in ['Country_Code', 'Country Code', '国家代码']:
                country_code_col = col
                break
        
        if country_code_col is None:
            st.error("仓库映射表中找不到Country Code列")
            st.write("现有列名:", list(mapping_df.columns))
            return None
        
        # 重命名为统一的Country_Code
        if country_code_col != 'Country_Code':
            mapping_df = mapping_df.rename(columns={country_code_col: 'Country_Code'})
        
        # 显示预览
        with st.expander("查看仓库映射表"):
            st.dataframe(mapping_df.head())
            st.write(f"总记录数: {len(mapping_df)}")
            st.write(f"国家代码分布: {mapping_df['Country_Code'].value_counts().to_dict()}")
        
        return mapping_df
        
    except Exception as e:
        st.error(f"加载仓库映射表失败: {str(e)}")
        return None

# ========== 5. JOIN库存数据和仓库映射表 ==========
def join_with_warehouse_region(inventory_df, mapping_df):
    """
    将库存数据与仓库区域表进行JOIN
    """
    if mapping_df is None or len(mapping_df) == 0:
        st.error("仓库映射表为空，无法进行JOIN")
        return inventory_df
    
    # 在库存数据中查找仓库列
    warehouse_col_inventory = None
    for col in inventory_df.columns:
        if '仓库' in col or 'warehouse' in col.lower():
            warehouse_col_inventory = col
            break
    
    if warehouse_col_inventory is None:
        st.error("库存数据中找不到仓库列")
        st.write("库存数据列名:", list(inventory_df.columns))
        return inventory_df
    
    st.info(f"""
    **JOIN信息:**
    - 库存数据仓库列: {warehouse_col_inventory}
    - 映射表仓库列: Warehouse
    - 国家标识: Country_Code
    """)
    
    # 准备数据
    inventory_join = inventory_df.copy()
    mapping_join = mapping_df.copy()
    
    # 创建JOIN键（统一转为大写并去除空格）
    inventory_join['_join_key'] = inventory_join[warehouse_col_inventory].astype(str).str.strip().str.upper()
    mapping_join['_join_key'] = mapping_join['Warehouse'].astype(str).str.strip().str.upper()
    
    # 选择需要的列
    mapping_cols = ['_join_key', 'Country_Code']
    if 'Type' in mapping_join.columns:
        mapping_cols.append('Type')
    if 'Description' in mapping_join.columns:
        mapping_cols.append('Description')
    if 'Country_Name' in mapping_join.columns:
        mapping_cols.append('Country_Name')
    
    # 执行LEFT JOIN
    merged_df = pd.merge(
        inventory_join,
        mapping_join[mapping_cols],
        on='_join_key',
        how='left'
    )
    
    # 删除临时列
    merged_df = merged_df.drop('_join_key', axis=1)
    
    # 重命名Country_Code为Country
    merged_df = merged_df.rename(columns={'Country_Code': 'Country'})
    
    # 统计匹配情况
    total_rows = len(merged_df)
    matched_rows = merged_df['Country'].notna().sum()
    match_rate = (matched_rows / total_rows * 100) if total_rows > 0 else 0
    
    st.success(f"""
    ✅ JOIN完成！
    - 总记录数: {total_rows}
    - 匹配成功: {matched_rows} ({match_rate:.1f}%)
    """)
    
    # 显示未匹配的仓库
    if matched_rows < total_rows:
        unmatched = merged_df[merged_df['Country'].isna()][warehouse_col_inventory].unique()
        st.warning(f"未匹配的仓库: {', '.join([str(w) for w in unmatched[:10]])}")
    
    return merged_df

# ========== 6. 数据预处理函数 ==========
def preprocess_data(df):
    """
    数据预处理：重命名列名
    """
    # 创建副本避免修改原数据
    df_copy = df.copy()
    
    # 重命名列
    for chinese_name, english_name in COLUMN_MAPPING.items():
        if chinese_name in df_copy.columns:
            df_copy = df_copy.rename(columns={chinese_name: english_name})
    
    # 确保数值列为数字类型
    numeric_cols = ['Total_Inventory', 'Available_Qty', 'Reserved_Qty', 'Defect_Qty',
                    'Pending_Inspection', 'Transfer_Transit', 'FBA_Transit', 
                    'FBA_Planned', 'Expected_Receipt', 'Projected_Inventory']
    
    # 添加所有库龄相关的列
    for band in AGE_BANDS:
        numeric_cols.extend(band['qty_cols'])
        numeric_cols.extend(band['cost_cols'])
    
    for col in numeric_cols:
        if col in df_copy.columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)
    
    return df_copy

# ========== 7. 计算各库龄段库存价值 ==========
def calculate_age_band_values(df):
    """
    计算每个SKU在各库龄段的库存价值
    """
    result = df.copy()
    
    for band in AGE_BANDS:
        band_name = band['name']
        
        # 计算该库龄段的总成本
        cost_cols = [col for col in band['cost_cols'] if col in df.columns]
        if cost_cols:
            result[f'{band_name}_Value'] = result[cost_cols].sum(axis=1)
        else:
            result[f'{band_name}_Value'] = 0
        
        # 计算该库龄段的总数量
        qty_cols = [col for col in band['qty_cols'] if col in df.columns]
        if qty_cols:
            result[f'{band_name}_Qty'] = result[qty_cols].sum(axis=1)
        else:
            result[f'{band_name}_Qty'] = 0
    
    # 计算总库存价值
    value_cols = [f"{band['name']}_Value" for band in AGE_BANDS if f"{band['name']}_Value" in result.columns]
    if value_cols:
        result['Total_Value'] = result[value_cols].sum(axis=1)
    else:
        result['Total_Value'] = 0
    
    return result

# ========== 8. ABC分类函数 ==========
def abc_classification(df, value_col, group_col=None):
    """
    ABC分类函数
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
                    sorted_df['cum_pct'] = sorted_df['value_pct'].cumsum()
                    
                    conditions = [
                        sorted_df['cum_pct'] <= 0.8,
                        sorted_df['cum_pct'] <= 0.95
                    ]
                    choices = ['A', 'B']
                    sorted_df['abc_class'] = np.select(conditions, choices, default='C')
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
            sorted_df['cum_pct'] = sorted_df['value_pct'].cumsum()
            
            conditions = [
                sorted_df['cum_pct'] <= 0.8,
                sorted_df['cum_pct'] <= 0.95
            ]
            choices = ['A', 'B']
            sorted_df['abc_class'] = np.select(conditions, choices, default='C')
        else:
            sorted_df['value_pct'] = 0
            sorted_df['cum_pct'] = 0
            sorted_df['abc_class'] = 'C'
        
        return sorted_df

# ========== 9. 生成报表1：库龄汇总 ==========
def generate_age_summary(df, country):
    """
    生成库龄汇总报表
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
                '库龄区间': band_name,
                '库存数量': total_qty,
                '库存价值': total_value
            })
    
    if not age_summary:
        return pd.DataFrame()
    
    summary_df = pd.DataFrame(age_summary)
    total_value = summary_df['库存价值'].sum()
    summary_df['价值占比'] = (summary_df['库存价值'] / total_value * 100).round(2)
    
    return summary_df

# ========== 10. 生成报表2：品牌ABC分类 ==========
def generate_brand_abc(df, country):
    """
    生成品牌ABC分类报表
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
        'SKU': 'SKU数量',
        'Total_Inventory': '总库存数量'
    }).reset_index()
    
    brand_summary = brand_summary[brand_summary['Total_Value'] > 0]
    
    if len(brand_summary) == 0:
        return pd.DataFrame()
    
    brand_abc = abc_classification(brand_summary, 'Total_Value')
    
    brand_abc = brand_abc.rename(columns={
        'Brand': '品牌',
        'Total_Value': '库存价值',
        'value_pct': '价值占比',
        'cum_pct': '累计占比',
        'abc_class': '品牌分类'
    })
    
    return brand_abc.sort_values('价值占比', ascending=False)

# ========== 11. 生成报表3：SKU ABC分类 ==========
def generate_sku_abc(df, country):
    """
    生成SKU ABC分类报表
    """
    if 'Country' not in df.columns:
        return pd.DataFrame()
    
    country_df = df[df['Country'] == country].copy()
    
    if len(country_df) == 0:
        return pd.DataFrame()
    
    # 准备SKU级数据
    sku_cols = ['Brand', 'SKU', 'Product_Name', 'Total_Value', 'Total_Inventory']
    available_cols = [col for col in sku_cols if col in country_df.columns]
    
    if not available_cols:
        return pd.DataFrame()
    
    sku_data = country_df[available_cols].copy()
    sku_data = sku_data[sku_data['Total_Value'] > 0]
    
    if len(sku_data) == 0:
        return pd.DataFrame()
    
    # 获取品牌分类
    brand_abc = generate_brand_abc(df, country)
    if len(brand_abc) > 0 and '品牌' in brand_abc.columns:
        brand_class_map = dict(zip(brand_abc['品牌'], brand_abc['品牌分类']))
        sku_data['品牌分类'] = sku_data['Brand'].map(brand_class_map)
    else:
        sku_data['品牌分类'] = '未分类'
    
    # SKU级ABC分类
    sku_abc = abc_classification(sku_data, 'Total_Value', group_col='Brand')
    
    sku_abc = sku_abc.rename(columns={
        'Brand': '品牌',
        'SKU': 'SKU',
        'Product_Name': '品名',
        'Total_Value': '库存价值',
        'Total_Inventory': '库存数量',
        'value_pct': '价值占比',
        'cum_pct': '累计占比',
        'abc_class': 'SKU分类'
    })
    
    return sku_abc.sort_values('库存价值', ascending=False)

# ========== 12. 保存到Google Sheets ==========
def save_to_gsheet(data_df, country, analysis_type):
    """
    将数据保存到对应国家的Google Sheets历史表
    """
    try:
        client = connect_to_gsheet()
        if client is None:
            return False
        
        data_df = data_df.copy()
        data_df['分析日期'] = datetime.now().strftime('%Y-%m-%d')
        data_df['时间戳'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        sheet_id_key = f"{country}_history_sheet_id"
        sheet_id = st.secrets["sheets"].get(sheet_id_key)
        
        if not sheet_id:
            st.error(f"未配置{country}的历史数据表")
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
        
        st.success(f"✅ {country} {analysis_type} 数据已保存")
        return True
        
    except Exception as e:
        st.error(f"保存失败: {str(e)}")
        return False

# ========== 13. 主程序 ==========
def main():
    st.sidebar.header("⚙️ 系统信息")
    
    with st.sidebar:
        st.markdown("""
        ### 📋 数据流程
        1. **加载静态映射表** (Google Sheets)
        2. **上传库存数据**
        3. **JOIN操作** (基于仓库代码)
        4. **使用 Country Code 划分国家**
        5. **生成分析报表**
        """)
        
        debug_mode = st.checkbox("🔧 调试模式", value=True)
        
        if st.button("🔄 测试Google Sheets连接"):
            client = connect_to_gsheet()
            if client:
                st.success("✅ 连接成功")
            else:
                st.error("❌ 连接失败")
    
    st.subheader("📤 上传库存数据文件")
    inventory_file = st.file_uploader(
        "请上传Excel格式的库存报表",
        type=['xlsx', 'xls']
    )
    
    if inventory_file:
        try:
            # 读取库存数据
            df = pd.read_excel(inventory_file)
            
            with st.expander("查看原始数据预览"):
                st.dataframe(df.head())
                st.write(f"总行数: {len(df)}")
                st.write(f"原始列名: {list(df.columns)}")
            
            # 步骤1：加载仓库映射表
            st.subheader("🗺️ 步骤1：加载仓库映射表")
            mapping_df = load_warehouse_region_mapping()
            
            if mapping_df is None:
                st.error("无法加载仓库映射表")
                st.stop()
            
            # 步骤2：JOIN操作
            st.subheader("🔗 步骤2：JOIN操作")
            df_with_region = join_with_warehouse_region(df, mapping_df)
            
            # 调试信息
            if debug_mode:
                st.subheader("🔍 调试信息")
                st.write("**JOIN后的列名:**", list(df_with_region.columns))
                
                if 'Country' in df_with_region.columns:
                    st.write("**国家代码分布:**")
                    country_counts = df_with_region['Country'].value_counts()
                    st.write(country_counts)
            
            # 步骤3：数据预处理
            st.subheader("🔄 步骤3：数据预处理")
            df_processed = preprocess_data(df_with_region)
            
            # 步骤4：计算库龄价值
            st.subheader("💰 步骤4：计算库存价值")
            df_with_values = calculate_age_band_values(df_processed)
            
            # 步骤5：按国家分析
            st.subheader("📊 步骤5：生成分析报表")
            
            if 'Country' not in df_with_values.columns:
                st.error("无法获取国家信息")
                st.stop()
            
            countries = df_with_values['Country'].dropna().unique()
            
            if len(countries) == 0:
                st.error("没有有效的国家数据")
                st.stop()
            
            st.success(f"发现 {len(countries)} 个国家: {', '.join(countries)}")
            
            # 为国家创建标签页
            tabs = st.tags = st.tabs([f"🇺🇸 {c}" if c == 'US' else f"🇨🇦 {c}" if c == 'CA' else f"🌍 {c}" for c in countries])
            
            for tab, country in zip(tabs, countries):
                with tab:
                    st.markdown(f"### {country} 库存分析")
                    
                    # 报表1：库龄汇总
                    st.markdown("#### 报表1：库龄汇总")
                    age_summary = generate_age_summary(df_with_values, country)
                    
                    if not age_summary.empty:
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.dataframe(
                                age_summary.style.format({
                                    '库存数量': '{:,.0f}',
                                    '库存价值': '${:,.2f}',
                                    '价值占比': '{:.1f}%'
                                }),
                                use_container_width=True
                            )
                        with col2:
                            if st.button(f"💾 保存库龄汇总", key=f"save_age_{country}"):
                                save_to_gsheet(age_summary, country, 'age_summary')
                    
                    # 报表2：品牌ABC
                    st.markdown("#### 报表2：品牌ABC分类")
                    brand_abc = generate_brand_abc(df_with_values, country)
                    
                    if not brand_abc.empty:
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.dataframe(
                                brand_abc.style.format({
                                    '库存价值': '${:,.2f}',
                                    'SKU数量': '{:,.0f}',
                                    '总库存数量': '{:,.0f}',
                                    '价值占比': '{:.2%}',
                                    '累计占比': '{:.2%}'
                                }),
                                use_container_width=True
                            )
                        with col2:
                            if st.button(f"💾 保存品牌ABC", key=f"save_brand_{country}"):
                                save_to_gsheet(brand_abc, country, 'brand_abc')
                    
                    # 报表3：SKU ABC
                    st.markdown("#### 报表3：SKU ABC分类")
                    sku_abc = generate_sku_abc(df_with_values, country)
                    
                    if not sku_abc.empty:
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            display_cols = ['品牌分类', '品牌', 'SKU', '品名', '库存数量', '库存价值', '价值占比', '累计占比', 'SKU分类']
                            available_cols = [col for col in display_cols if col in sku_abc.columns]
                            
                            st.dataframe(
                                sku_abc[available_cols].head(100).style.format({
                                    '库存数量': '{:,.0f}',
                                    '库存价值': '${:,.2f}',
                                    '价值占比': '{:.2%}',
                                    '累计占比': '{:.2%}'
                                }),
                                use_container_width=True
                            )
                            st.caption(f"显示前100条，共{len(sku_abc)}条")
                        with col2:
                            if st.button(f"💾 保存SKU ABC", key=f"save_sku_{country}"):
                                save_to_gsheet(sku_abc.head(1000), country, 'sku_abc')
            
        except Exception as e:
            st.error(f"处理数据时出错: {str(e)}")
            st.exception(e)

if __name__ == "__main__":
    main()