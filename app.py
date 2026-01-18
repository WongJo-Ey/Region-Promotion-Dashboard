import streamlit as st
import pandas as pd
import os
import shutil
import glob
import datetime
import altair as alt
import numpy as np
import duckdb
import gc
import warnings
import json
import hashlib
import pickle
import re
from io import BytesIO, StringIO
from datetime import datetime, timedelta
warnings.filterwarnings('ignore')

st.set_page_config(page_title="Region Promo Evidence Dashboard", layout="wide")
st.title("📊 Region-Level Promo Evidence Dashboard")
st.write("Upload your PRE-CLEANED region-level sales files for fast analysis.")

# -------------------------------
# Configuration
# -------------------------------
TEMP_FILE = "region_cleaned_data_temp.csv"
FORECAST_FILE = "forecast_data_temp.csv"
MAX_BACKUPS = 2

# -------------------------------
# FIXED DATE PARSING FUNCTIONS
# -------------------------------
def parse_sales_date(date_val):
    """Parse sales dates that are in dd/mm/yyyy format"""
    if pd.isna(date_val):
        return pd.NaT
    
    # Convert to string and clean
    date_str = str(date_val).strip()
    
    # Remove any time component if present
    if ' ' in date_str:
        date_str = date_str.split(' ')[0]
    
    # Try dd/mm/yyyy format first - PRIMARY FORMAT
    try:
        return pd.to_datetime(date_str, format='%d/%m/%Y', errors='raise')
    except:
        # If that fails, try other common formats
        other_formats = [
            '%d-%m-%Y',   # 01-10-2025
            '%d.%m.%Y',   # 01.10.2025
            '%Y-%m-%d',   # 2025-10-01 (ISO)
            '%m/%d/%Y',   # 10/01/2025 (US format - fallback)
            '%Y/%m/%d',   # 2025/10/01
        ]
        
        for fmt in other_formats:
            try:
                return pd.to_datetime(date_str, format=fmt, errors='raise')
            except:
                continue
        
        # Last resort: let pandas infer
        try:
            return pd.to_datetime(date_str, errors='coerce')
        except:
            return pd.NaT

def parse_forecast_date(date_val):
    """Parse forecast dates (same logic as sales dates)"""
    return parse_sales_date(date_val)

# -------------------------------
# Helper Function for Reading Excel with Row 2 Header - FIXED
# -------------------------------
def _read_excel_with_header_row2(file_path_or_object):
    """
    Read Excel file with header at row 2 (third row, 0-indexed)
    Your files have headers in row 2
    """
    try:
        # Read with header at row 2 (third row, 0-indexed)
        df = pd.read_excel(file_path_or_object, header=0)
        
        # Drop completely empty rows
        df = df.dropna(how='all')
        
        # Clean column names - strip whitespace
        df.columns = [str(col).strip() for col in df.columns]
        
        return df, ["✅ Read Excel with header at row 1", f"Shape: {df.shape}", f"Columns: {list(df.columns)}"]
        
    except Exception as e:
        return pd.DataFrame(), [f"Error reading Excel: {str(e)}"]

# -------------------------------
# Static Analysis Cache System
# -------------------------------
class SmartAnalysisCache:
    """Smart caching system for pre-computed analysis"""
    
    def __init__(self, cache_dir=".analysis_cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.cache_metadata_file = os.path.join(cache_dir, "cache_metadata.json")
        self.load_metadata()
    
    def load_metadata(self):
        """Load cache metadata"""
        if os.path.exists(self.cache_metadata_file):
            with open(self.cache_metadata_file, 'r') as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {"data_hashes": {}, "cache_stats": {}, "baseline_hashes": {}}
    
    def save_metadata(self):
        """Save cache metadata"""
        with open(self.cache_metadata_file, 'w') as f:
            json.dump(self.metadata, f)
    
    def get_data_signature(self, df):
        """Create unique signature for dataset"""
        if df.empty:
            return "empty"
        
        # Use key metrics for signature
        signature_parts = [
            str(df.shape),
            str(df['Article'].nunique() if 'Article' in df.columns else 0),
            str(df['Sales Date'].min() if 'Sales Date' in df.columns else ""),
            str(df['Sales Date'].max() if 'Sales Date' in df.columns else ""),
            str(len(df))
        ]
        
        signature = "_".join(signature_parts)
        data_hash = hashlib.md5(signature.encode()).hexdigest()[:12]
        
        # Store data info in metadata
        if data_hash not in self.metadata["data_hashes"]:
            self.metadata["data_hashes"][data_hash] = {
                "rows": len(df),
                "articles": df['Article'].nunique() if 'Article' in df.columns else 0,
                "date_range": f"{df['Sales Date'].min()} to {df['Sales Date'].max()}" if 'Sales Date' in df.columns else "",
                "last_used": datetime.now().isoformat()
            }
            self.save_metadata()
        
        return data_hash
    
    def get_filter_signature(self, filters_dict, exclude_min_days=False):
        """Create signature for filter combination"""
        # Sort filters for consistent hashing
        sorted_filters = dict(sorted(filters_dict.items()))
        
        # Remove min_days if requested (for baseline cache)
        if exclude_min_days and 'min_days' in sorted_filters:
            sorted_filters = {k: v for k, v in sorted_filters.items() if k != 'min_days'}
        
        filter_str = json.dumps(sorted_filters, sort_keys=True)
        return hashlib.md5(filter_str.encode()).hexdigest()[:12]
    
    def get_cache_key(self, data_hash, filter_hash, analysis_type):
        """Get cache file path"""
        return os.path.join(cache.cache_dir, f"cache_{data_hash}_{filter_hash}_{analysis_type}.pkl")
    
    def cache_exists(self, data_hash, filter_hash, analysis_type):
        """Check if cache exists and is valid"""
        cache_file = self.get_cache_key(data_hash, filter_hash, analysis_type)
        
        if not os.path.exists(cache_file):
            return False
        
        # Check if cache is too old (older than 7 days)
        cache_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_file))
        if cache_age.days > 7:
            os.remove(cache_file)  # Remove old cache
            return False
        
        return True
    
    def load_from_cache(self, data_hash, filter_hash, analysis_type):
        """Load analysis from cache"""
        cache_file = self.get_cache_key(data_hash, filter_hash, analysis_type)
        
        try:
            with open(cache_file, 'rb') as f:
                result = pickle.load(f)
            
            # Update cache stats
            cache_key = f"{data_hash}_{filter_hash}_{analysis_type}"
            if cache_key not in self.metadata["cache_stats"]:
                self.metadata["cache_stats"][cache_key] = {"hits": 0, "last_used": ""}
            
            self.metadata["cache_stats"][cache_key]["hits"] += 1
            self.metadata["cache_stats"][cache_key]["last_used"] = datetime.now().isoformat()
            self.save_metadata()
            
            return result
        except Exception as e:
            st.warning(f"Cache load error: {str(e)}")
            return None
    
    def save_to_cache(self, data_hash, filter_hash, analysis_type, results):
        """Save analysis to cache"""
        cache_file = self.get_cache_key(data_hash, filter_hash, analysis_type)
        
        # Add metadata to results
        results_with_meta = {
            **results,
            "cached_at": datetime.now().isoformat(),
            "data_signature": data_hash,
            "filter_signature": filter_hash
        }
        
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(results_with_meta, f)
            
            # Update metadata
            cache_key = f"{data_hash}_{filter_hash}_{analysis_type}"
            self.metadata["cache_stats"][cache_key] = {
                "hits": 0,
                "created": datetime.now().isoformat(),
                "size_kb": os.path.getsize(cache_file) / 1024
            }
            self.save_metadata()
            
            return True
        except Exception as e:
            st.error(f"Cache save error: {str(e)}")
            return False
    
    def clear_cache(self, force=False):
        """Clear all cache"""
        if force or st.checkbox("Confirm cache clearance"):
            for file in os.listdir(self.cache_dir):
                if file != "cache_metadata.json":
                    os.remove(os.path.join(self.cache_dir, file))
            
            self.metadata = {"data_hashes": {}, "cache_stats": {}, "baseline_hashes": {}}
            self.save_metadata()
            
            # Clear Streamlit cache too
            st.cache_data.clear()
            gc.collect()
            
            return True
        return False
    
    def get_cache_stats(self):
        """Get cache statistics"""
        total_files = len([f for f in os.listdir(self.cache_dir) if f.endswith('.pkl')])
        total_size = sum(os.path.getsize(os.path.join(self.cache_dir, f)) 
                        for f in os.listdir(self.cache_dir) if f.endswith('.pkl')) / 1024
        
        return {
            "total_cached_analyses": total_files,
            "total_cache_size_kb": f"{total_size:.1f}",
            "data_hashes_count": len(self.metadata.get("data_hashes", {})),
            "cache_hits": sum(stats.get("hits", 0) for stats in self.metadata.get("cache_stats", {}).values())
        }
    
    def store_baseline_hash(self, data_hash, filter_hash, baseline_hash):
        """Store baseline hash for specific data and filters"""
        key = f"{data_hash}_{filter_hash}"
        self.metadata["baseline_hashes"][key] = baseline_hash
        self.save_metadata()
    
    def get_baseline_hash(self, data_hash, filter_hash):
        """Get baseline hash for specific data and filters"""
        key = f"{data_hash}_{filter_hash}"
        return self.metadata.get("baseline_hashes", {}).get(key)

# -------------------------------
# SIMPLIFIED Forecast Data Handler - FIXED DATE PARSING
# -------------------------------
class ForecastHandler:
    """Simple forecast data handler with proper date parsing"""
    
    def __init__(self):
        self.forecast_df = None
        self.forecast_loaded = False
    
    def load_forecast_data(self, uploaded_files):
        """Load forecast data from uploaded files"""
        if not uploaded_files:
            return False
        
        dfs = []
        for file in uploaded_files:
            try:
                if file.name.endswith(".csv"):
                    df = pd.read_csv(file, low_memory=False)
                    st.sidebar.info(f"Loaded CSV file: {file.name} with {len(df)} rows")
                else:
                    # Use the new method for Excel files
                    df, debug_info = _read_excel_with_header_row2(file)
                    # Show debugging info in sidebar
                    with st.sidebar.expander(f"Debug info for {file.name}", expanded=False):
                        for line in debug_info:
                            st.write(line)
                
                # Clean and process data
                df = self._clean_forecast_data_simple(df)
                
                if df is not None and not df.empty:
                    dfs.append(df)
                
            except Exception as e:
                st.sidebar.error(f"Error loading forecast file {file.name}: {str(e)}")
                continue
        
        if dfs:
            self.forecast_df = pd.concat(dfs, ignore_index=True)
            self.forecast_df = self.forecast_df.drop_duplicates()
            self.forecast_loaded = True
            return True
        
        return False
    
    def _clean_forecast_data_simple(self, df):
        """Simple cleaning for forecast data with proper date parsing for dd/mm/yyyy"""
        if df.empty:
            return None
        
        df_clean = df.copy()
        
        # Clean column names - remove all spaces and special characters
        df_clean.columns = [str(col).strip().replace('.', '').replace('  ', ' ') for col in df_clean.columns]
        
        # Show original column names for debugging
        original_cols = list(df_clean.columns)
        st.sidebar.info(f"Original columns after cleaning: {original_cols}")
        
        # Create column mapping
        column_mapping = {}
        
        # Define search patterns for key fields - REMOVED article field
        key_fields = {
            'description': ['description', 'desc', 'item description'],
            'start_date': ['date', 'startdate', 'promodate', 'start date'],
            'end_date': ['end', 'enddate', 'todate', 'end date'],
            'forecast_qty': ['ttlforecastqty', 'forecastqty', 'totalforecast', 'fcstqty', 'revisedttlfcstqty', 'ttl forecast qty']
        }
        
        # Already found target columns
        found_targets = set()
        
        # Find matching columns - more flexible matching
        for col in df_clean.columns:
            col_clean = str(col).lower().replace(' ', '').replace('_', '').replace('-', '').replace('.', '')
            
            for field_type, patterns in key_fields.items():
                target_name = self._get_target_column_name(field_type)
                
                # Skip if already found this type
                if target_name in found_targets:
                    continue
                    
                for pattern in patterns:
                    pattern_clean = pattern.replace(' ', '')
                    if pattern_clean in col_clean:
                        column_mapping[col] = target_name
                        found_targets.add(target_name)
                        st.sidebar.info(f"✓ Matched '{col}' -> '{target_name}'")
                        break
                if col in column_mapping:
                    break
        
        # Apply column mapping
        if column_mapping:
            df_clean = df_clean.rename(columns=column_mapping)
            st.sidebar.info(f"Applied mappings: {column_mapping}")
        
        # Check for required columns
        required_cols = ['Description', 'Start_Date', 'Forecast_Qty']
        missing_cols = [col for col in required_cols if col not in df_clean.columns]
        
        if missing_cols:
            st.sidebar.error(f"Missing required forecast columns: {missing_cols}")
            st.sidebar.error(f"Available columns: {list(df_clean.columns)}")
            return None
        
        # Process dates using our FIXED date parser
        df_clean['Start_Date'] = df_clean['Start_Date'].apply(parse_forecast_date)
        
        # Process End_Date if exists
        if 'End_Date' in df_clean.columns:
            df_clean['End_Date'] = df_clean['End_Date'].apply(parse_forecast_date)
        else:
            df_clean['End_Date'] = df_clean['Start_Date']
        
        # REMOVED: Article extraction - not needed
        
        # Calculate duration - handle NaT values
        df_clean['Duration_Days'] = 1  # Default
        valid_dates = df_clean['Start_Date'].notna() & df_clean['End_Date'].notna()
        if valid_dates.any():
            df_clean.loc[valid_dates, 'Duration_Days'] = (df_clean.loc[valid_dates, 'End_Date'] - df_clean.loc[valid_dates, 'Start_Date']).dt.days + 1
        
        # Clean forecast quantity
        df_clean['Forecast_Qty'] = pd.to_numeric(df_clean['Forecast_Qty'], errors='coerce')
        
        # Remove rows with missing essential data - FIXED: removed Article_Extracted requirement
        df_clean = df_clean.dropna(subset=['Start_Date', 'Forecast_Qty'])
        
        # Show date parsing results
        if not df_clean.empty:
            st.sidebar.success(f"✓ Cleaned forecast data: {len(df_clean)} rows")
            if df_clean['Start_Date'].notna().any():
                min_date = df_clean['Start_Date'].min()
                max_date = df_clean['Start_Date'].max()
                if pd.notna(min_date) and pd.notna(max_date):
                    st.sidebar.info(f"✓ Date range: {min_date.date()} to {max_date.date()}")
                else:
                    st.sidebar.info("✓ Forecast data loaded but date range unavailable")
            else:
                st.sidebar.warning("No valid dates found in forecast data")
        
        return df_clean
    
    def _get_target_column_name(self, field_type):
        """Helper to get target column name"""
        mapping = {
            'description': 'Description',
            'start_date': 'Start_Date',
            'end_date': 'End_Date',
            'forecast_qty': 'Forecast_Qty'
        }
        return mapping.get(field_type, field_type)
    
    def match_forecast_with_promotions_simple(self, promo_df):
        """Simple forecast matching - only run if forecast is loaded"""
        if not self.forecast_loaded or self.forecast_df is None or self.forecast_df.empty:
            # Return promo_df with empty forecast columns
            promo_df['Has_Forecast'] = False
            promo_df['Forecast_Qty'] = None
            promo_df['Forecast_Daily_Qty'] = None
            return promo_df
        
        # Make a copy to avoid modifying original
        promo_df_with_forecast = promo_df.copy()
        
        # Initialize forecast columns
        promo_df_with_forecast['Has_Forecast'] = False
        promo_df_with_forecast['Forecast_Qty'] = None
        promo_df_with_forecast['Forecast_Daily_Qty'] = None
        
        # Only match for promo records
        promo_mask = promo_df_with_forecast['Promotion Status'] != 'NON-PROMO'
        
        if promo_mask.sum() == 0:
            return promo_df_with_forecast
        
        # Get unique promo articles
        promo_articles = promo_df_with_forecast.loc[promo_mask, 'Article'].unique()
        
        match_count = 0
        for article in promo_articles:
            article_str = str(article)
            
            # Find matching forecasts for this article - SIMPLIFIED: only use Description field
            article_forecasts = self.forecast_df[
                self.forecast_df['Description'].str.contains(article_str, na=False)
            ]
            
            if not article_forecasts.empty:
                # Take the first matching forecast
                forecast = article_forecasts.iloc[0]
                
                # Apply to all promo records for this article
                article_promo_mask = (promo_df_with_forecast['Article'] == article) & promo_mask
                promo_df_with_forecast.loc[article_promo_mask, 'Has_Forecast'] = True
                promo_df_with_forecast.loc[article_promo_mask, 'Forecast_Qty'] = forecast['Forecast_Qty']
                promo_df_with_forecast.loc[article_promo_mask, 'Forecast_Daily_Qty'] = forecast['Forecast_Qty'] / forecast['Duration_Days']
                match_count += 1
        
        if match_count > 0:
            st.sidebar.success(f"✅ Matched {match_count} articles with forecast data")
        
        return promo_df_with_forecast
    
    def get_forecast_stats(self):
        """Get forecast data statistics - FIXED date handling"""
        if not self.forecast_loaded or self.forecast_df is None or self.forecast_df.empty:
            return None
        
        try:
            stats = {
                'total_forecasts': len(self.forecast_df),
                'unique_descriptions': self.forecast_df['Description'].nunique()
            }
            
            # Safely get date range
            if not self.forecast_df['Start_Date'].empty:
                min_date = self.forecast_df['Start_Date'].min()
                max_date = self.forecast_df['Start_Date'].max()
                
                if pd.notna(min_date) and pd.notna(max_date):
                    # Check if dates are datetime objects
                    if hasattr(min_date, 'date'):
                        stats['date_range'] = f"{min_date.date()} to {max_date.date()}"
                    else:
                        # Try to convert to string
                        stats['date_range'] = f"{str(min_date)[:10]} to {str(max_date)[:10]}"
                else:
                    stats['date_range'] = "Date range not available"
            else:
                stats['date_range'] = "No dates available"
            
            # Calculate forecast quantities
            if not self.forecast_df['Forecast_Qty'].empty:
                stats['avg_forecast_qty'] = float(self.forecast_df['Forecast_Qty'].mean())
                stats['total_forecast_qty'] = float(self.forecast_df['Forecast_Qty'].sum())
            else:
                stats['avg_forecast_qty'] = 0
                stats['total_forecast_qty'] = 0
            
            return stats
            
        except Exception as e:
            st.sidebar.error(f"Error getting forecast stats: {str(e)}")
            # Return basic stats without dates
            return {
                'total_forecasts': len(self.forecast_df),
                'unique_descriptions': self.forecast_df['Description'].nunique(),
                'date_range': 'Error parsing dates',
                'avg_forecast_qty': 0,
                'total_forecast_qty': 0
            }
    
    def clear_forecast_data(self):
        """Clear forecast data"""
        self.forecast_df = None
        self.forecast_loaded = False
        if os.path.exists(FORECAST_FILE):
            os.remove(FORECAST_FILE)

# -------------------------------
# DuckDB Analysis Engine (for main data only)
# -------------------------------
class FastAnalysisEngine:
    """Fast in-memory analysis engine using DuckDB"""
    
    def __init__(self):
        self.conn = None
        self.is_initialized = False
        
    def initialize(self, df):
        """Initialize engine with data"""
        if self.is_initialized:
            return self.conn
            
        with st.spinner("⚡ Setting up analysis engine..."):
            # Create in-memory database
            self.conn = duckdb.connect(':memory:')
            
            # Register data
            self.conn.register('raw_data', df)
            
            # Create optimized views
            self._create_optimized_views()
            
            self.is_initialized = True
        
        return self.conn
    
    def _create_optimized_views(self):
        """Create pre-aggregated views for fast queries"""
        # Article metrics for fast filtering
        self.conn.execute("""
            CREATE OR REPLACE VIEW article_metrics AS
            SELECT 
                Article,
                COUNT(*) as total_rows,
                COUNT(DISTINCT "Sales Date") as unique_days,
                SUM(CASE WHEN "Promotion Status" != 'NON-PROMO' THEN 1 ELSE 0 END) as promo_count,
                SUM(CASE WHEN "Promotion Status" = 'NON-PROMO' THEN 1 ELSE 0 END) as nonpromo_count,
                MIN("Sales Date") as first_date,
                MAX("Sales Date") as last_date
            FROM raw_data
            GROUP BY Article
        """)
        
        # Promo data summary
        self.conn.execute("""
            CREATE OR REPLACE VIEW promo_data AS
            SELECT 
                Article,
                "Bonus Buy",
                Region,
                "Sales Date",
                "Net Sales",
                "Sales Quantity",
                "Promotion Status",
                "Sub Category"
            FROM raw_data
            WHERE "Promotion Status" != 'NON-PROMO'
        """)
        
        # Non-promo data summary
        self.conn.execute("""
            CREATE OR REPLACE VIEW nonpromo_data AS
            SELECT 
                Article,
                Region,
                "Sales Date",
                "Net Sales"
            FROM raw_data
            WHERE "Promotion Status" = 'NON-PROMO'
        """)
    
    def get_valid_articles_fast(self, min_promo=2, min_nonpromo=2, limit=500):
        """Ultra-fast query for valid articles"""
        query = f"""
            SELECT Article 
            FROM article_metrics 
            WHERE promo_count >= {min_promo} 
              AND nonpromo_count >= {min_nonpromo}
            ORDER BY total_rows DESC
            LIMIT {limit}
        """
        result = self.conn.execute(query).df()
        return result['Article'].tolist() if not result.empty else []
    
    def get_article_data(self, article):
        """Get all data for specific article"""
        query = """
            SELECT * 
            FROM raw_data 
            WHERE Article = ?
        """
        result = self.conn.execute(query, [article]).df()
        return result
    
    def cleanup(self):
        """Manual cleanup"""
        if self.conn:
            self.conn.close()
        self.conn = None
        self.is_initialized = False
        gc.collect()

# -------------------------------
# Memory-safe caching decorator
# -------------------------------
def safe_cache_data(func=None, *, ttl=3600, max_entries=2, show_spinner=True):
    """Memory-safe caching decorator"""
    if func is None:
        return lambda f: safe_cache_data(f, ttl=ttl, max_entries=max_entries, show_spinner=show_spinner)
    
    cached_func = st.cache_data(ttl=ttl, max_entries=max_entries, show_spinner=show_spinner)(func)
    
    def wrapper(*args, **kwargs):
        result = cached_func(*args, **kwargs)
        gc.collect()
        return result
    
    return wrapper

# -------------------------------
# Helper Functions
# -------------------------------
def get_uplift_interpretation(uplift_pct):
    """Interpret uplift percentage with business context"""
    if pd.isna(uplift_pct):
        return "📊 **Insufficient Data**"
    elif uplift_pct > 0.3:
        return "🚀 **Strong Success**"
    elif uplift_pct > 0.1:
        return "✅ **Positive Impact**"
    elif uplift_pct > -0.05:
        return "⚖️ **Neutral/Breakeven**"
    elif uplift_pct > -0.2:
        return "⚠️ **Needs Adjustment**"
    else:
        return "❌ **Significant Loss**"

def get_confidence_level(promo_days, baseline_value):
    """Determine confidence level based on data quality"""
    if promo_days >= 7 and baseline_value >= 20:
        return "High"
    elif promo_days >= 3 and baseline_value >= 10:
        return "Medium"
    else:
        return "Low"

def aggregate_promotions_with_forecast(df, include_region_count=False):
    """Aggregate promotions by Article and Bonus Buy with forecast data"""
    agg_dict = {
        'PromoSales': 'sum',
        'PromoQuantity': 'sum',
        'BaselineSales': 'sum',
        'Duration': 'mean',
    }
    
    # Add forecast columns if they exist
    if 'Forecast_Qty' in df.columns:
        agg_dict['Forecast_Qty'] = 'max'
    
    if include_region_count:
        agg_dict['Region'] = 'nunique'
    
    df_agg = df.groupby(['Article', 'Bonus Buy']).agg(agg_dict).reset_index()
    
    # Recalculate uplift for aggregated data
    df_agg['Uplift_Pct'] = (df_agg['PromoSales'] - df_agg['BaselineSales']) / df_agg['BaselineSales']
    df_agg['Uplift_Display'] = df_agg['Uplift_Pct'].apply(lambda x: f"{x:+.1%}")
    
    # Add forecast comparison if available
    if 'Forecast_Qty' in df_agg.columns:
        # FIXED: Avoid division by zero
        df_agg['Forecast_Accuracy'] = df_agg.apply(
            lambda row: (row['PromoQuantity'] - row['Forecast_Qty']) / row['Forecast_Qty'] 
            if pd.notna(row['Forecast_Qty']) and row['Forecast_Qty'] > 0 else None, 
            axis=1
        )
        df_agg['Forecast_Accuracy_Display'] = df_agg['Forecast_Accuracy'].apply(lambda x: f"{x:+.1%}" if pd.notna(x) else "N/A")
    
    return df_agg

def get_forecast_interpretation(accuracy_pct):
    """Interpret forecast accuracy"""
    if pd.isna(accuracy_pct):
        return "📊 **No Forecast**"
    elif abs(accuracy_pct) <= 0.1:
        return "🎯 **Excellent Forecast**"
    elif abs(accuracy_pct) <= 0.25:
        return "✅ **Good Forecast**"
    elif abs(accuracy_pct) <= 0.5:
        return "⚠️ **Fair Forecast**"
    elif accuracy_pct > 0.5:
        return "📈 **Over-Forecast**"
    else:
        return "📉 **Under-Forecast**"

# -------------------------------
# Cached analysis functions - SIMPLIFIED
# -------------------------------
@safe_cache_data(ttl=3600, max_entries=2)
def calculate_baselines(_filtered_df):
    """Calculate article baselines - CACHED for performance"""
    # Get articles with promotions
    promo_articles = _filtered_df[
        _filtered_df['Promotion Status'] != 'NON-PROMO'
    ]['Article'].unique()
    
    article_baselines = []
    article_clean_days = []
    
    for article in promo_articles:
        article_data = _filtered_df[_filtered_df['Article'] == article]
        
        # Get article's promo dates
        article_promo_dates = article_data[
            article_data['Promotion Status'] != 'NON-PROMO'
        ]['Sales Date'].unique()
        
        # Get clean non-promo days
        clean_nonpromo = article_data[
            (article_data['Promotion Status'] == 'NON-PROMO') & 
            (~article_data['Sales Date'].isin(article_promo_dates))
        ]
        
        clean_days = clean_nonpromo['Sales Date'].nunique()
        article_clean_days.append(clean_days)
        
        if clean_days >= 3:
            daily_avg = clean_nonpromo['Net Sales'].sum() / clean_days
            
            baseline_info = {
                'Article': article,
                'Daily_Avg': daily_avg,
                'Clean_Days': clean_days
            }
            
            article_baselines.append(baseline_info)
    
    if article_baselines:
        baselines_df = pd.DataFrame(article_baselines)
        return baselines_df, sum(article_clean_days), len([d for d in article_clean_days if d >= 3])
    
    return None, 0, 0

@safe_cache_data(ttl=3600, max_entries=2)
def analyze_promotions(_filtered_df, _baselines_df, _min_promo_days, _reliable_region_list):
    """Analyze promotions - SIMPLIFIED version"""
    
    # Get promo data
    df_promo = _filtered_df[_filtered_df['Promotion Status'] != 'NON-PROMO']
    
    # Filter to reliable regions only
    df_reliable_promo = df_promo[df_promo['Region'].isin(_reliable_region_list)]
    
    if len(df_reliable_promo) == 0:
        return None
    
    # Group by relevant columns
    group_cols = ['Article', 'Bonus Buy', 'Region']
    if 'Sub Category' in df_reliable_promo.columns:
        group_cols.append('Sub Category')
    if 'PSA' in df_reliable_promo.columns:
        group_cols.append('PSA')
    
    promo_summary = df_reliable_promo.groupby(group_cols).agg({
        'Net Sales': 'sum',
        'Sales Quantity': 'sum',
        'Sales Date': 'nunique'
    }).reset_index()
    promo_summary = promo_summary.rename(columns={
        'Net Sales': 'PromoSales',
        'Sales Quantity': 'PromoQuantity',
        'Sales Date': 'PromoDays'
    })
    
    # Merge with baselines
    promo_summary = promo_summary.merge(
        _baselines_df[['Article', 'Daily_Avg']],
        on='Article',
        how='inner'
    )
    
    # Calculate uplift
    promo_summary['BaselineSales'] = promo_summary['Daily_Avg'] * promo_summary['PromoDays']
    promo_summary['Uplift_Pct'] = (promo_summary['PromoSales'] - promo_summary['BaselineSales']) / promo_summary['BaselineSales']
    
    # Add forecast data if available
    if 'Has_Forecast' in df_reliable_promo.columns:
        forecast_data = df_reliable_promo.groupby(group_cols).agg({
            'Has_Forecast': 'max',
            'Forecast_Qty': 'max',
            'Forecast_Daily_Qty': 'max'
        }).reset_index()
        
        promo_summary = promo_summary.merge(
            forecast_data[group_cols + ['Has_Forecast', 'Forecast_Qty', 'Forecast_Daily_Qty']],
            on=group_cols,
            how='left'
        )
        
        if 'Forecast_Qty' in promo_summary.columns:
            # FIXED: Avoid division by zero
            promo_summary['Forecast_Accuracy'] = promo_summary.apply(
                lambda row: (row['PromoQuantity'] - row['Forecast_Qty']) / row['Forecast_Qty'] 
                if pd.notna(row['Forecast_Qty']) and row['Forecast_Qty'] > 0 else None, 
                axis=1
            )
            promo_summary['Forecast_Interpretation'] = promo_summary['Forecast_Accuracy'].apply(get_forecast_interpretation)
    
    # Apply filters
    valid_promos = promo_summary[
        (promo_summary['PromoDays'] >= _min_promo_days) &
        (promo_summary['BaselineSales'] >= 100) &
        (promo_summary['Uplift_Pct'].notnull())
    ].copy()
    
    if len(valid_promos) > 0:
        # Add interpretation and confidence
        valid_promos['Interpretation'] = valid_promos['Uplift_Pct'].apply(get_uplift_interpretation)
        valid_promos['Confidence'] = valid_promos.apply(
            lambda row: get_confidence_level(row['PromoDays'], row['BaselineSales']/row['PromoDays']), 
            axis=1
        )
        valid_promos['Uplift_Display'] = valid_promos['Uplift_Pct'].apply(lambda x: f"{x:+.1%}")
        valid_promos['Duration'] = valid_promos['PromoDays']
        
        return valid_promos
    
    return None

# -------------------------------
# Cache helper functions
# -------------------------------
def get_cached_baselines(df, filters_dict):
    """Get baselines from cache or compute fresh"""
    cache = st.session_state.smart_cache
    
    # Generate signatures
    data_hash = cache.get_data_signature(df)
    baseline_filter_hash = cache.get_filter_signature(filters_dict, exclude_min_days=True)
    
    # Create a baseline-specific cache key
    baseline_cache_key = f"{data_hash}_{baseline_filter_hash}_baselines"
    baseline_cache_file = os.path.join(cache.cache_dir, f"cache_{baseline_cache_key}.pkl")
    
    # Check cache
    if os.path.exists(baseline_cache_file):
        try:
            with open(baseline_cache_file, 'rb') as f:
                cached_result = pickle.load(f)
            
            if cached_result:
                # Update cache stats
                if baseline_cache_key not in cache.metadata["cache_stats"]:
                    cache.metadata["cache_stats"][baseline_cache_key] = {"hits": 0, "last_used": ""}
                
                cache.metadata["cache_stats"][baseline_cache_key]["hits"] += 1
                cache.metadata["cache_stats"][baseline_cache_key]["last_used"] = datetime.now().isoformat()
                cache.save_metadata()
                
                return cached_result
        except:
            pass
    
    # Compute fresh baselines
    baselines_df, total_clean_days, valid_articles = calculate_baselines(df)
    
    if baselines_df is not None:
        result = {
            'baselines_df': baselines_df,
            'total_clean_days': total_clean_days,
            'valid_articles': valid_articles,
            'computed_at': datetime.now().isoformat(),
            'data_signature': data_hash,
            'filter_signature': baseline_filter_hash
        }
        
        # Cache the results
        try:
            with open(baseline_cache_file, 'wb') as f:
                pickle.dump(result, f)
            
            # Update metadata
            cache.metadata["cache_stats"][baseline_cache_key] = {
                "hits": 0,
                "created": datetime.now().isoformat(),
                "size_kb": os.path.getsize(baseline_cache_file) / 1024
            }
            cache.save_metadata()
        except Exception as e:
            st.error(f"Baseline cache save error: {str(e)}")
        
        return result
    
    return None

def get_cached_promo_analysis(df, filters_dict, baselines_result):
    """Get promo analysis from cache or compute fresh"""
    cache = st.session_state.smart_cache
    
    # Generate signatures
    data_hash = cache.get_data_signature(df)
    filter_hash = cache.get_filter_signature(filters_dict)
    
    # Create a promo-specific cache key
    promo_cache_key = f"{data_hash}_{filter_hash}_promo_analysis"
    promo_cache_file = os.path.join(cache.cache_dir, f"cache_{promo_cache_key}.pkl")
    
    # Check cache
    if os.path.exists(promo_cache_file):
        try:
            with open(promo_cache_file, 'rb') as f:
                cached_result = pickle.load(f)
            
            if cached_result:
                # Update cache stats
                if promo_cache_key not in cache.metadata["cache_stats"]:
                    cache.metadata["cache_stats"][promo_cache_key] = {"hits": 0, "last_used": ""}
                
                cache.metadata["cache_stats"][promo_cache_key]["hits"] += 1
                cache.metadata["cache_stats"][promo_cache_key]["last_used"] = datetime.now().isoformat()
                cache.save_metadata()
                
                return cached_result
        except:
            pass
    
    # Compute fresh promo analysis
    min_days = filters_dict.get('min_days', 3)
    
    if filters_dict.get('region') != "All":
        reliable_region_list = [filters_dict.get('region')]
    else:
        regions_with_promo = df[df['Promotion Status'] != 'NON-PROMO']['Region'].unique()
        reliable_region_list = list(regions_with_promo)
    
    valid_promos = analyze_promotions(
        df, 
        baselines_result['baselines_df'], 
        min_days,
        reliable_region_list
    )
    
    # Calculate forecast coverage
    forecast_coverage = None
    if valid_promos is not None and 'Has_Forecast' in valid_promos.columns:
        forecast_promos = valid_promos[valid_promos['Has_Forecast'] == True]
        forecast_coverage = len(forecast_promos) / len(valid_promos) if len(valid_promos) > 0 else 0
    
    result = {
        'valid_promos': valid_promos,
        'reliable_region_list': reliable_region_list,
        'forecast_coverage': forecast_coverage,
        'computed_at': datetime.now().isoformat(),
        'data_signature': data_hash,
        'filter_signature': filter_hash
    }
    
    # Cache the results
    try:
        with open(promo_cache_file, 'wb') as f:
            pickle.dump(result, f)
        
        # Update metadata
        cache.metadata["cache_stats"][promo_cache_key] = {
            "hits": 0,
            "created": datetime.now().isoformat(),
            "size_kb": os.path.getsize(promo_cache_file) / 1024
        }
        cache.save_metadata()
    except Exception as e:
        st.error(f"Promo analysis cache save error: {str(e)}")
    
    return result

# -------------------------------
# Main analysis function
# -------------------------------
def perform_cached_summary_analysis(filtered_df, filters_dict):
    """Perform summary analysis with caching"""
    
    # Step 1: Get or compute baselines
    baselines_result = get_cached_baselines(filtered_df, filters_dict)
    
    if baselines_result is None:
        return None
    
    # Step 2: Get or compute promo analysis
    promo_result = get_cached_promo_analysis(filtered_df, filters_dict, baselines_result)
    
    # Combine results
    return {
        'filtered_df': filtered_df,
        'valid_promos': promo_result.get('valid_promos'),
        'baselines_df': baselines_result.get('baselines_df'),
        'total_clean_days': baselines_result.get('total_clean_days'),
        'valid_articles': baselines_result.get('valid_articles'),
        'reliable_region_list': promo_result.get('reliable_region_list'),
        'forecast_coverage': promo_result.get('forecast_coverage'),
        'filters_applied': filters_dict,
        'computed_at': datetime.now().isoformat()
    }

# -------------------------------
# Initialize session state
# -------------------------------
if 'files_processed' not in st.session_state:
    st.session_state.files_processed = False
if 'analysis_complete' not in st.session_state:
    st.session_state.analysis_complete = False
if 'filtered_df' not in st.session_state:
    st.session_state.filtered_df = None
if 'analysis_df' not in st.session_state:
    st.session_state.analysis_df = None
if 'analysis_cache' not in st.session_state:
    st.session_state.analysis_cache = {}
if 'analysis_engine' not in st.session_state:
    st.session_state.analysis_engine = FastAnalysisEngine()
if 'smart_cache' not in st.session_state:
    st.session_state.smart_cache = SmartAnalysisCache()
if 'use_cache' not in st.session_state:
    st.session_state.use_cache = True
if 'force_refresh' not in st.session_state:
    st.session_state.force_refresh = False
if 'selected_psa' not in st.session_state:
    st.session_state.selected_psa = None
if 'forecast_handler' not in st.session_state:
    st.session_state.forecast_handler = ForecastHandler()
if 'current_min_days' not in st.session_state:
    st.session_state.current_min_days = 3
if 'last_baseline_hash' not in st.session_state:
    st.session_state.last_baseline_hash = None

# -------------------------------
# SIDEBAR: File Upload Section with CLEAR DATA OPTION
# -------------------------------
st.sidebar.subheader("📤 Upload Data")

# Clear existing data button
st.sidebar.markdown("---")
st.sidebar.subheader("🗑️ Clear Existing Data")
if st.sidebar.button("⚠️ CLEAR ALL DATA AND START FRESH", type="secondary", help="This will delete all existing data and start fresh"):
    # Clear data files
    if os.path.exists(TEMP_FILE):
        os.remove(TEMP_FILE)
    
    # Clear forecast files
    if os.path.exists(FORECAST_FILE):
        os.remove(FORECAST_FILE)
    
    # Clear all backups
    for backup in glob.glob("region_cleaned_data_temp_backup_*.csv"):
        os.remove(backup)
    
    # Clear session state
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    
    # Reinitialize essential variables
    st.session_state.files_processed = False
    st.session_state.analysis_complete = False
    st.session_state.filtered_df = None
    st.session_state.analysis_df = None
    st.session_state.analysis_cache = {}
    st.session_state.analysis_engine = FastAnalysisEngine()
    st.session_state.smart_cache = SmartAnalysisCache()
    st.session_state.forecast_handler = ForecastHandler()
    
    st.sidebar.success("✅ All data cleared! Please upload fresh files.")
    st.rerun()

# Check if temp file exists
temp_file_exists = os.path.exists(TEMP_FILE)

# Forecast file upload section
st.sidebar.markdown("---")
st.sidebar.subheader("📈 Upload Forecast Data (Optional)")

forecast_files = st.sidebar.file_uploader(
    "Upload Forecast Files (CSV/Excel)", 
    type=["csv", "xlsx"], 
    accept_multiple_files=True,
    help="Upload forecast files with columns: Description, DATE, END, TTL Forecast Qty",
    key="forecast_uploader"
)

if forecast_files:
    with st.spinner("Loading forecast data..."):
        forecast_loaded = st.session_state.forecast_handler.load_forecast_data(forecast_files)
        
        if forecast_loaded:
            # Save forecast data
            st.session_state.forecast_handler.forecast_df.to_csv(FORECAST_FILE, index=False)
            forecast_stats = st.session_state.forecast_handler.get_forecast_stats()
            if forecast_stats:
                st.sidebar.success(f"✅ Loaded {forecast_stats['total_forecasts']} forecasts")
                st.sidebar.info(f"📅 Date range: {forecast_stats['date_range']}")
            else:
                st.sidebar.warning("Forecast data loaded but could not generate statistics")
        else:
            st.sidebar.warning("No valid forecast data loaded")

# Check if forecast file exists
if os.path.exists(FORECAST_FILE) and st.session_state.forecast_handler.forecast_df is None:
    try:
        forecast_df = pd.read_csv(FORECAST_FILE)
        
        # Parse dates when loading from CSV using our fixed parser
        if 'Start_Date' in forecast_df.columns:
            forecast_df['Start_Date'] = forecast_df['Start_Date'].apply(parse_forecast_date)
        if 'End_Date' in forecast_df.columns:
            forecast_df['End_Date'] = forecast_df['End_Date'].apply(parse_forecast_date)
        
        st.session_state.forecast_handler.forecast_df = forecast_df
        st.session_state.forecast_handler.forecast_loaded = True
        st.sidebar.info("✓ Loaded previously saved forecast data")
    except Exception as e:
        st.sidebar.error(f"Error loading forecast data: {str(e)}")

# Clear forecast data button
if st.session_state.forecast_handler.forecast_loaded:
    if st.sidebar.button("🗑️ Clear Forecast Data", type="secondary", key="clear_forecast"):
        st.session_state.forecast_handler.clear_forecast_data()
        st.sidebar.success("Forecast data cleared!")
        st.rerun()

# -------------------------------
# MAIN DATA LOADING - FIXED FOR YOUR COLUMN NAMES
# -------------------------------
if temp_file_exists:
    # Show current data info
    try:
        # First, check file size
        file_size = os.path.getsize(TEMP_FILE) / (1024 * 1024 * 1024)  # Size in GB
        st.sidebar.info(f"📁 Data file size: {file_size:.2f} GB")
        
        # Read data with optimized settings for large files
        with st.spinner("Loading data file..."):
            # Read in chunks if file is large
            if file_size > 1.0:  # If file is larger than 1GB
                st.sidebar.info("Large file detected, reading in chunks...")
                chunks = []
                chunk_size = 1000000  # 1 million rows per chunk
                
                for chunk in pd.read_csv(TEMP_FILE, chunksize=chunk_size, low_memory=False):
                    # Process dates in each chunk using our FIXED parser
                    if 'Sales Date' in chunk.columns:
                        chunk['Sales Date'] = chunk['Sales Date'].apply(parse_sales_date)
                    
                    chunks.append(chunk)
                
                existing_df = pd.concat(chunks, ignore_index=True)
            else:
                # Read entire file at once
                existing_df = pd.read_csv(TEMP_FILE, low_memory=False)
                
                # Process dates using our FIXED parser
                if 'Sales Date' in existing_df.columns:
                    existing_df['Sales Date'] = existing_df['Sales Date'].apply(parse_sales_date)
            
            # Check date parsing results
            initial_rows = len(existing_df)
            if 'Sales Date' in existing_df.columns:
                # Count invalid dates BEFORE dropping
                invalid_dates = existing_df['Sales Date'].isna().sum()
                if invalid_dates > 0:
                    st.sidebar.warning(f"Found {invalid_dates} rows with invalid dates")
                    
                    # Show sample of problematic dates
                    invalid_samples = existing_df[existing_df['Sales Date'].isna()].head(3)
                    with st.sidebar.expander("Show problematic date samples", expanded=False):
                        st.write(invalid_samples[['Sales Date']].head(10))
                
                # Only drop rows with invalid dates if they exist
                if invalid_dates > 0:
                    existing_df = existing_df.dropna(subset=['Sales Date'])
                    st.sidebar.info(f"Removed {invalid_dates} rows with invalid dates. Kept {len(existing_df):,} rows.")
            
            st.sidebar.success(f"✅ Data loaded: {existing_df.shape[0]:,} rows, {existing_df.shape[1]} columns")
            
            # Show data info
            if 'Sales Date' in existing_df.columns:
                if not existing_df.empty and existing_df['Sales Date'].notna().any():
                    date_min = existing_df['Sales Date'].min()
                    date_max = existing_df['Sales Date'].max()
                    if pd.notna(date_min) and pd.notna(date_max):
                        st.sidebar.info(f"📅 Date range: {date_min.date()} to {date_max.date()}")
                    else:
                        st.sidebar.warning("Could not determine date range")
                else:
                    st.sidebar.warning("No valid dates found in data")
            
            if 'Article' in existing_df.columns:
                st.sidebar.info(f"📦 Unique articles: {existing_df['Article'].nunique():,}")
            
            if 'Region' in existing_df.columns:
                st.sidebar.info(f"📍 Unique regions: {existing_df['Region'].nunique():,}")
        
        # Store in session state
        st.session_state.existing_df = existing_df
        
        # Show uploader for additional files
        uploaded_files = st.sidebar.file_uploader(
            "Upload additional CSV/Excel files", 
            type=["csv", "xlsx"], 
            accept_multiple_files=True,
            help="Upload additional pre-cleaned files to add to existing data",
            key="additional_files"
        )
        
        if uploaded_files:
            with st.spinner("Loading additional files..."):
                dfs = []
                for file in uploaded_files:
                    try:
                        # Read with minimal processing
                        if file.name.endswith(".csv"):
                            df = pd.read_csv(file, low_memory=False)
                            st.sidebar.info(f"Loaded CSV file: {file.name} with {len(df)} rows")
                        else:
                            # Use the new method for Excel files with YOUR headers
                            df, debug_info = _read_excel_with_header_row2(file)
                            # Show debug info
                            with st.sidebar.expander(f"File info: {file.name}", expanded=False):
                                for line in debug_info:
                                    st.write(line)
                        
                        # YOUR EXACT COLUMN NAMES - NO NEED FOR MAPPING!
                        # Just clean up any whitespace in column names
                        df.columns = [str(col).strip() for col in df.columns]
                        
                        # Show what columns we found
                        st.sidebar.info(f"📋 Columns in {file.name}: {list(df.columns)}")
                        
                        # REQUIRED COLUMNS CHECK - using YOUR exact column names
                        required_columns = ['Region', 'PSA', 'Sub Category', 'Bonus Buy', 'Sales Date', 
                                           'Promotion Status', 'Article', 'Net Sales', 'Sales Quantity']
                        
                        # Check which required columns are present
                        available_columns = list(df.columns)
                        missing_columns = [col for col in required_columns if col not in available_columns]
                        
                        if missing_columns:
                            st.sidebar.error(f"❌ File '{file.name}' missing columns: {', '.join(missing_columns)}")
                            st.sidebar.info(f"Available columns: {available_columns}")
                            continue
                        
                        st.sidebar.success(f"✅ All required columns found in {file.name}")
                        
                        # MINIMAL PROCESSING
                        if 'Promotion Status' in df.columns:
                            df['Promotion Status'] = df['Promotion Status'].astype(str).str.upper()
                        
                        # Use our FIXED date parser for dd/mm/yyyy
                        if 'Sales Date' in df.columns:
                            df['Sales Date'] = df['Sales Date'].apply(parse_sales_date)
                        
                        if 'Sales Quantity' in df.columns:
                            df['Sales Quantity'] = pd.to_numeric(df['Sales Quantity'], errors='coerce').fillna(0)
                        if 'Net Sales' in df.columns:
                            df['Net Sales'] = pd.to_numeric(df['Net Sales'], errors='coerce').fillna(0)
                        
                        # Only drop rows with invalid dates if they exist
                        if 'Sales Date' in df.columns:
                            invalid_dates = df['Sales Date'].isna().sum()
                            if invalid_dates > 0:
                                df = df.dropna(subset=['Sales Date'])
                                st.sidebar.info(f"Removed {invalid_dates} invalid dates from {file.name}")
                        
                        df['source_file'] = file.name
                        dfs.append(df)
                        
                    except Exception as e:
                        st.sidebar.error(f"Error loading {file.name}: {str(e)}")
                        continue
                
                if dfs:
                    # Combine new data with existing
                    new_df = pd.concat(dfs, ignore_index=True)
                    new_df = new_df.drop_duplicates()
                    
                    # Backup previous data
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_file = f"region_cleaned_data_temp_backup_{timestamp}.csv"
                    shutil.copy(TEMP_FILE, backup_file)
                    backups = sorted(glob.glob("region_cleaned_data_temp_backup_*.csv"), reverse=True)
                    while len(backups) > MAX_BACKUPS:
                        os.remove(backups[-1])
                        backups.pop(-1)
                    
                    # Combine with existing and save
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates()
                    combined_df.to_csv(TEMP_FILE, index=False)
                    
                    st.sidebar.success(f"✅ Added {new_df.shape[0]:,} rows from {len(dfs)} files")
                    st.sidebar.info(f"📊 Total data: {combined_df.shape[0]:,} rows")
                    
                    # Clear session state to force reload
                    st.session_state.analysis_df = None
                    st.session_state.analysis_complete = False
                    st.session_state.filtered_df = None
                    st.session_state.analysis_cache = {}
                    st.session_state.analysis_engine.cleanup()
                    st.session_state.analysis_engine = FastAnalysisEngine()
                    st.session_state.selected_psa = None
                    st.session_state.existing_df = None  # Clear cached data
                    
                    st.rerun()
        
        # Clear data button
        st.sidebar.markdown("---")
        if st.sidebar.button("🗑️ Clear All Data", type="secondary", key="clear_all_data"):
            if os.path.exists(TEMP_FILE):
                os.remove(TEMP_FILE)
                for backup in glob.glob("region_cleaned_data_temp_backup_*.csv"):
                    os.remove(backup)
                # Reset session state
                st.session_state.files_processed = False
                st.session_state.analysis_complete = False
                st.session_state.filtered_df = None
                st.session_state.analysis_df = None
                st.session_state.analysis_cache = {}
                st.session_state.analysis_engine.cleanup()
                st.session_state.analysis_engine = FastAnalysisEngine()
                st.session_state.selected_psa = None
                st.session_state.forecast_handler.clear_forecast_data()
                st.session_state.existing_df = None
                st.sidebar.success("Data cleared")
                st.rerun()
        
        # Download data button
        if st.sidebar.button("📥 Download Current Data"):
            with open(TEMP_FILE, 'rb') as f:
                st.sidebar.download_button(
                    label="Download CSV",
                    data=f,
                    file_name="promo_analysis_data.csv",
                    mime="text/csv",
                    key="sidebar_download"
                )
    
    except Exception as e:
        st.sidebar.error(f"Error loading data: {str(e)}")
        st.sidebar.info("Please re-upload your data files")

else:
    # No data exists yet
    st.sidebar.info("No data loaded. Upload files to begin.")
    
    uploaded_files = st.sidebar.file_uploader(
        "Upload CSV or Excel files (PRE-CLEANED)", 
        type=["csv", "xlsx"], 
        accept_multiple_files=True,
        help="""Files must be pre-cleaned with required columns""",
        key="initial_upload"
    )
    
    if uploaded_files:
        with st.spinner("Loading files..."):
            dfs = []
            for file in uploaded_files:
                try:
                    # Read with minimal processing
                    if file.name.endswith(".csv"):
                        df = pd.read_csv(file, low_memory=False)
                        st.sidebar.info(f"Loaded CSV file: {file.name} with {len(df)} rows")
                    else:
                        # Use the new method for Excel files with YOUR headers
                        df, debug_info = _read_excel_with_header_row2(file)
                        # Show debug info
                        with st.sidebar.expander(f"File info: {file.name}", expanded=False):
                            for line in debug_info:
                                st.write(line)
                    
                    # YOUR EXACT COLUMN NAMES - NO NEED FOR MAPPING!
                    # Just clean up any whitespace in column names
                    df.columns = [str(col).strip() for col in df.columns]
                    
                    # Show what columns we found
                    st.sidebar.info(f"📋 Columns in {file.name}: {list(df.columns)}")
                    
                    # REQUIRED COLUMNS CHECK - using YOUR exact column names
                    required_columns = ['Region', 'PSA', 'Sub Category', 'Bonus Buy', 'Sales Date', 
                                       'Promotion Status', 'Article', 'Net Sales', 'Sales Quantity']
                    
                    # Check which required columns are present
                    available_columns = list(df.columns)
                    missing_columns = [col for col in required_columns if col not in available_columns]
                    
                    if missing_columns:
                        st.sidebar.error(f"❌ File '{file.name}' missing columns: {', '.join(missing_columns)}")
                        st.sidebar.info(f"Available columns: {available_columns}")
                        continue
                    
                    st.sidebar.success(f"✅ All required columns found in {file.name}")
                    
                    # MINIMAL PROCESSING
                    if 'Promotion Status' in df.columns:
                        df['Promotion Status'] = df['Promotion Status'].astype(str).str.upper()
                    
                    # Use our FIXED date parser for dd/mm/yyyy
                    if 'Sales Date' in df.columns:
                        df['Sales Date'] = df['Sales Date'].apply(parse_sales_date)
                    
                    if 'Sales Quantity' in df.columns:
                        df['Sales Quantity'] = pd.to_numeric(df['Sales Quantity'], errors='coerce').fillna(0)
                    if 'Net Sales' in df.columns:
                        df['Net Sales'] = pd.to_numeric(df['Net Sales'], errors='coerce').fillna(0)
                    
                    # Only drop rows with invalid dates if they exist
                    if 'Sales Date' in df.columns:
                        invalid_dates = df['Sales Date'].isna().sum()
                        if invalid_dates > 0:
                            df = df.dropna(subset=['Sales Date'])
                            st.sidebar.info(f"Removed {invalid_dates} invalid dates from {file.name}")
                    
                    df['source_file'] = file.name
                    dfs.append(df)
                    
                except Exception as e:
                    st.sidebar.error(f"Error loading {file.name}: {str(e)}")
                    continue
            
            if dfs:
                # Combine all files
                new_df = pd.concat(dfs, ignore_index=True)
                new_df = new_df.drop_duplicates()
                
                # Save to CSV
                new_df.to_csv(TEMP_FILE, index=False)
                
                # Show date info for debugging
                if 'Sales Date' in new_df.columns:
                    valid_dates = new_df['Sales Date'].notna().sum()
                    invalid_dates = new_df['Sales Date'].isna().sum()
                    st.sidebar.success(f"✅ Loaded {len(new_df):,} rows from {len(dfs)} files")
                    st.sidebar.info(f"📅 Valid dates: {valid_dates:,}, Invalid: {invalid_dates}")
                    
                    if valid_dates > 0:
                        date_min = new_df['Sales Date'].min()
                        date_max = new_df['Sales Date'].max()
                        if pd.notna(date_min) and pd.notna(date_max):
                            st.sidebar.info(f"📅 Date range: {date_min.date()} to {date_max.date()}")
                
                st.session_state.files_processed = True
                st.rerun()

# -------------------------------
# Data Preparation - FIXED DATE PARSING
# -------------------------------
@st.cache_data(ttl=3600, max_entries=1)
def prepare_analysis_data(_forecast_handler, df):
    """Prepare data for analysis - FIXED date parsing"""
    df_prep = df.copy()
    
    # Ensure Sales Date is datetime (should already be from our parser)
    if 'Sales Date' in df_prep.columns:
        if not pd.api.types.is_datetime64_any_dtype(df_prep['Sales Date']):
            # Fallback: try our parser again
            df_prep['Sales Date'] = df_prep['Sales Date'].apply(parse_sales_date)
        
        # Remove rows with invalid dates (should be minimal)
        initial_rows = len(df_prep)
        df_prep = df_prep.dropna(subset=['Sales Date'])
        removed_rows = initial_rows - len(df_prep)
        if removed_rows > 0:
            st.sidebar.warning(f"Removed {removed_rows} rows with invalid dates during preparation")
        
        # Add time-based columns
        df_prep['Year'] = df_prep['Sales Date'].dt.year
        df_prep['Month'] = df_prep['Sales Date'].dt.month
    
    # Clean Article column
    if 'Article' in df_prep.columns:
        df_prep['Article'] = df_prep['Article'].astype(str).str.strip()
    
    # OPTIONAL: Match with forecast data if available
    if _forecast_handler.forecast_loaded:
        df_prep = _forecast_handler.match_forecast_with_promotions_simple(df_prep)
    else:
        # Add empty forecast columns for consistency
        df_prep['Has_Forecast'] = False
        df_prep['Forecast_Qty'] = None
        df_prep['Forecast_Daily_Qty'] = None
    
    return df_prep

# -------------------------------
# Sidebar Cache Management
# -------------------------------
st.sidebar.markdown("---")
st.sidebar.subheader("💾 Cache Management")

# Show cache stats
cache_stats = st.session_state.smart_cache.get_cache_stats()
st.sidebar.caption(f"Cached analyses: {cache_stats['total_cached_analyses']}")
st.sidebar.caption(f"Cache size: {cache_stats['total_cache_size_kb']} KB")
st.sidebar.caption(f"Cache hits: {cache_stats['cache_hits']}")

# Cache management buttons
if st.sidebar.button("🗑️ Clear All Cache", type="secondary", key="clear_cache_btn"):
    if st.session_state.smart_cache.clear_cache():
        st.sidebar.success("Cache cleared!")
        st.rerun()

# Add memory cleanup button to sidebar
st.sidebar.markdown("---")
st.sidebar.subheader("🧹 Memory Management")

if st.sidebar.button("🔧 Clear Cache & Free Memory", key="clear_memory_btn"):
    # Clear all caches
    st.cache_data.clear()
    
    # Clear smart cache
    st.session_state.smart_cache.clear_cache(force=True)
    
    # Force garbage collection
    gc.collect()
    
    # Clear session state cache
    if 'analysis_cache' in st.session_state:
        st.session_state.analysis_cache = {}
    
    # Cleanup analysis engine
    st.session_state.analysis_engine.cleanup()
    
    st.sidebar.success("Cache cleared and memory freed!")
    st.rerun()

# -------------------------------
# Load and prepare data - FIXED MAIN DATA LOADING
# -------------------------------
if 'existing_df' in st.session_state and st.session_state.existing_df is not None:
    # Use already loaded data
    raw_df = st.session_state.existing_df
else:
    # Try to load data from file
    if os.path.exists(TEMP_FILE):
        try:
            # Load with optimized settings
            with st.spinner("Loading data..."):
                file_size = os.path.getsize(TEMP_FILE) / (1024 * 1024 * 1024)
                
                if file_size > 1.0:
                    # Read in chunks for large files
                    chunks = []
                    chunk_size = 1000000
                    
                    for chunk in pd.read_csv(TEMP_FILE, chunksize=chunk_size, low_memory=False):
                        # Process dates in each chunk using our FIXED parser
                        if 'Sales Date' in chunk.columns:
                            chunk['Sales Date'] = chunk['Sales Date'].apply(parse_sales_date)
                        
                        chunks.append(chunk)
                    
                    raw_df = pd.concat(chunks, ignore_index=True)
                else:
                    # Read entire file
                    raw_df = pd.read_csv(TEMP_FILE, low_memory=False)
                    
                    # Process dates using our FIXED parser
                    if 'Sales Date' in raw_df.columns:
                        raw_df['Sales Date'] = raw_df['Sales Date'].apply(parse_sales_date)
                
                # Check date parsing results
                if 'Sales Date' in raw_df.columns:
                    valid_dates = raw_df['Sales Date'].notna().sum()
                    invalid_dates = raw_df['Sales Date'].isna().sum()
                    
                    st.info(f"📅 Date parsing: {valid_dates:,} valid, {invalid_dates:,} invalid")
                    
                    if invalid_dates > 0:
                        # Show sample of problematic dates
                        invalid_samples = raw_df[raw_df['Sales Date'].isna()].head(3)
                        with st.expander("Show problematic date samples", expanded=True):
                            st.write("Sample of rows with invalid dates:")
                            st.write(invalid_samples.head(10))
                        
                        # Drop invalid dates
                        raw_df = raw_df.dropna(subset=['Sales Date'])
                        st.info(f"Removed {invalid_dates} rows with invalid dates")
                
                # Store in session state for future use
                st.session_state.existing_df = raw_df
        except Exception as e:
            st.error(f"Error loading data file: {str(e)}")
            st.stop()
    else:
        st.warning("No data available. Please upload files first.")
        st.stop()

# Prepare the data - FIXED with better error handling
if st.session_state.analysis_df is None:
    try:
        # Try the standard preparation first
        analysis_df = prepare_analysis_data(st.session_state.forecast_handler, raw_df)
        st.session_state.analysis_df = analysis_df
        
        # Show success message with date info
        if 'Sales Date' in analysis_df.columns:
            date_min = analysis_df['Sales Date'].min()
            date_max = analysis_df['Sales Date'].max()
            if pd.notna(date_min) and pd.notna(date_max):
                st.success(f"✅ Data prepared: {len(analysis_df):,} rows ready for analysis")
                st.info(f"📅 Date range in analysis data: {date_min.date()} to {date_max.date()}")
            else:
                st.success(f"✅ Data prepared: {len(analysis_df):,} rows ready for analysis")
        else:
            st.success(f"✅ Data prepared: {len(analysis_df):,} rows ready for analysis")
    except Exception as e:
        st.error(f"Error preparing data: {str(e)}")
        
        # Fallback to manual preparation without the problematic forecast handler
        try:
            st.info("Attempting alternative data preparation...")
            analysis_df = raw_df.copy()
            
            # Basic date processing
            if 'Sales Date' in analysis_df.columns:
                analysis_df['Sales Date'] = analysis_df['Sales Date'].apply(parse_sales_date)
                analysis_df = analysis_df.dropna(subset=['Sales Date'])
                analysis_df['Year'] = analysis_df['Sales Date'].dt.year
                analysis_df['Month'] = analysis_df['Sales Date'].dt.month
            
            # Clean Article column
            if 'Article' in analysis_df.columns:
                analysis_df['Article'] = analysis_df['Article'].astype(str).str.strip()
            
            # Add empty forecast columns
            analysis_df['Has_Forecast'] = False
            analysis_df['Forecast_Qty'] = None
            analysis_df['Forecast_Daily_Qty'] = None
            
            st.session_state.analysis_df = analysis_df
            st.success(f"✅ Data prepared (basic method): {len(analysis_df):,} rows ready for analysis")
            
        except Exception as e2:
            st.error(f"Fallback preparation also failed: {str(e2)}")
            # Use raw data as-is
            st.session_state.analysis_df = raw_df
            st.warning("Using raw data without full preparation")
else:
    analysis_df = st.session_state.analysis_df

# Initialize analysis engine
engine = st.session_state.analysis_engine

# -------------------------------
# Tabs
# -------------------------------
tab_summary, tab_article, tab_upload = st.tabs(["📊 Summary Dashboard", "📈 Article Analysis", "📁 Data Management"])

# ============================================================================
# SUMMARY DASHBOARD
# ============================================================================
with tab_summary:
    st.title("📊 Region-Level Promo Evidence Dashboard")
    st.markdown("**Fast analysis using pre-cleaned data**")
    
    # Show forecast status
    if st.session_state.forecast_handler.forecast_loaded:
        forecast_stats = st.session_state.forecast_handler.get_forecast_stats()
        if forecast_stats:
            st.success(f"✅ Forecast data loaded: {forecast_stats['total_forecasts']} forecasts")
            st.info(f"📅 Forecast date range: {forecast_stats['date_range']}")
        else:
            st.info("ℹ️ Forecast data loaded but statistics unavailable")
    else:
        st.info("ℹ️ No forecast data loaded. Upload forecast files in the sidebar to enable forecast analysis.")
    
    if analysis_df.empty:
        st.info("No data available. Please upload files first.")
        st.stop()
    
    # Check required columns - UPDATED FOR YOUR COLUMN NAMES
    required_cols = ['Region', 'Article', 'Promotion Status', 'Net Sales', 'Sales Date', 'Year', 'Month']
    missing_cols = [col for col in required_cols if col not in analysis_df.columns]
    
    if missing_cols:
        st.error(f"Missing required columns: {', '.join(missing_cols)}")
        st.info(f"Available columns: {list(analysis_df.columns)}")
        st.stop()
    
    # Initialize session state for analysis
    if 'selected_year' not in st.session_state:
        st.session_state.selected_year = None
    if 'selected_month' not in st.session_state:
        st.session_state.selected_month = None
    if 'selected_subcat' not in st.session_state:
        st.session_state.selected_subcat = None
    if 'selected_region' not in st.session_state:
        st.session_state.selected_region = None
    if 'selected_psa' not in st.session_state:
        st.session_state.selected_psa = None
    
    # FILTERS IN MAIN PANEL
    st.subheader("🎯 Analysis Filters")
    
    col1, col2, col3, col4, col5, col6 = st.columns([2, 2, 2, 2, 2, 1])
    with col1:
        # Year filter with "All" option
        years = ["All"] + sorted(analysis_df['Year'].dropna().unique().tolist())
        selected_year = st.selectbox("Select Year", years, key="summary_year")
    with col2:
        # Month filter with "All" option
        months = ["All"] + sorted(analysis_df['Month'].dropna().unique().tolist())
        selected_month = st.selectbox("Select Month", months, key="summary_month")
    with col3:
        if 'Sub Category' in analysis_df.columns:
            subcats = ["All"] + sorted(analysis_df['Sub Category'].dropna().unique().tolist())
            selected_subcat = st.selectbox("Sub Category", subcats, key="subcat")
        else:
            selected_subcat = "All"
    with col4:
        # PSA filter with "All" option
        if 'PSA' in analysis_df.columns:
            psas = ["All"] + sorted(analysis_df['PSA'].dropna().unique().tolist())
            selected_psa = st.selectbox("PSA", psas, key="psa_filter")
        else:
            selected_psa = "All"
    with col5:
        # Region filter with "All" option
        regions = ["All"] + sorted(analysis_df['Region'].dropna().unique().tolist())
        selected_region = st.selectbox("Region", regions, key="region_filter")
    with col6:
        min_promo_days = st.slider("Min Promo Days", 1, 30, 3,
                                  help="Filter out short promotions for stable analysis")
    
    # Store min_promo_days in session state
    st.session_state.current_min_days = min_promo_days
    
    # Cache options
    col_cache1, col_cache2 = st.columns([1, 3])
    with col_cache1:
        use_cache = st.checkbox("Use Cache", value=True, help="Use cached results when available")
    with col_cache2:
        force_refresh = st.checkbox("Force refresh analysis (ignore cache)", value=False, 
                                   help="Force fresh analysis, bypassing cache")
    
    # Analyze button
    if st.button("🚀 Analyze Data", type="primary", key="analyze_button"):
        with st.spinner("Analyzing data..."):
            # Step 1: Filter data
            filtered = analysis_df.copy()
            
            # Apply Year filter
            if selected_year != "All":
                filtered = filtered[filtered['Year'] == selected_year]
            
            # Apply Month filter
            if selected_month != "All":
                filtered = filtered[filtered['Month'] == selected_month]
            
            # Apply Sub Category filter
            if selected_subcat != "All" and 'Sub Category' in filtered.columns:
                filtered = filtered[filtered['Sub Category'] == selected_subcat]
            
            # Apply PSA filter
            if selected_psa != "All" and 'PSA' in filtered.columns:
                filtered = filtered[filtered['PSA'] == selected_psa]
            
            # Apply Region filter
            if selected_region != "All":
                filtered = filtered[filtered['Region'] == selected_region]
            
            # Store in session state
            st.session_state.filtered_df = filtered
            
            # Get current filters
            current_filters = {
                'year': selected_year,
                'month': selected_month,
                'subcat': selected_subcat,
                'psa': selected_psa,
                'region': selected_region,
                'min_days': min_promo_days
            }
            
            # Check if we should use cache
            if use_cache and not force_refresh:
                # Try to get cached analysis
                cached_results = perform_cached_summary_analysis(filtered, current_filters)
                
                if cached_results:
                    # Populate session state from cache
                    st.session_state.valid_promos = cached_results.get('valid_promos')
                    st.session_state.baselines_df = cached_results.get('baselines_df')
                    st.session_state.total_clean_days = cached_results.get('total_clean_days')
                    st.session_state.valid_articles = cached_results.get('valid_articles')
                    st.session_state.reliable_region_list = cached_results.get('reliable_region_list')
                    st.session_state.forecast_coverage = cached_results.get('forecast_coverage')
                    
                    # Show cache info
                    cache_date = cached_results.get('computed_at', 'Unknown')
                    st.success(f"✅ Loaded cached analysis from {cache_date[:10]}")
                else:
                    st.warning("No cached analysis found, computing fresh...")
                    # Compute fresh analysis
                    results = perform_cached_summary_analysis(filtered, current_filters)
                    
                    if results:
                        st.session_state.valid_promos = results.get('valid_promos')
                        st.session_state.baselines_df = results.get('baselines_df')
                        st.session_state.total_clean_days = results.get('total_clean_days')
                        st.session_state.valid_articles = results.get('valid_articles')
                        st.session_state.reliable_region_list = results.get('reliable_region_list')
                        st.session_state.forecast_coverage = results.get('forecast_coverage')
                        
                        st.success("✅ Fresh analysis complete")
                    else:
                        st.error("❌ Analysis failed - no valid promotions found")
            else:
                # Compute fresh analysis
                with st.spinner("Computing fresh analysis (ignore cache)..."):
                    results = perform_cached_summary_analysis(filtered, current_filters)
                    
                    if results:
                        st.session_state.valid_promos = results.get('valid_promos')
                        st.session_state.baselines_df = results.get('baselines_df')
                        st.session_state.total_clean_days = results.get('total_clean_days')
                        st.session_state.valid_articles = results.get('valid_articles')
                        st.session_state.reliable_region_list = results.get('reliable_region_list')
                        st.session_state.forecast_coverage = results.get('forecast_coverage')
                        
                        st.success("✅ Fresh analysis complete")
                    else:
                        st.error("❌ Analysis failed - no valid promotions found")
            
            # Update other session state variables
            st.session_state.analysis_complete = True
            st.session_state.selected_year = selected_year
            st.session_state.selected_month = selected_month
            st.session_state.selected_subcat = selected_subcat
            st.session_state.selected_psa = selected_psa
            st.session_state.selected_region = selected_region
            st.session_state.min_promo_days = min_promo_days
            st.session_state.use_cache = use_cache
            st.session_state.force_refresh = force_refresh
            
            st.success(f"✅ Data filtered: {len(filtered):,} records")
            st.rerun()
    
    # Use a container to display results
    results_container = st.container()
    
    # Display results if analysis is complete
    if st.session_state.analysis_complete and st.session_state.filtered_df is not None:
        with results_container:
            filtered = st.session_state.filtered_df
            
            # Data overview
            st.subheader("📈 Data Overview")
            
            # Build caption with ALL filters
            caption_parts = []
            if st.session_state.selected_year != "All":
                caption_parts.append(f"Year: {st.session_state.selected_year}")
            else:
                caption_parts.append(f"Year: All")
            
            if st.session_state.selected_month != "All":
                caption_parts.append(f"Month: {st.session_state.selected_month}")
            else:
                caption_parts.append(f"Month: All")
                
            if st.session_state.selected_subcat != "All":
                caption_parts.append(f"Sub Category: {st.session_state.selected_subcat}")
            else:
                caption_parts.append(f"Sub Category: All")
            
            if st.session_state.selected_psa != "All":
                caption_parts.append(f"PSA: {st.session_state.selected_psa}")
            else:
                caption_parts.append(f"PSA: All")
                
            if st.session_state.selected_region != "All":
                caption_parts.append(f"Region: {st.session_state.selected_region}")
            else:
                caption_parts.append(f"Region: All")
            
            # Add min promo days to caption
            caption_parts.append(f"Min Promo Days: {st.session_state.min_promo_days}")
            
            st.caption(" | ".join(caption_parts))
            
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                total_records = len(filtered)
                st.metric("Total Records", f"{total_records:,}")
            with col2:
                promo_records = len(filtered[filtered['Promotion Status'] != 'NON-PROMO'])
                st.metric("Promo Records", f"{promo_records:,}")
            with col3:
                nonpromo_records = len(filtered[filtered['Promotion Status'] == 'NON-PROMO'])
                st.metric("Non-Promo Records", f"{nonpromo_records:,}")
            with col4:
                unique_articles = filtered['Article'].nunique()
                st.metric("Unique Articles", f"{unique_articles:,}")
            with col5:
                if 'Has_Forecast' in filtered.columns:
                    forecast_records = filtered['Has_Forecast'].sum()
                    st.metric("Forecast Matches", f"{forecast_records:,}")
            
            if promo_records == 0:
                st.warning("No promotion data found for selected period")
            else:
                # Check if we have valid promotions
                if hasattr(st.session_state, 'valid_promos') and st.session_state.valid_promos is not None:
                    valid_promos = st.session_state.valid_promos
                    
                    # Apply min_promo_days filter to valid_promos if needed
                    current_min_days = st.session_state.min_promo_days
                    
                    # Filter valid_promos by min_days
                    valid_promos_filtered = valid_promos[valid_promos['Duration'] >= current_min_days].copy()
                    
                    # Show info about filtering
                    if len(valid_promos_filtered) < len(valid_promos):
                        filtered_out = len(valid_promos) - len(valid_promos_filtered)
                        st.info(f"📊 **Duration Filter Applied:** Showing {len(valid_promos_filtered)} promotions with ≥{current_min_days} days (filtered out {filtered_out} shorter promotions)")
                    else:
                        st.info(f"📅 **Clean Baseline Analysis:** Using {st.session_state.total_clean_days:,} non-promo days from {st.session_state.valid_articles:,} articles (no mixed promo/non-promo sales)")
                    
                    if st.session_state.selected_region != "All":
                        st.info(f"📊 **Region Focus:** Analyzing promotions for {st.session_state.selected_region} region only")
                    else:
                        st.info(f"📊 **Region Coverage:** Analyzing promotions across {len(st.session_state.reliable_region_list)} regions with promotion data")
                    
                    # Show forecast coverage if available
                    if hasattr(st.session_state, 'forecast_coverage') and st.session_state.forecast_coverage is not None:
                        forecast_coverage_pct = st.session_state.forecast_coverage * 100
                        if forecast_coverage_pct > 0:
                            st.success(f"📈 **Forecast Coverage:** {forecast_coverage_pct:.1f}% of promotions have forecast data")
                        else:
                            st.info("📈 **Forecast Coverage:** No forecast data available for analyzed promotions")
                    
                    # Promo analysis
                    if len(st.session_state.reliable_region_list) > 0:
                        st.subheader("🏆 Promotion Performance Analysis")
                        
                        if len(valid_promos_filtered) > 0:
                            # Top N promotions table
                            st.subheader("🏆 Overall Top & Bottom Performing Promotions")
                            
                            top_n = st.select_slider("Show top N promotions", options=[10, 20, 30, 50], value=20, key="overall_top_n")
                            
                            # First aggregate ALL filtered promotions
                            all_promos_agg = aggregate_promotions_with_forecast(valid_promos_filtered, include_region_count=True)
                            
                            # Sort by uplift percentage
                            all_promos_agg_sorted = all_promos_agg.sort_values('Uplift_Pct', ascending=False)
                            
                            # Take top N from aggregated data
                            top_promos_agg = all_promos_agg_sorted.head(min(top_n, len(all_promos_agg_sorted)))
                            bottom_promos_agg = all_promos_agg_sorted.tail(min(top_n, len(all_promos_agg_sorted))).sort_values('Uplift_Pct', ascending=True)
                            
                            tab_top, tab_bottom = st.tabs([f"Top {len(top_promos_agg)}", f"Bottom {len(bottom_promos_agg)}"])
                            
                            with tab_top:
                                # Calculate metrics from the aggregated top promotions
                                avg_uplift_top = top_promos_agg['Uplift_Pct'].mean() * 100
                                total_uplift_top = (top_promos_agg['PromoSales'].sum() - top_promos_agg['BaselineSales'].sum()) / 1000
                                
                                # Calculate average duration for top promotions
                                avg_duration_top = top_promos_agg['Duration'].mean()
                                
                                # Calculate forecast accuracy if available
                                forecast_accuracy_top = None
                                if 'Forecast_Accuracy' in top_promos_agg.columns:
                                    forecast_accuracy_top = top_promos_agg['Forecast_Accuracy'].mean() * 100
                                
                                col1, col2, col3, col4 = st.columns(4)
                                with col1:
                                    st.metric("Average Uplift", f"{avg_uplift_top:+.1f}%")
                                with col2:
                                    st.metric("Total Uplift", f"${total_uplift_top:,.0f}K")
                                with col3:
                                    st.metric("Avg Duration", f"{avg_duration_top:.1f} days")
                                with col4:
                                    if forecast_accuracy_top is not None:
                                        st.metric("Forecast Accuracy", f"{forecast_accuracy_top:+.1f}%")
                                    else:
                                        st.metric("Total Sales", f"${top_promos_agg['PromoSales'].sum():,.0f}")
                                
                                st.caption(f"Showing {len(top_promos_agg)} unique promotions (aggregated across regions) with ≥{current_min_days} days")
                                
                                # Show aggregated data
                                display_top = top_promos_agg.copy()
                                
                                # Select columns to display with proper ordering
                                display_cols = ['Article', 'Bonus Buy', 'PromoSales', 'BaselineSales']
                                
                                # Add PromoQuantity column
                                if 'PromoQuantity' in display_top.columns:
                                    display_cols.append('PromoQuantity')
                                
                                # Add forecast columns if available
                                if 'Forecast_Qty' in display_top.columns:
                                    display_cols.append('Forecast_Qty')
                                
                                display_cols.extend(['Uplift_Display', 'Duration', 'Region'])
                                
                                # Add forecast accuracy if available
                                if 'Forecast_Accuracy_Display' in display_top.columns:
                                    display_cols.append('Forecast_Accuracy_Display')
                                
                                display_top = display_top[display_cols].copy()
                                
                                # Create dictionary for column renaming
                                column_rename_dict = {
                                    'Article': 'Article',
                                    'Bonus Buy': 'Promotion',
                                    'PromoSales': 'Promo Sales',
                                    'BaselineSales': 'Baseline',
                                    'Uplift_Display': 'Uplift %',
                                    'Duration': 'Duration (Days)',
                                    'Region': 'Regions Count'
                                }
                                
                                if 'PromoQuantity' in display_top.columns:
                                    column_rename_dict['PromoQuantity'] = 'Promo Qty'
                                if 'Forecast_Qty' in display_top.columns:
                                    column_rename_dict['Forecast_Qty'] = 'Forecast Qty'
                                if 'Forecast_Accuracy_Display' in display_top.columns:
                                    column_rename_dict['Forecast_Accuracy_Display'] = 'Forecast Accuracy'
                                
                                # Rename columns
                                display_top = display_top.rename(columns=column_rename_dict)
                                
                                # Sort by Uplift % (highest to lowest)
                                display_top['Uplift_Numeric'] = display_top['Uplift %'].str.rstrip('%').astype(float)
                                display_top = display_top.sort_values('Uplift_Numeric', ascending=False)
                                display_top = display_top.drop('Uplift_Numeric', axis=1)
                                
                                # Format numeric columns
                                for col in ['Promo Sales', 'Baseline']:
                                    if col in display_top.columns:
                                        display_top[col] = display_top[col].apply(lambda x: f"${x:,.0f}")
                                
                                for col in ['Promo Qty', 'Forecast Qty']:
                                    if col in display_top.columns:
                                        display_top[col] = display_top[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
                                
                                st.dataframe(display_top, use_container_width=True, height=400)
                            
                            with tab_bottom:
                                # Calculate metrics from the aggregated bottom promotions
                                avg_uplift_bottom = bottom_promos_agg['Uplift_Pct'].mean() * 100
                                total_uplift_bottom = (bottom_promos_agg['PromoSales'].sum() - bottom_promos_agg['BaselineSales'].sum()) / 1000
                                
                                # Calculate average duration for bottom promotions
                                avg_duration_bottom = bottom_promos_agg['Duration'].mean()
                                
                                # Calculate forecast accuracy if available
                                forecast_accuracy_bottom = None
                                if 'Forecast_Accuracy' in bottom_promos_agg.columns:
                                    forecast_accuracy_bottom = bottom_promos_agg['Forecast_Accuracy'].mean() * 100
                                
                                col1, col2, col3, col4 = st.columns(4)
                                with col1:
                                    st.metric("Average Uplift", f"{avg_uplift_bottom:+.1f}%")
                                with col2:
                                    st.metric("Total Uplift", f"${total_uplift_bottom:,.0f}K")
                                with col3:
                                    st.metric("Avg Duration", f"{avg_duration_bottom:.1f} days")
                                with col4:
                                    if forecast_accuracy_bottom is not None:
                                        st.metric("Forecast Accuracy", f"{forecast_accuracy_bottom:+.1f}%")
                                    else:
                                        st.metric("Total Sales", f"${bottom_promos_agg['PromoSales'].sum():,.0f}")
                                
                                st.caption(f"Showing {len(bottom_promos_agg)} unique promotions (aggregated across regions) with ≥{current_min_days} days")
                                
                                # Show aggregated data
                                display_bottom = bottom_promos_agg.copy()
                                
                                # Select columns to display with proper ordering
                                display_cols = ['Article', 'Bonus Buy', 'PromoSales', 'BaselineSales']
                                
                                # Add PromoQuantity column
                                if 'PromoQuantity' in display_bottom.columns:
                                    display_cols.append('PromoQuantity')
                                
                                # Add forecast columns if available
                                if 'Forecast_Qty' in display_bottom.columns:
                                    display_cols.append('Forecast_Qty')
                                
                                display_cols.extend(['Uplift_Display', 'Duration', 'Region'])
                                
                                # Add forecast accuracy if available
                                if 'Forecast_Accuracy_Display' in display_bottom.columns:
                                    display_cols.append('Forecast_Accuracy_Display')
                                
                                display_bottom = display_bottom[display_cols].copy()
                                
                                # Create dictionary for column renaming
                                column_rename_dict = {
                                    'Article': 'Article',
                                    'Bonus Buy': 'Promotion',
                                    'PromoSales': 'Promo Sales',
                                    'BaselineSales': 'Baseline',
                                    'Uplift_Display': 'Uplift %',
                                    'Duration': 'Duration (Days)',
                                    'Region': 'Regions Count'
                                }
                                
                                if 'PromoQuantity' in display_bottom.columns:
                                    column_rename_dict['PromoQuantity'] = 'Promo Qty'
                                if 'Forecast_Qty' in display_bottom.columns:
                                    column_rename_dict['Forecast_Qty'] = 'Forecast Qty'
                                if 'Forecast_Accuracy_Display' in display_bottom.columns:
                                    column_rename_dict['Forecast_Accuracy_Display'] = 'Forecast Accuracy'
                                
                                # Rename columns
                                display_bottom = display_bottom.rename(columns=column_rename_dict)
                                
                                # Sort by Uplift % (lowest to highest)
                                display_bottom['Uplift_Numeric'] = display_bottom['Uplift %'].str.rstrip('%').astype(float)
                                display_bottom = display_bottom.sort_values('Uplift_Numeric', ascending=True)
                                display_bottom = display_bottom.drop('Uplift_Numeric', axis=1)
                                
                                # Format numeric columns
                                for col in ['Promo Sales', 'Baseline']:
                                    if col in display_bottom.columns:
                                        display_bottom[col] = display_bottom[col].apply(lambda x: f"${x:,.0f}")
                                
                                for col in ['Promo Qty', 'Forecast Qty']:
                                    if col in display_bottom.columns:
                                        display_bottom[col] = display_bottom[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
                                
                                st.dataframe(display_bottom, use_container_width=True, height=400)
                            
                            # Strategic Action Analysis
                            st.subheader("🎯 Overall Strategic Action Analysis")
                            
                            # Use the SAME aggregated data as the top/bottom tables
                            all_promos_agg = aggregate_promotions_with_forecast(valid_promos_filtered, include_region_count=True)
                            
                            # Add interpretation to the aggregated data
                            all_promos_agg['Interpretation'] = all_promos_agg['Uplift_Pct'].apply(get_uplift_interpretation)
                            
                            # Group recommendations using the SAME aggregated data
                            recommendations = all_promos_agg.groupby('Interpretation').agg({
                                'Bonus Buy': 'count',
                                'Uplift_Pct': 'mean',
                                'PromoSales': 'sum',
                                'BaselineSales': 'sum',
                                'Region': 'sum',
                                'Duration': 'mean'
                            }).reset_index()
                            recommendations = recommendations.rename(columns={
                                'Bonus Buy': 'Count',
                                'Uplift_Pct': 'Avg_Uplift',
                                'PromoSales': 'Total_Sales',
                                'BaselineSales': 'Total_Baseline',
                                'Region': 'Total_Regions',
                                'Duration': 'Avg_Duration'
                            })
                            recommendations['Total_Uplift'] = recommendations['Total_Sales'] - recommendations['Total_Baseline']
                            recommendations = recommendations.sort_values('Count', ascending=False)
                            
                            # Display recommendations with dropdowns
                            for _, row in recommendations.iterrows():
                                interpretation = row['Interpretation']
                                count = row['Count']
                                
                                # Get promotions for this interpretation
                                category_promos = all_promos_agg[all_promos_agg['Interpretation'] == interpretation].copy()
                                
                                if "Strong Success" in interpretation:
                                    with st.container():
                                        st.success(f"**{interpretation}** ({count} unique promotions)")
                                        
                                        col1, col2, col3, col4, col5 = st.columns(5)
                                        with col1:
                                            st.metric("Avg Uplift", f"{row['Avg_Uplift']:+.1%}", 
                                                     delta="mean uplift across unique promotions")
                                        with col2:
                                            st.metric("Total Sales", f"${row['Total_Sales']:,.0f}")
                                        with col3:
                                            st.metric("Total Uplift", f"${row['Total_Uplift']:,.0f}")
                                        with col4:
                                            st.metric("Avg Duration", f"{row['Avg_Duration']:.1f} days")
                                        with col5:
                                            st.write(f"**Action:** Scale mechanics, repeat in similar articles")
                                            st.caption(f"Across {row['Total_Regions']} total region-promotion combinations")
                                        
                                        # DROPDOWN for promotion details
                                        with st.expander(f"📋 View {count} Promotions Details", expanded=False):
                                            # Prepare display data
                                            display_promos = category_promos.copy()
                                            display_promos = display_promos.sort_values('Uplift_Pct', ascending=False)
                                            
                                            # Select columns to display
                                            display_cols = ['Article', 'Bonus Buy', 'PromoSales', 'BaselineSales']
                                            if 'PromoQuantity' in display_promos.columns:
                                                display_cols.append('PromoQuantity')
                                            if 'Forecast_Qty' in display_promos.columns:
                                                display_cols.append('Forecast_Qty')
                                            display_cols.extend(['Uplift_Display', 'Duration', 'Region'])
                                            if 'Forecast_Accuracy_Display' in display_promos.columns:
                                                display_cols.append('Forecast_Accuracy_Display')
                                            
                                            display_promos = display_promos[display_cols].copy()
                                            
                                            # Create dictionary for column renaming
                                            column_rename_dict = {
                                                'Article': 'Article',
                                                'Bonus Buy': 'Promotion',
                                                'PromoSales': 'Promo Sales',
                                                'BaselineSales': 'Baseline',
                                                'Uplift_Display': 'Uplift %',
                                                'Duration': 'Duration (Days)',
                                                'Region': 'Regions Count'
                                            }
                                            
                                            if 'PromoQuantity' in display_promos.columns:
                                                column_rename_dict['PromoQuantity'] = 'Promo Qty'
                                            if 'Forecast_Qty' in display_promos.columns:
                                                column_rename_dict['Forecast_Qty'] = 'Forecast Qty'
                                            if 'Forecast_Accuracy_Display' in display_promos.columns:
                                                column_rename_dict['Forecast_Accuracy_Display'] = 'Forecast Accuracy'
                                            
                                            # Rename columns
                                            display_promos = display_promos.rename(columns=column_rename_dict)
                                            
                                            # Sort by Uplift % (highest to lowest)
                                            display_promos['Uplift_Numeric'] = display_promos['Uplift %'].str.rstrip('%').astype(float)
                                            display_promos = display_promos.sort_values('Uplift_Numeric', ascending=False)
                                            display_promos = display_promos.drop('Uplift_Numeric', axis=1)
                                            
                                            # Format numeric columns
                                            for col in ['Promo Sales', 'Baseline']:
                                                if col in display_promos.columns:
                                                    display_promos[col] = display_promos[col].apply(lambda x: f"${x:,.0f}")
                                            
                                            for col in ['Promo Qty', 'Forecast Qty']:
                                                if col in display_promos.columns:
                                                    display_promos[col] = display_promos[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
                                            
                                            st.dataframe(display_promos, use_container_width=True, height=300)
                                
                                elif "Significant Loss" in interpretation:
                                    with st.container():
                                        st.error(f"**{interpretation}** ({count} unique promotions)")
                                        
                                        col1, col2, col3, col4, col5 = st.columns(5)
                                        with col1:
                                            st.metric("Avg Uplift", f"{row['Avg_Uplift']:+.1%}", 
                                                     delta="mean uplift across unique promotions")
                                        with col2:
                                            st.metric("Total Sales", f"${row['Total_Sales']:,.0f}")
                                        with col3:
                                            st.metric("Total Loss", f"-${abs(row['Total_Uplift']):,.0f}")
                                        with col4:
                                            st.metric("Avg Duration", f"{row['Avg_Duration']:.1f} days")
                                        with col5:
                                            st.write(f"**Action:** Stop immediately, analyze root causes")
                                            st.caption(f"Across {row['Total_Regions']} total region-promotion combinations")
                                        
                                        # DROPDOWN for promotion details
                                        with st.expander(f"📋 View {count} Promotions Details", expanded=False):
                                            # Prepare display data
                                            display_promos = category_promos.copy()
                                            display_promos = display_promos.sort_values('Uplift_Pct', ascending=True)  # Sort worst first
                                            
                                            # Select columns to display
                                            display_cols = ['Article', 'Bonus Buy', 'PromoSales', 'BaselineSales']
                                            if 'PromoQuantity' in display_promos.columns:
                                                display_cols.append('PromoQuantity')
                                            if 'Forecast_Qty' in display_promos.columns:
                                                display_cols.append('Forecast_Qty')
                                            display_cols.extend(['Uplift_Display', 'Duration', 'Region'])
                                            if 'Forecast_Accuracy_Display' in display_promos.columns:
                                                display_cols.append('Forecast_Accuracy_Display')
                                            
                                            display_promos = display_promos[display_cols].copy()
                                            
                                            # Create dictionary for column renaming
                                            column_rename_dict = {
                                                'Article': 'Article',
                                                'Bonus Buy': 'Promotion',
                                                'PromoSales': 'Promo Sales',
                                                'BaselineSales': 'Baseline',
                                                'Uplift_Display': 'Uplift %',
                                                'Duration': 'Duration (Days)',
                                                'Region': 'Regions Count'
                                            }
                                            
                                            if 'PromoQuantity' in display_promos.columns:
                                                column_rename_dict['PromoQuantity'] = 'Promo Qty'
                                            if 'Forecast_Qty' in display_promos.columns:
                                                column_rename_dict['Forecast_Qty'] = 'Forecast Qty'
                                            if 'Forecast_Accuracy_Display' in display_promos.columns:
                                                column_rename_dict['Forecast_Accuracy_Display'] = 'Forecast Accuracy'
                                            
                                            # Rename columns
                                            display_promos = display_promos.rename(columns=column_rename_dict)
                                            
                                            # Sort by Uplift % (lowest to highest - worst first)
                                            display_promos['Uplift_Numeric'] = display_promos['Uplift %'].str.rstrip('%').astype(float)
                                            display_promos = display_promos.sort_values('Uplift_Numeric', ascending=True)
                                            display_promos = display_promos.drop('Uplift_Numeric', axis=1)
                                            
                                            # Format numeric columns
                                            for col in ['Promo Sales', 'Baseline']:
                                                if col in display_promos.columns:
                                                    display_promos[col] = display_promos[col].apply(lambda x: f"${x:,.0f}")
                                            
                                            for col in ['Promo Qty', 'Forecast Qty']:
                                                if col in display_promos.columns:
                                                    display_promos[col] = display_promos[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
                                            
                                            st.dataframe(display_promos, use_container_width=True, height=300)
                                
                                elif "Positive Impact" in interpretation or "Needs Adjustment" in interpretation or "Neutral/Breakeven" in interpretation:
                                    # For other categories, use a simpler expander
                                    with st.container():
                                        if "Positive Impact" in interpretation:
                                            st.info(f"**{interpretation}** ({count} unique promotions)")
                                        elif "Needs Adjustment" in interpretation:
                                            st.warning(f"**{interpretation}** ({count} unique promotions)")
                                        else:
                                            st.info(f"**{interpretation}** ({count} unique promotions)")
                                        
                                        col1, col2, col3, col4 = st.columns(4)
                                        with col1:
                                            st.metric("Avg Uplift", f"{row['Avg_Uplift']:+.1%}")
                                        with col2:
                                            st.metric("Total Sales", f"${row['Total_Sales']:,.0f}")
                                        with col3:
                                            st.metric("Total Uplift", f"${row['Total_Uplift']:,.0f}")
                                        with col4:
                                            st.metric("Avg Duration", f"{row['Avg_Duration']:.1f} days")
                                        
                                        # DROPDOWN for promotion details
                                        with st.expander(f"📋 View {count} Promotions Details", expanded=False):
                                            # Prepare display data
                                            display_promos = category_promos.copy()
                                            display_promos = display_promos.sort_values('Uplift_Pct', ascending=False)
                                            
                                            # Select columns to display
                                            display_cols = ['Article', 'Bonus Buy', 'PromoSales', 'BaselineSales', 'Uplift_Display', 'Duration', 'Region']
                                            display_promos = display_promos[display_cols].copy()
                                            
                                            # Rename columns
                                            display_promos = display_promos.rename(columns={
                                                'Article': 'Article',
                                                'Bonus Buy': 'Promotion',
                                                'PromoSales': 'Promo Sales',
                                                'BaselineSales': 'Baseline',
                                                'Uplift_Display': 'Uplift %',
                                                'Duration': 'Duration (Days)',
                                                'Region': 'Regions Count'
                                            })
                                            
                                            # Format numeric columns
                                            display_promos['Promo Sales'] = display_promos['Promo Sales'].apply(lambda x: f"${x:,.0f}")
                                            display_promos['Baseline'] = display_promos['Baseline'].apply(lambda x: f"${x:,.0f}")
                                            
                                            st.dataframe(display_promos, use_container_width=True, height=300)
                            
                        else:
                            st.warning(f"No valid promotions found with ≥{current_min_days} days")
                    else:
                        st.warning("No promotion data found for selected region(s)")
                else:
                    st.warning("No articles with sufficient clean baseline data found")
    else:
        st.info("👈 Select filters and click 'Analyze Data' to begin analysis")
    
    # Clear analysis button
    if st.session_state.analysis_complete:
        st.markdown("---")
        if st.button("🔄 Clear Analysis", type="secondary", key="clear_analysis"):
            st.session_state.analysis_complete = False
            st.session_state.filtered_df = None
            st.session_state.selected_year = None
            st.session_state.selected_month = None
            st.session_state.selected_subcat = None
            st.session_state.selected_psa = None
            st.session_state.selected_region = None
            st.session_state.analysis_cache = {}
            st.rerun()

# ============================================================================
# ARTICLE DASHBOARD
# ============================================================================
with tab_article:
    st.title("📈 Article-Level Promo Performance")
    st.markdown("**Analyze promotion effectiveness for specific articles**")
    
    if analysis_df.empty:
        st.info("No data available. Please upload files first.")
    else:
        # Initialize engine with data
        engine.initialize(analysis_df)
        
        # Get valid articles FAST using DuckDB
        with st.spinner("Finding articles with promo history..."):
            valid_articles = engine.get_valid_articles_fast()
            
            if len(valid_articles) == 0:
                st.error("❌ No articles found with both promotion and clean non-promotion data")
                st.info("""
                **Requirements for analysis:**
                1. Article must have at least 2 promo sales
                2. Article must have at least 2 non-promo sales
                """)
                st.stop()
            
            # Sort valid articles alphabetically
            promo_articles_sorted = sorted(valid_articles)
        
        # Article selection
        col1, col2 = st.columns([3, 1])
        with col1:
            selected_article = st.selectbox("Select Article", promo_articles_sorted, key="article_select")
        with col2:
            min_days_threshold = st.slider("Min Days", 1, 30, 2, 
                                          help="Minimum promotion days for analysis",
                                          key="article_min_days")
        
        # Get data for selected article - FAST with DuckDB
        with st.spinner(f"Loading data for {selected_article}..."):
            df_article_all = engine.get_article_data(selected_article)
        
        # Get article info
        article_subcat = df_article_all['Sub Category'].iloc[0] if len(df_article_all) > 0 and 'Sub Category' in df_article_all.columns else "Unknown"
        article_psa = df_article_all['PSA'].iloc[0] if len(df_article_all) > 0 and 'PSA' in df_article_all.columns else "Unknown"
        
        # Check for forecast data
        has_forecast = 'Has_Forecast' in df_article_all.columns and df_article_all['Has_Forecast'].any()
        
        # Get clean non-promo data
        article_promo_dates = df_article_all[
            df_article_all['Promotion Status'] != 'NON-PROMO'
        ]['Sales Date'].unique()
        
        non_promo = df_article_all[
            (df_article_all['Promotion Status'] == 'NON-PROMO') & 
            (~df_article_all['Sales Date'].isin(article_promo_dates))
        ]
        
        promo = df_article_all[df_article_all['Promotion Status'] != 'NON-PROMO']
        
        # Check data availability
        if len(promo) == 0:
            st.error(f"❌ No promotion data found for {selected_article}")
            st.stop()
        
        if len(non_promo) == 0:
            st.error(f"❌ No clean non-promotion baseline data found for {selected_article}")
            st.info(f"**Note:** Need at least 2 days with NO promo sales for this specific article")
            st.stop()
        
        # Show clean baseline info
        clean_baseline_days = non_promo['Sales Date'].nunique()
        total_nonpromo_days = df_article_all[df_article_all['Promotion Status'] == 'NON-PROMO']['Sales Date'].nunique()
        
        # Article Overview
        st.subheader(f"📦 Promotion Performance: {selected_article}")
        st.caption(f"**Sub Category:** {article_subcat} | **PSA:** {article_psa}")
        
        if has_forecast:
            st.success("✅ Forecast data available for this article")
        
        # Quick PROMO stats
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Promotion Days", promo['Sales Date'].nunique())
        with col2:
            st.metric("Promo Records", len(promo))
        with col3:
            total_promo_sales = promo['Net Sales'].sum()
            st.metric("Total Promo Sales", f"${total_promo_sales:,.0f}")
        with col4:
            unique_promos = promo['Bonus Buy'].nunique()
            st.metric("Promotion Types", unique_promos)
        with col5:
            st.metric("Clean Baseline Days", clean_baseline_days, 
                     f"of {total_nonpromo_days} total non-promo days")
        
        # Calculate baselines from CLEAN non-promo data
        region_nonpromo = non_promo.groupby('Region').agg(
            NonPromoSales=('Net Sales', 'sum'),
            NonPromoDays=('Sales Date', 'nunique'),
            AvgQuantity=('Sales Quantity', 'mean')
        ).reset_index()
        region_nonpromo['Daily_NonPromo_Avg'] = region_nonpromo['NonPromoSales'] / region_nonpromo['NonPromoDays']
        
        # Overall article baseline
        overall_daily_avg = non_promo['Net Sales'].sum() / max(non_promo['Sales Date'].nunique(), 1)
        
        # Promotion Analysis - Group by promotion type and region WITH FORECAST
        promo_summary_by_bonus = promo.groupby(['Bonus Buy', 'Region']).agg(
            PromoSales=('Net Sales', 'sum'),
            PromoQuantity=('Sales Quantity', 'sum'),
            PromoDays=('Sales Date', 'nunique'),
            AvgDailySales=('Net Sales', 'mean'),
            AvgDailyQuantity=('Sales Quantity', 'mean')
        ).reset_index()
        
        # Add forecast data if available
        if has_forecast:
            forecast_summary = promo.groupby(['Bonus Buy', 'Region']).agg({
                'Has_Forecast': 'max',
                'Forecast_Qty': 'max',
                'Forecast_Daily_Qty': 'max'
            }).reset_index()
            
            promo_summary_by_bonus = promo_summary_by_bonus.merge(
                forecast_summary,
                on=['Bonus Buy', 'Region'],
                how='left'
            )
        
        # Apply minimum days filter
        promo_summary_by_bonus = promo_summary_by_bonus[promo_summary_by_bonus['PromoDays'] >= min_days_threshold]
        
        if promo_summary_by_bonus.empty:
            st.warning(f"No promotions with at least {min_days_threshold} days of data")
            st.stop()
        
        # Calculate uplift at region level
        promo_summary_by_bonus = promo_summary_by_bonus.merge(
            region_nonpromo[['Region', 'Daily_NonPromo_Avg']], 
            on='Region', 
            how='left'
        )
        promo_summary_by_bonus['Baseline'] = promo_summary_by_bonus['Daily_NonPromo_Avg'] * promo_summary_by_bonus['PromoDays']
        promo_summary_by_bonus['Uplift'] = promo_summary_by_bonus['PromoSales'] - promo_summary_by_bonus['Baseline']
        promo_summary_by_bonus['Uplift_Pct'] = promo_summary_by_bonus['Uplift'] / promo_summary_by_bonus['Baseline']
        
        # Calculate forecast metrics if available
        if has_forecast and 'Forecast_Qty' in promo_summary_by_bonus.columns:
            # FIXED: Avoid division by zero
            promo_summary_by_bonus['Forecast_Accuracy'] = promo_summary_by_bonus.apply(
                lambda row: (row['PromoQuantity'] - row['Forecast_Qty']) / row['Forecast_Qty'] 
                if pd.notna(row['Forecast_Qty']) and row['Forecast_Qty'] > 0 else None, 
                axis=1
            )
            promo_summary_by_bonus['Forecast_Interpretation'] = promo_summary_by_bonus['Forecast_Accuracy'].apply(get_forecast_interpretation)
        
        # Filter reliable promotions (baseline >= 10)
        promo_summary_reliable = promo_summary_by_bonus[promo_summary_by_bonus['Baseline'] >= 10].copy()
        
        if not promo_summary_reliable.empty:
            # Aggregate by promotion type for overall view
            promo_agg = promo_summary_reliable.groupby('Bonus Buy').agg(
                Total_Sales=('PromoSales', 'sum'),
                Total_Quantity=('PromoQuantity', 'sum'),
                Avg_PromoDays=('PromoDays', 'mean'),
                Region_Count=('Region', 'nunique'),
                Baseline=('Baseline', 'sum'),
                Uplift=('Uplift', 'sum')
            ).reset_index()
            
            # Add forecast data if available
            if has_forecast and 'Forecast_Qty' in promo_summary_reliable.columns:
                forecast_agg = promo_summary_reliable.groupby('Bonus Buy').agg(
                    Total_Forecast=('Forecast_Qty', 'max'),
                    Forecast_Accuracy=('Forecast_Accuracy', 'mean')
                ).reset_index()
                promo_agg = promo_agg.merge(forecast_agg, on='Bonus Buy', how='left')
            
            promo_agg['Uplift_Pct'] = promo_agg['Uplift'] / promo_agg['Baseline']
            promo_agg['Uplift_Numeric'] = promo_agg['Uplift_Pct'] * 100
            promo_agg['Uplift_Display'] = promo_agg['Uplift_Numeric'].apply(lambda x: f"{x:+.1f}%")
            
            # Calculate average uplift
            avg_uplift_pct = promo_agg['Uplift_Numeric'].mean()
            num_reliable = len(promo_agg)
            
            # Calculate forecast accuracy if available
            avg_forecast_accuracy = None
            if has_forecast and 'Forecast_Accuracy' in promo_agg.columns:
                avg_forecast_accuracy = promo_agg['Forecast_Accuracy'].mean() * 100
            
            # Show average uplift
            st.subheader("📊 Overall Promotion Effectiveness")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Average Uplift", f"{avg_uplift_pct:+.1f}%", 
                         f"Based on {num_reliable} reliable promotions")
            with col2:
                success_rate = (promo_agg['Uplift_Pct'] > 0).mean() * 100
                st.metric("Success Rate", f"{success_rate:.1f}%")
            with col3:
                avg_duration = promo_agg['Avg_PromoDays'].mean()
                st.metric("Avg Duration", f"{avg_duration:.1f} days")
            with col4:
                if avg_forecast_accuracy is not None:
                    st.metric("Forecast Accuracy", f"{avg_forecast_accuracy:+.1f}%")
                else:
                    st.metric("Reliable Regions", promo_summary_reliable['Region'].nunique())
            
            # Effectiveness indicator
            if avg_uplift_pct >= 10:
                st.success(f"✅ **PROMOTIONS ARE EFFECTIVE** - Average uplift of {avg_uplift_pct:+.1f}%")
            elif avg_uplift_pct >= 0:
                st.warning(f"⚠️ **MIXED RESULTS** - Average uplift of {avg_uplift_pct:+.1f}%")
            else:
                st.error(f"❌ **PROMOTIONS ARE INEFFECTIVE** - Average uplift of {avg_uplift_pct:+.1f}%")
            
            # FIXED: Promotion Performance Comparison Chart WITH RED DIAMOND BASELINE POINTS
            st.subheader("📈 Promotion Performance Comparison")
            
            # Sort by numeric uplift
            promo_agg = promo_agg.sort_values('Uplift_Numeric', ascending=False)
            
            # Create bar chart for promotions
            bar_chart = alt.Chart(promo_agg).mark_bar().encode(
                x=alt.X('Total_Sales:Q', title='Total Sales ($)', axis=alt.Axis(format='$,.0f')),
                y=alt.Y('Bonus Buy:N', sort='-x', title='Promotion'),
                color=alt.Color('Uplift_Numeric:Q', 
                              scale=alt.Scale(scheme='redyellowgreen'),
                              legend=alt.Legend(title='Uplift %', format='+.0f')),
                tooltip=[
                    alt.Tooltip('Bonus Buy:N', title='Promotion'),
                    alt.Tooltip('Total_Sales:Q', title='Total Sales', format='$,.0f'),
                    alt.Tooltip('Avg_PromoDays:Q', title='Avg Promo Days', format='.1f'),
                    alt.Tooltip('Baseline:Q', title='Expected Baseline', format='$,.0f'),
                    alt.Tooltip('Uplift:Q', title='Uplift Amount', format='$,.0f'),
                    alt.Tooltip('Uplift_Pct:Q', title='Uplift %', format='.1%')
                ]
            )
            
            # Create red diamond for baseline - ONE DIAMOND ON EACH BAR
            baseline_diamonds = alt.Chart(promo_agg).mark_point(
                shape='diamond',
                size=100,
                color='red',
                filled=True
            ).encode(
                x=alt.X('Baseline:Q', title=''),
                y=alt.Y('Bonus Buy:N', sort='-x'),
                tooltip=[
                    alt.Tooltip('Bonus Buy:N', title='Promotion'),
                    alt.Tooltip('Baseline:Q', title='Expected Baseline', format='$,.0f'),
                    alt.Tooltip('Uplift_Pct:Q', title='Uplift vs Baseline', format='.1%')
                ]
            )
            
            # Combine the charts - baseline diamonds will appear ON the bars
            combined_chart = (bar_chart + baseline_diamonds).properties(
                height=400,
                title='Promotion Performance vs Baseline (Red diamond = Expected baseline sales)'
            )
            
            st.altair_chart(combined_chart, use_container_width=True)
            
            # Promotion Performance Details Table
            st.subheader("📋 Promotion Performance Details")
            
            display_df = promo_agg.copy()
            
            # Select columns to display
            display_cols = ['Bonus Buy', 'Total_Sales', 'Total_Quantity', 
                          'Avg_PromoDays', 'Region_Count', 'Baseline', 'Uplift', 'Uplift_Numeric']
            
            # Add forecast columns if available
            if has_forecast and 'Total_Forecast' in display_df.columns:
                display_cols.extend(['Total_Forecast', 'Forecast_Accuracy'])
            
            display_df = display_df[display_cols].copy()
            
            # Create dictionary for column renaming
            rename_dict = {
                'Bonus Buy': 'Promotion',
                'Total_Sales': 'Promo Sales',
                'Total_Quantity': 'Promo Quantity',
                'Avg_PromoDays': 'Avg Duration',
                'Region_Count': 'Regions',
                'Baseline': 'Expected Baseline',
                'Uplift': 'Uplift Amount',
                'Uplift_Numeric': 'Uplift %'
            }
            
            # Add forecast column names if they exist
            if 'Total_Forecast' in display_df.columns:
                rename_dict['Total_Forecast'] = 'Forecast Qty'
            if 'Forecast_Accuracy' in display_df.columns:
                rename_dict['Forecast_Accuracy'] = 'Forecast Accuracy %'
            
            # Rename columns
            display_df = display_df.rename(columns=rename_dict)
            
            # Sort by Uplift %
            display_df = display_df.sort_values('Uplift %', ascending=False)
            
            # Format columns for display
            for col in ['Promo Sales', 'Expected Baseline', 'Uplift Amount']:
                if col in display_df.columns:
                    display_df[col] = display_df[col].apply(lambda x: f"${x:,.0f}")
            
            if 'Uplift %' in display_df.columns:
                display_df['Uplift %'] = display_df['Uplift %'].apply(lambda x: f"{x:+.1f}%")
            
            if 'Forecast Qty' in display_df.columns:
                display_df['Forecast Qty'] = display_df['Forecast Qty'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
            if 'Forecast Accuracy %' in display_df.columns:
                display_df['Forecast Accuracy %'] = display_df['Forecast Accuracy %'].apply(lambda x: f"{x*100:+.1f}%" if pd.notna(x) else "N/A")
            
            st.dataframe(display_df, use_container_width=True, height=400)
            
            # =================================================================
            # FIXED: 4-QUADRANT SCATTER CHART - FIXED TOOLTIP ISSUES
            # =================================================================
            st.subheader("📊 Promotion Quadrant Analysis: Sales vs Quantity")
            
            # Prepare data for scatter plot - use promotion type level data (each circle = one promotion type)
            scatter_data = promo_agg.copy()
            
            if len(scatter_data) > 0:
                # FIXED: Create a proper Promotion_Name column for tooltips
                scatter_data['Promotion_Name'] = scatter_data['Bonus Buy'].astype(str)
                
                # Create the scatter plot - Sales vs Quantity
                scatter = alt.Chart(scatter_data).mark_circle(size=300).encode(
                    x=alt.X('Total_Sales:Q', 
                           title='Total Sales ($)',
                           scale=alt.Scale(zero=False),
                           axis=alt.Axis(format='$,.0f')),
                    y=alt.Y('Total_Quantity:Q', 
                           title='Total Quantity Sold',
                           scale=alt.Scale(zero=False)),
                    color=alt.Color('Uplift_Pct:Q',
                                  scale=alt.Scale(scheme='redyellowgreen'),
                                  legend=alt.Legend(title='Uplift %', format='.0%')),
                    size=alt.Size('Avg_PromoDays:Q', 
                                 legend=alt.Legend(title='Avg Duration (days)'),
                                 scale=alt.Scale(range=[200, 800])),
                    tooltip=[
                        alt.Tooltip('Promotion_Name:N', title='Promotion'),
                        alt.Tooltip('Total_Sales:Q', title='Promo Sales', format='$,.0f'),
                        alt.Tooltip('Total_Quantity:Q', title='Promo Quantity', format=',.0f'),
                        alt.Tooltip('Uplift_Pct:Q', title='Uplift %', format='.1%'),
                        alt.Tooltip('Avg_PromoDays:Q', title='Avg Duration', format='.1f')
                        # REMOVED: Uplift amount and Region from tooltip as requested
                    ]
                ).properties(
                    height=500,
                    title='Promotion Performance: Sales vs Quantity (Each circle = one promotion type)'
                )
                
                # Add quadrant lines (median lines)
                median_sales = scatter_data['Total_Sales'].median()
                median_quantity = scatter_data['Total_Quantity'].median()
                
                hline = alt.Chart(pd.DataFrame({'y': [median_quantity]})).mark_rule(
                    strokeDash=[5, 5], color='gray'
                ).encode(y='y:Q')
                
                vline = alt.Chart(pd.DataFrame({'x': [median_sales]})).mark_rule(
                    strokeDash=[5, 5], color='gray'
                ).encode(x='x:Q')
                
                # Add quadrant labels
                quadrants_df = pd.DataFrame({
                    'x': [scatter_data['Total_Sales'].quantile(0.25), 
                          scatter_data['Total_Sales'].quantile(0.75),
                          scatter_data['Total_Sales'].quantile(0.25),
                          scatter_data['Total_Sales'].quantile(0.75)],
                    'y': [scatter_data['Total_Quantity'].quantile(0.75),
                          scatter_data['Total_Quantity'].quantile(0.75),
                          scatter_data['Total_Quantity'].quantile(0.25),
                          scatter_data['Total_Quantity'].quantile(0.25)],
                    'text': ['High Volume\nLow Value', 'High Volume\nHigh Value',
                            'Low Volume\nLow Value', 'Low Volume\nHigh Value']
                })
                
                text = alt.Chart(quadrants_df).mark_text(
                    align='center',
                    baseline='middle',
                    fontSize=14,
                    fontWeight='bold',
                    color='gray'
                ).encode(
                    x='x:Q',
                    y='y:Q',
                    text='text:N'
                )
                
                # Combine all elements
                final_chart = (scatter + hline + vline + text).configure_axis(
                    labelFontSize=12,
                    titleFontSize=14
                ).configure_title(
                    fontSize=16
                )
                
                st.altair_chart(final_chart, use_container_width=True)
                
                # Quadrant interpretation - Updated for Sales vs Quantity
                st.markdown("**Quadrant Analysis:**")
                col_q1, col_q2, col_q3, col_q4 = st.columns(4)
                
                with col_q1:
                    st.info("**🔴 High Volume, Low Value**\n\nHigh quantity but low revenue - consider price optimization")
                
                with col_q2:
                    st.success("**🟢 High Volume, High Value**\n\nBest performers - maximize distribution and repeat")
                
                with col_q3:
                    st.warning("**🟡 Low Volume, Low Value**\n\nLimited impact - consider discontinuing or testing new mechanics")
                
                with col_q4:
                    st.info("**🔵 Low Volume, High Value**\n\nHigh value per unit - focus on premium positioning or targeting")
        
        else:
            st.warning("No promotions with sufficient baseline data in reliable regions")

# ============================================================================
# DATA MANAGEMENT TAB
# ============================================================================
with tab_upload:
    st.title("📁 Data Management")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Current Data Status")
        if os.path.exists(TEMP_FILE):
            file_size = os.path.getsize(TEMP_FILE) / (1024 * 1024 * 1024)
            st.success(f"✅ Data file exists: {file_size:.2f} GB")
            
            if 'existing_df' in st.session_state and st.session_state.existing_df is not None:
                existing_df = st.session_state.existing_df
                st.success(f"📊 Data loaded: {existing_df.shape[0]:,} rows, {existing_df.shape[1]} columns")
                
                # Show data info
                if 'Sales Date' in existing_df.columns:
                    if not existing_df.empty and existing_df['Sales Date'].notna().any():
                        date_min = existing_df['Sales Date'].min()
                        date_max = existing_df['Sales Date'].max()
                        if pd.notna(date_min) and pd.notna(date_max):
                            st.info(f"📅 Date range: {date_min.date()} to {date_max.date()}")
                        else:
                            st.warning("Could not determine date range")
                    else:
                        st.warning("No valid dates found in data")
                
                if 'Article' in existing_df.columns:
                    st.info(f"📦 Unique articles: {existing_df['Article'].nunique():,}")
                
                if 'Region' in existing_df.columns:
                    st.info(f"📍 Unique regions: {existing_df['Region'].nunique():,}")
            
            # Show forecast status
            if st.session_state.forecast_handler.forecast_loaded:
                forecast_stats = st.session_state.forecast_handler.get_forecast_stats()
                if forecast_stats:
                    st.success(f"✅ Forecast data loaded: {forecast_stats['total_forecasts']} forecasts")
                    st.info(f"📅 Forecast date range: {forecast_stats['date_range']}")
                else:
                    st.info("ℹ️ Forecast data loaded but statistics unavailable")
            else:
                st.info("ℹ️ No forecast data loaded")
            
            # Quick preview
            if st.button("Preview First 100 Rows", key="preview_data"):
                if 'existing_df' in st.session_state and st.session_state.existing_df is not None:
                    preview = st.session_state.existing_df.head(100)
                    st.dataframe(preview, use_container_width=True)
        else:
            st.warning("No data loaded")
    
    with col2:
        st.subheader("Quick Actions")
        
        # Clear cache button
        if st.button("🔄 Clear Analysis Cache", key="clear_cache_tab"):
            st.session_state.analysis_cache = {}
            st.session_state.analysis_complete = False
            st.session_state.filtered_df = None
            st.success("Analysis cache cleared!")
        
        # Refresh data button
        if st.button("🔃 Refresh Data", key="refresh_data"):
            st.session_state.analysis_df = None
            st.session_state.analysis_cache = {}
            st.session_state.analysis_complete = False
            st.session_state.filtered_df = None
            st.session_state.existing_df = None
            st.success("Data will be refreshed on next analysis!")
            st.rerun()
        
        # Download forecast data button
        if st.session_state.forecast_handler.forecast_loaded:
            if st.button("📥 Download Forecast Data", key="download_forecast"):
                forecast_df = st.session_state.forecast_handler.forecast_df
                csv = forecast_df.to_csv(index=False)
                st.download_button(
                    label="Download Forecast CSV",
                    data=csv,
                    file_name="forecast_data.csv",
                    mime="text/csv",
                    key="download_forecast_btn"
                )
        
        # Backup info
        st.subheader("📦 Backups")
        backup_files = sorted(glob.glob("region_cleaned_data_temp_backup_*.csv"), reverse=True)
        if backup_files:
            st.info(f"Available backups: {len(backup_files)}")
            for i, backup in enumerate(backup_files[:3]):
                backup_time = backup.split('_')[-1].replace('.csv', '')
                st.caption(f"{i+1}. {backup_time}")
        else:
            st.caption("No backups available")
    
    # Add cache management section
    st.markdown("---")
    st.subheader("📦 Cache Management")
    
    col_cache1, col_cache2 = st.columns(2)
    with col_cache1:
        st.write("**Cache Statistics:**")
        for key, value in cache_stats.items():
            st.write(f"- {key.replace('_', ' ').title()}: {value}")
    
    with col_cache2:
        st.write("**Cache Actions:**")
        if st.button("🔄 Refresh Cache Stats", key="refresh_cache_stats"):
            st.rerun()
        
        if st.button("🧹 Clear Cache", type="secondary", key="clear_cache_tab2"):
            if st.session_state.smart_cache.clear_cache():
                st.success("Cache cleared successfully!")
                st.rerun()