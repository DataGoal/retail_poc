"""
Nike Global Retail - Star Schema Sample Data Generator
=======================================================
This script generates consistent, realistic sample data for all 12 tables
in the Nike retail data warehouse star schema.

Tables:
- Teradata: fact_sales_transactions, fact_inventory_snapshot
- Snowflake: dim_customer, dim_promotion
- Databricks: dim_product, dim_store, dim_date, dim_channel, dim_employee, 
              dim_geography, fact_returns, fact_web_sessions

Key Considerations:
1. Data consistency across tables (PK/FK relationships)
2. Realistic data for business decision-making
3. Accurate KPI calculations
4. CSV output for seamless database ingestion
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import random
import uuid
import hashlib
import os
from typing import Dict, List, Tuple

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# ============================================================
# CONFIGURATION
# ============================================================
OUTPUT_DIR = "/Volumes/databricks-poc-updated/nike_poc/data_volume/nike_sample_data_csv"
START_DATE = date(2023, 1, 1)
END_DATE = date(2025, 5, 31)
NUM_CUSTOMERS = 50000
NUM_PRODUCTS = 2000
NUM_STORES = 500
NUM_EMPLOYEES = 2500
NUM_PROMOTIONS = 150
NUM_TRANSACTIONS = 500000
NUM_WEB_SESSIONS = 800000

# Nike-specific constants
DIVISIONS = {
    'FW': 'Footwear',
    'AP': 'Apparel',
    'EQ': 'Equipment'
}

CATEGORIES = {
    'FW': ['Running', 'Basketball', 'Training', 'Lifestyle', 'Soccer', 'Golf'],
    'AP': ['Running', 'Training', 'Lifestyle', 'Basketball', 'Soccer', 'Golf'],
    'EQ': ['Bags', 'Balls', 'Accessories', 'Protective Gear', 'Training Equipment']
}

SUBCATEGORIES = {
    'Running': ['Road Running', 'Trail Running', 'Track & Field', 'Race Day'],
    'Basketball': ['Performance', 'Lifestyle', 'Retro'],
    'Training': ['Gym Training', 'Cross Training', 'HIIT'],
    'Lifestyle': ['Casual', 'Premium', 'Collaboration'],
    'Soccer': ['Firm Ground', 'Indoor', 'Turf'],
    'Golf': ['Spikeless', 'Spiked', 'Tour'],
    'Bags': ['Backpacks', 'Duffels', 'Gym Bags'],
    'Balls': ['Basketball', 'Soccer', 'Football'],
    'Accessories': ['Hats', 'Socks', 'Gloves', 'Headbands'],
    'Protective Gear': ['Shin Guards', 'Knee Pads', 'Elbow Pads'],
    'Training Equipment': ['Mats', 'Bands', 'Weights']
}

PRODUCT_LINES = {
    'Running': ['Pegasus', 'Vaporfly', 'Invincible', 'Infinity Run', 'Structure', 'Zoom Fly'],
    'Basketball': ['LeBron', 'KD', 'Giannis', 'Ja', 'Sabrina', 'Air Jordan'],
    'Training': ['Metcon', 'Free', 'SuperRep', 'Legend', 'Air Max Alpha'],
    'Lifestyle': ['Air Max', 'Air Force 1', 'Dunk', 'Blazer', 'Cortez', 'Waffle'],
    'Soccer': ['Mercurial', 'Phantom', 'Tiempo', 'Premier'],
    'Golf': ['Air Max 90 G', 'Air Jordan Golf', 'Victory', 'Infinity'],
    'Default': ['Essential', 'Pro', 'Elite', 'Performance']
}

COLORWAYS = ['White/Black', 'Black/White', 'University Red', 'Royal Blue', 
             'Volt', 'Pink Foam', 'Obsidian', 'Pure Platinum', 'Phantom',
             'Total Orange', 'Light Bone', 'Sail', 'Hyper Royal']

SIZES_FOOTWEAR = ['US6', 'US6.5', 'US7', 'US7.5', 'US8', 'US8.5', 'US9', 'US9.5', 
                  'US10', 'US10.5', 'US11', 'US11.5', 'US12', 'US13', 'US14']
SIZES_APPAREL = ['XS', 'S', 'M', 'L', 'XL', 'XXL', '3XL']
SIZES_EQUIPMENT = ['One Size', 'S/M', 'M/L', 'L/XL']

GENDER_TARGETS = ['MENS', 'WOMENS', 'UNISEX', 'KIDS']
AGE_GROUPS = ['ADULT', 'YOUTH', 'TODDLER']

REGIONS = {
    'AMER': {'name': 'Americas', 'countries': ['US', 'CA', 'MX', 'BR']},
    'EMEA': {'name': 'Europe, Middle East & Africa', 'countries': ['GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'AE']},
    'APAC': {'name': 'Asia Pacific', 'countries': ['CN', 'JP', 'KR', 'AU', 'IN']}
}

COUNTRY_INFO = {
    'US': {'name': 'United States', 'states': ['CA', 'NY', 'TX', 'FL', 'IL', 'WA', 'OR', 'CO', 'AZ', 'GA'],
           'cities': ['Los Angeles', 'New York', 'Houston', 'Miami', 'Chicago', 'Seattle', 'Portland', 'Denver', 'Phoenix', 'Atlanta']},
    'CA': {'name': 'Canada', 'states': ['ON', 'BC', 'AB', 'QC'], 'cities': ['Toronto', 'Vancouver', 'Calgary', 'Montreal']},
    'MX': {'name': 'Mexico', 'states': ['CDMX', 'JAL', 'NL'], 'cities': ['Mexico City', 'Guadalajara', 'Monterrey']},
    'BR': {'name': 'Brazil', 'states': ['SP', 'RJ', 'MG'], 'cities': ['Sao Paulo', 'Rio de Janeiro', 'Belo Horizonte']},
    'GB': {'name': 'United Kingdom', 'states': ['ENG', 'SCT', 'WAL'], 'cities': ['London', 'Manchester', 'Edinburgh', 'Cardiff']},
    'DE': {'name': 'Germany', 'states': ['BY', 'NW', 'BE'], 'cities': ['Berlin', 'Munich', 'Hamburg', 'Frankfurt']},
    'FR': {'name': 'France', 'states': ['IDF', 'ARA', 'PAC'], 'cities': ['Paris', 'Lyon', 'Marseille']},
    'IT': {'name': 'Italy', 'states': ['LOM', 'LAZ', 'CAM'], 'cities': ['Milan', 'Rome', 'Naples']},
    'ES': {'name': 'Spain', 'states': ['MD', 'CT', 'AN'], 'cities': ['Madrid', 'Barcelona', 'Seville']},
    'NL': {'name': 'Netherlands', 'states': ['NH', 'ZH'], 'cities': ['Amsterdam', 'Rotterdam']},
    'AE': {'name': 'United Arab Emirates', 'states': ['DU', 'AD'], 'cities': ['Dubai', 'Abu Dhabi']},
    'CN': {'name': 'China', 'states': ['BJ', 'SH', 'GD'], 'cities': ['Beijing', 'Shanghai', 'Guangzhou', 'Shenzhen']},
    'JP': {'name': 'Japan', 'states': ['TK', 'OS', 'KY'], 'cities': ['Tokyo', 'Osaka', 'Kyoto']},
    'KR': {'name': 'South Korea', 'states': ['SE', 'BS'], 'cities': ['Seoul', 'Busan']},
    'AU': {'name': 'Australia', 'states': ['NSW', 'VIC', 'QLD'], 'cities': ['Sydney', 'Melbourne', 'Brisbane']},
    'IN': {'name': 'India', 'states': ['MH', 'DL', 'KA'], 'cities': ['Mumbai', 'Delhi', 'Bangalore']}
}

STORE_TYPES = ['FLAGSHIP', 'FACTORY', 'CONCEPT', 'PARTNER', 'ECOM']
STORE_FORMATS = ['FULL_LINE', 'SPORT', 'RUN', 'BASKETBALL', 'DIGITAL']
CHANNEL_TYPES = ['BRICK_MORTAR', 'ECOMMERCE', 'WHOLESALE', 'APP']

LOYALTY_TIERS = ['Bronze', 'Silver', 'Gold', 'Elite']
CUSTOMER_SEGMENTS = ['Runner', 'Athlete', 'Casual', 'Fashion', 'Collector', 'Parent']
PREFERRED_SPORTS = ['Running', 'Basketball', 'Training', 'Soccer', 'Golf', 'Lifestyle']
LTV_BANDS = ['Low', 'Mid', 'High', 'VIP']
ACQUISITION_CHANNELS = ['Organic Search', 'Paid Search', 'Social Media', 'Email', 'Referral', 'Direct', 'Affiliate']

PROMOTION_TYPES = ['MARKDOWN', 'COUPON', 'BOGO', 'BUNDLE', 'LOYALTY', 'CLEARANCE']
DISCOUNT_TYPES = ['PERCENT', 'FIXED_AMOUNT', 'FREE_GIFT']
CAMPAIGN_CATEGORIES = ['SEASONAL', 'SPORT', 'PRODUCT_LAUNCH', 'CLEARANCE', 'MEMBER_EXCLUSIVE']
MARKETING_CHANNELS = ['EMAIL', 'APP', 'IN_STORE', 'SOCIAL', 'WEB', 'SMS']

RETURN_REASONS = {
    'SIZE': 'Wrong Size',
    'FIT': 'Poor Fit',
    'DEFECT': 'Product Defect',
    'COLOR': 'Color Mismatch',
    'QUALITY': 'Quality Issues',
    'CHANGED_MIND': 'Changed Mind',
    'DAMAGED': 'Received Damaged',
    'LATE_DELIVERY': 'Late Delivery'
}

RETURN_CONDITIONS = ['SALEABLE', 'DAMAGED', 'DEFECTIVE']

DEVICE_TYPES = ['DESKTOP', 'MOBILE', 'TABLET', 'APP']
OS_PLATFORMS = ['iOS', 'Android', 'Windows', 'MacOS', 'Linux']
BROWSERS = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Samsung Internet']
TRAFFIC_SOURCES = ['ORGANIC', 'PAID', 'EMAIL', 'SOCIAL', 'DIRECT', 'REFERRAL', 'AFFILIATE']


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def generate_hash(value: str) -> str:
    """Generate SHA-256 hash for PII fields."""
    return hashlib.sha256(value.encode()).hexdigest()[:64]

def generate_uuid() -> str:
    """Generate a UUID string."""
    return str(uuid.uuid4())

def date_to_sk(d: date) -> int:
    """Convert date to surrogate key (YYYYMMDD format)."""
    return int(d.strftime('%Y%m%d'))

def get_fiscal_info(d: date) -> Tuple[int, int, int, str]:
    """
    Calculate Nike fiscal calendar info (FY ends May 31).
    Returns: (fiscal_year, fiscal_quarter, fiscal_month, fiscal_quarter_name)
    """
    if d.month >= 6:
        fiscal_year = d.year + 1
        fiscal_month = d.month - 5
    else:
        fiscal_year = d.year
        fiscal_month = d.month + 7
    
    fiscal_quarter = (fiscal_month - 1) // 3 + 1
    fiscal_quarter_name = f"FQ{fiscal_quarter}-FY{fiscal_year}"
    
    return fiscal_year, fiscal_quarter, fiscal_month, fiscal_quarter_name

def get_retail_calendar(d: date) -> Tuple[int, int, int]:
    """
    Calculate 4-5-4 retail calendar.
    Returns: (retail_year, retail_period, retail_week)
    """
    # Simplified 4-5-4 calculation
    if d.month >= 2:
        retail_year = d.year
    else:
        retail_year = d.year - 1
    
    day_of_year = d.timetuple().tm_yday
    retail_week = (day_of_year - 1) // 7 + 1
    retail_period = (retail_week - 1) // 4 + 1
    
    return retail_year, retail_period, retail_week

def get_season(d: date) -> str:
    """Determine retail season."""
    month = d.month
    if month in [3, 4, 5]:
        return 'SPRING'
    elif month in [6, 7, 8]:
        return 'SUMMER'
    elif month in [9, 10, 11]:
        return 'FALL'
    else:
        return 'HOLIDAY'

def is_holiday(d: date) -> Tuple[bool, str]:
    """Check if date is a US retail holiday."""
    holidays = {
        (1, 1): 'New Year Day',
        (7, 4): 'Independence Day',
        (12, 25): 'Christmas Day',
        (12, 26): 'Boxing Day'
    }
    
    # Thanksgiving (4th Thursday of November)
    if d.month == 11 and d.weekday() == 3:
        first_thursday = 1 + (3 - date(d.year, 11, 1).weekday()) % 7
        fourth_thursday = first_thursday + 21
        if d.day == fourth_thursday:
            return True, 'Thanksgiving'
    
    # Black Friday
    if d.month == 11 and d.weekday() == 4:
        first_thursday = 1 + (3 - date(d.year, 11, 1).weekday()) % 7
        black_friday = first_thursday + 22
        if d.day == black_friday:
            return True, 'Black Friday'
    
    # Cyber Monday
    if d.month == 11 or d.month == 12:
        if d.weekday() == 0:
            first_thursday = 1 + (3 - date(d.year, 11, 1).weekday()) % 7
            cyber_monday = first_thursday + 25
            if d.month == 11 and d.day == cyber_monday:
                return True, 'Cyber Monday'
            if d.month == 12 and cyber_monday > 30 and d.day == cyber_monday - 30:
                return True, 'Cyber Monday'
    
    if (d.month, d.day) in holidays:
        return True, holidays[(d.month, d.day)]
    
    return False, None


# ============================================================
# DATA GENERATION FUNCTIONS
# ============================================================

def generate_dim_date() -> pd.DataFrame:
    """
    Generate dim_date table with full calendar hierarchy.
    """
    print("Generating dim_date...")
    
    dates = []
    current_date = START_DATE
    
    while current_date <= END_DATE:
        date_sk = date_to_sk(current_date)
        
        # Gregorian calendar
        day_of_week_num = current_date.isoweekday() % 7 + 1  # 1=Sun, 7=Sat
        day_of_week_name = current_date.strftime('%A')
        day_of_week_abbr = current_date.strftime('%a')
        day_of_month = current_date.day
        day_of_year = current_date.timetuple().tm_yday
        week_of_year = current_date.isocalendar()[1]
        iso_week_number = week_of_year
        month_num = current_date.month
        month_name = current_date.strftime('%B')
        month_abbr = current_date.strftime('%b')
        month_year = current_date.strftime('%b-%Y')
        quarter_num = (month_num - 1) // 3 + 1
        quarter_name = f"Q{quarter_num}-{current_date.year}"
        year_num = current_date.year
        
        # First/last days
        first_day_of_month = current_date.replace(day=1)
        if month_num == 12:
            last_day_of_month = current_date.replace(day=31)
        else:
            last_day_of_month = (current_date.replace(month=month_num + 1, day=1) - timedelta(days=1))
        
        first_day_of_quarter = date(year_num, (quarter_num - 1) * 3 + 1, 1)
        if quarter_num == 4:
            last_day_of_quarter = date(year_num, 12, 31)
        else:
            last_day_of_quarter = date(year_num, quarter_num * 3 + 1, 1) - timedelta(days=1)
        
        # Fiscal calendar
        fiscal_year, fiscal_quarter, fiscal_month, fiscal_quarter_name = get_fiscal_info(current_date)
        fiscal_week = (fiscal_month - 1) * 4 + (day_of_month - 1) // 7 + 1
        
        # Retail calendar
        retail_year, retail_period, retail_week = get_retail_calendar(current_date)
        
        # Flags
        is_weekday = current_date.weekday() < 5
        is_weekend = not is_weekday
        holiday_flag, holiday_name = is_holiday(current_date)
        is_black_friday = holiday_name == 'Black Friday'
        is_cyber_monday = holiday_name == 'Cyber Monday'
        is_back_to_school = month_num in [7, 8] and day_of_month >= 15
        season = get_season(current_date)
        
        dates.append({
            'date_sk': date_sk,
            'full_date': current_date,
            'day_of_week_num': day_of_week_num,
            'day_of_week_name': day_of_week_name,
            'day_of_week_abbr': day_of_week_abbr,
            'day_of_month': day_of_month,
            'day_of_year': day_of_year,
            'week_of_year': week_of_year,
            'iso_week_number': iso_week_number,
            'month_num': month_num,
            'month_name': month_name,
            'month_abbr': month_abbr,
            'month_year': month_year,
            'quarter_num': quarter_num,
            'quarter_name': quarter_name,
            'year_num': year_num,
            'first_day_of_month': first_day_of_month,
            'last_day_of_month': last_day_of_month,
            'first_day_of_quarter': first_day_of_quarter,
            'last_day_of_quarter': last_day_of_quarter,
            'fiscal_week_num': fiscal_week,
            'fiscal_month_num': fiscal_month,
            'fiscal_quarter_num': fiscal_quarter,
            'fiscal_year_num': fiscal_year,
            'fiscal_quarter_name': fiscal_quarter_name,
            'retail_period_num': retail_period,
            'retail_week_num': retail_week,
            'retail_year_num': retail_year,
            'is_weekday': is_weekday,
            'is_weekend': is_weekend,
            'is_holiday': holiday_flag,
            'holiday_name': holiday_name,
            'is_black_friday': is_black_friday,
            'is_cyber_monday': is_cyber_monday,
            'is_back_to_school': is_back_to_school,
            'season': season
        })
        
        current_date += timedelta(days=1)
    
    df = pd.DataFrame(dates)
    print(f"  Generated {len(df)} date records")
    return df


def generate_dim_geography() -> pd.DataFrame:
    """
    Generate dim_geography table with hierarchical geographic data.
    """
    print("Generating dim_geography...")
    
    geo_records = []
    geo_sk = 1
    parent_mapping = {}
    
    # Level 1: Regions
    for region_code, region_info in REGIONS.items():
        parent_mapping[region_code] = geo_sk
        geo_records.append({
            'geo_sk': geo_sk,
            'geo_level': 'REGION',
            'geo_code': region_code,
            'geo_name': region_info['name'],
            'parent_geo_sk': None,
            'world_region': region_code,
            'sub_region': None,
            'country_code': None,
            'country_name': None,
            'state_code': None,
            'state_name': None,
            'metro_market': None,
            'city': None,
            'population': np.random.randint(500000000, 2000000000),
            'gdp_per_capita_usd': round(np.random.uniform(15000, 60000), 2),
            'nike_market_priority': 'TIER_1'
        })
        geo_sk += 1
    
    # Level 2-6: Countries, States, Cities
    for region_code, region_info in REGIONS.items():
        region_sk = parent_mapping[region_code]
        
        for country_code in region_info['countries']:
            country_info = COUNTRY_INFO[country_code]
            country_sk = geo_sk
            parent_mapping[country_code] = country_sk
            
            # Country level
            geo_records.append({
                'geo_sk': geo_sk,
                'geo_level': 'COUNTRY',
                'geo_code': country_code,
                'geo_name': country_info['name'],
                'parent_geo_sk': region_sk,
                'world_region': region_code,
                'sub_region': 'NORTH_AMERICA' if country_code in ['US', 'CA', 'MX'] else region_code,
                'country_code': country_code,
                'country_name': country_info['name'],
                'state_code': None,
                'state_name': None,
                'metro_market': None,
                'city': None,
                'population': np.random.randint(10000000, 500000000),
                'gdp_per_capita_usd': round(np.random.uniform(10000, 80000), 2),
                'nike_market_priority': 'TIER_1' if country_code in ['US', 'CN', 'GB', 'DE', 'JP'] else 'TIER_2'
            })
            geo_sk += 1
            
            # State level
            for i, state_code in enumerate(country_info['states']):
                state_sk = geo_sk
                parent_mapping[f"{country_code}_{state_code}"] = state_sk
                
                geo_records.append({
                    'geo_sk': geo_sk,
                    'geo_level': 'STATE',
                    'geo_code': state_code,
                    'geo_name': state_code,
                    'parent_geo_sk': country_sk,
                    'world_region': region_code,
                    'sub_region': 'NORTH_AMERICA' if country_code in ['US', 'CA', 'MX'] else region_code,
                    'country_code': country_code,
                    'country_name': country_info['name'],
                    'state_code': state_code,
                    'state_name': state_code,
                    'metro_market': None,
                    'city': None,
                    'population': np.random.randint(1000000, 50000000),
                    'gdp_per_capita_usd': round(np.random.uniform(20000, 100000), 2),
                    'nike_market_priority': 'TIER_2'
                })
                geo_sk += 1
                
                # City level (if available)
                if i < len(country_info['cities']):
                    city = country_info['cities'][i]
                    geo_records.append({
                        'geo_sk': geo_sk,
                        'geo_level': 'CITY',
                        'geo_code': f"{state_code}_{city[:3].upper()}",
                        'geo_name': city,
                        'parent_geo_sk': state_sk,
                        'world_region': region_code,
                        'sub_region': 'NORTH_AMERICA' if country_code in ['US', 'CA', 'MX'] else region_code,
                        'country_code': country_code,
                        'country_name': country_info['name'],
                        'state_code': state_code,
                        'state_name': state_code,
                        'metro_market': city,
                        'city': city,
                        'population': np.random.randint(100000, 10000000),
                        'gdp_per_capita_usd': round(np.random.uniform(30000, 120000), 2),
                        'nike_market_priority': 'TIER_2' if country_code in ['US', 'CN'] else 'TIER_3'
                    })
                    geo_sk += 1
    
    df = pd.DataFrame(geo_records)
    print(f"  Generated {len(df)} geography records")
    return df


def generate_dim_channel() -> pd.DataFrame:
    """
    Generate dim_channel table with sales channel classification.
    """
    print("Generating dim_channel...")
    
    channels = [
        {'channel_sk': 1, 'channel_nk': 'NIKE_STORE', 'channel_name': 'Nike Retail Store',
         'channel_category': 'DIRECT', 'channel_type': 'STORE', 'is_digital': False, 
         'is_owned': True, 'platform_name': 'Nike Retail'},
        {'channel_sk': 2, 'channel_nk': 'NIKE_COM', 'channel_name': 'Nike.com',
         'channel_category': 'DIRECT', 'channel_type': 'WEB', 'is_digital': True,
         'is_owned': True, 'platform_name': 'Nike.com'},
        {'channel_sk': 3, 'channel_nk': 'NIKE_APP', 'channel_name': 'Nike App',
         'channel_category': 'DIRECT', 'channel_type': 'APP', 'is_digital': True,
         'is_owned': True, 'platform_name': 'Nike App'},
        {'channel_sk': 4, 'channel_nk': 'SNKRS_APP', 'channel_name': 'SNKRS App',
         'channel_category': 'DIRECT', 'channel_type': 'APP', 'is_digital': True,
         'is_owned': True, 'platform_name': 'SNKRS'},
        {'channel_sk': 5, 'channel_nk': 'FACTORY_STORE', 'channel_name': 'Nike Factory Store',
         'channel_category': 'DIRECT', 'channel_type': 'STORE', 'is_digital': False,
         'is_owned': True, 'platform_name': 'Nike Factory'},
        {'channel_sk': 6, 'channel_nk': 'FOOT_LOCKER', 'channel_name': 'Foot Locker',
         'channel_category': 'WHOLESALE', 'channel_type': 'STORE', 'is_digital': False,
         'is_owned': False, 'platform_name': 'Foot Locker'},
        {'channel_sk': 7, 'channel_nk': 'DICKS_SPORTING', 'channel_name': "Dick's Sporting Goods",
         'channel_category': 'WHOLESALE', 'channel_type': 'STORE', 'is_digital': False,
         'is_owned': False, 'platform_name': "Dick's"},
        {'channel_sk': 8, 'channel_nk': 'JD_SPORTS', 'channel_name': 'JD Sports',
         'channel_category': 'WHOLESALE', 'channel_type': 'STORE', 'is_digital': False,
         'is_owned': False, 'platform_name': 'JD Sports'},
        {'channel_sk': 9, 'channel_nk': 'AMAZON', 'channel_name': 'Amazon',
         'channel_category': 'MARKETPLACE', 'channel_type': 'WEB', 'is_digital': True,
         'is_owned': False, 'platform_name': 'Amazon'},
        {'channel_sk': 10, 'channel_nk': 'ZALANDO', 'channel_name': 'Zalando',
         'channel_category': 'MARKETPLACE', 'channel_type': 'WEB', 'is_digital': True,
         'is_owned': False, 'platform_name': 'Zalando'}
    ]
    
    df = pd.DataFrame(channels)
    print(f"  Generated {len(df)} channel records")
    return df


def generate_dim_store(dim_channel: pd.DataFrame) -> pd.DataFrame:
    """
    Generate dim_store table with store and channel information.
    """
    print("Generating dim_store...")
    
    stores = []
    store_sk = 1
    
    for region_code, region_info in REGIONS.items():
        for country_code in region_info['countries']:
            country_info = COUNTRY_INFO[country_code]
            
            # Number of stores per country based on market size
            if country_code == 'US':
                num_stores = 150
            elif country_code in ['CN', 'GB', 'DE', 'JP']:
                num_stores = 40
            else:
                num_stores = 20
            
            for i in range(num_stores):
                state_idx = i % len(country_info['states'])
                city_idx = i % len(country_info['cities'])
                
                state_code = country_info['states'][state_idx]
                city = country_info['cities'][city_idx]
                
                store_type = np.random.choice(STORE_TYPES, p=[0.1, 0.25, 0.1, 0.35, 0.2])
                is_digital = store_type == 'ECOM'
                channel_type = 'ECOMMERCE' if is_digital else np.random.choice(['BRICK_MORTAR', 'WHOLESALE'])
                
                store_format = 'DIGITAL' if is_digital else np.random.choice(['FULL_LINE', 'SPORT', 'RUN', 'BASKETBALL'])
                
                open_date = START_DATE - timedelta(days=np.random.randint(365, 3650))
                
                stores.append({
                    'store_sk': store_sk,
                    'store_nk': f"NKS-{country_code}-{store_sk:05d}",
                    'store_name': f"Nike {city} {'Online' if is_digital else store_format.replace('_', ' ').title()}",
                    'store_type': store_type,
                    'region_code': region_code,
                    'region_name': region_info['name'],
                    'country_code': country_code,
                    'country_name': country_info['name'],
                    'state_province_code': state_code,
                    'state_province_name': state_code,
                    'metro_market': city,
                    'city': city,
                    'store_address': f"{np.random.randint(100, 9999)} Main Street",
                    'postal_code': f"{np.random.randint(10000, 99999)}",
                    'latitude': round(np.random.uniform(25, 50), 7),
                    'longitude': round(np.random.uniform(-125, -70), 7),
                    'square_footage': np.random.randint(2000, 25000) if not is_digital else None,
                    'num_floors': np.random.choice([1, 2, 3]) if not is_digital else None,
                    'store_format': store_format,
                    'channel_type': channel_type,
                    'is_digital': is_digital,
                    'open_date_sk': date_to_sk(open_date),
                    'close_date_sk': None,
                    'is_active': True,
                    'store_manager_employee_sk': None,  # Will be updated later
                    'ownership_model': 'OWNED' if store_type in ['FLAGSHIP', 'FACTORY', 'ECOM'] else np.random.choice(['FRANCHISE', 'PARTNER']),
                    'partner_account_id': None if store_type in ['FLAGSHIP', 'FACTORY', 'ECOM'] else f"PTR-{np.random.randint(1000, 9999)}",
                    'created_timestamp': datetime.now()
                })
                store_sk += 1
                
                if store_sk > NUM_STORES:
                    break
            if store_sk > NUM_STORES:
                break
        if store_sk > NUM_STORES:
            break
    
    df = pd.DataFrame(stores)
    print(f"  Generated {len(df)} store records")
    return df


def generate_dim_employee(dim_store: pd.DataFrame) -> pd.DataFrame:
    """
    Generate dim_employee table with store associate details.
    """
    print("Generating dim_employee...")
    
    employees = []
    employee_sk = 1
    
    job_titles = ['Sales Associate', 'Senior Sales Associate', 'Floor Manager', 
                  'Assistant Store Manager', 'Store Manager', 'District Manager',
                  'Visual Merchandiser', 'Stock Associate', 'Cashier']
    departments = ['Sales', 'Operations', 'Management', 'Merchandising', 'Inventory']
    
    stores = dim_store[dim_store['is_digital'] == False]['store_sk'].tolist()
    
    for store_sk in stores:
        # Each store has 5-15 employees
        num_employees = np.random.randint(5, 16)
        
        for _ in range(num_employees):
            hire_date = START_DATE - timedelta(days=np.random.randint(30, 2000))
            is_active = np.random.random() > 0.05  # 95% active
            termination_date = None if is_active else (hire_date + timedelta(days=np.random.randint(180, 1500)))
            
            employees.append({
                'employee_sk': employee_sk,
                'employee_nk': f"EMP-{employee_sk:06d}",
                'full_name_hash': generate_hash(f"Employee_{employee_sk}"),
                'job_title': np.random.choice(job_titles),
                'department': np.random.choice(departments),
                'store_sk': store_sk,
                'hire_date_sk': date_to_sk(hire_date),
                'termination_date_sk': date_to_sk(termination_date) if termination_date else None,
                'is_active': is_active,
                'effective_date': hire_date,
                'expiry_date': date(9999, 12, 31),
                'is_current_flag': True
            })
            employee_sk += 1
            
            if employee_sk > NUM_EMPLOYEES:
                break
        if employee_sk > NUM_EMPLOYEES:
            break
    
    df = pd.DataFrame(employees)
    print(f"  Generated {len(df)} employee records")
    return df


def generate_dim_product() -> pd.DataFrame:
    """
    Generate dim_product table with full product hierarchy.
    """
    print("Generating dim_product...")
    
    products = []
    product_sk = 1
    
    for div_code, div_name in DIVISIONS.items():
        categories = CATEGORIES[div_code]
        
        for category in categories:
            subcategories = SUBCATEGORIES.get(category, ['Default'])
            
            for subcategory in subcategories:
                product_lines = PRODUCT_LINES.get(category, PRODUCT_LINES['Default'])
                
                for product_line in product_lines:
                    # Generate 3-8 products per product line
                    num_variants = np.random.randint(3, 9)
                    
                    for _ in range(num_variants):
                        colorway = np.random.choice(COLORWAYS)
                        
                        if div_code == 'FW':
                            sizes = SIZES_FOOTWEAR
                        elif div_code == 'AP':
                            sizes = SIZES_APPAREL
                        else:
                            sizes = SIZES_EQUIPMENT
                        
                        size = np.random.choice(sizes)
                        gender = np.random.choice(GENDER_TARGETS)
                        age_group = 'ADULT' if gender != 'KIDS' else np.random.choice(['YOUTH', 'TODDLER'])
                        
                        # Pricing based on division and product line
                        if div_code == 'FW':
                            base_retail = np.random.uniform(80, 250)
                        elif div_code == 'AP':
                            base_retail = np.random.uniform(30, 150)
                        else:
                            base_retail = np.random.uniform(15, 80)
                        
                        # Hero products cost more
                        is_hero = np.random.random() < 0.1
                        if is_hero:
                            base_retail *= 1.3
                        
                        standard_retail = round(base_retail, 2)
                        standard_cost = round(standard_retail * np.random.uniform(0.35, 0.50), 2)
                        
                        launch_date = START_DATE - timedelta(days=np.random.randint(0, 1500))
                        
                        products.append({
                            'product_sk': product_sk,
                            'product_nk': f"SKU-{div_code}-{product_sk:06d}",
                            'upc_code': f"{np.random.randint(100000000000, 999999999999, dtype=np.int64)}",
                            'division_code': div_code,
                            'division_name': div_name,
                            'category_code': category[:3].upper(),
                            'category_name': category,
                            'subcategory_code': subcategory[:3].upper(),
                            'subcategory_name': subcategory,
                            'product_line_code': product_line[:3].upper(),
                            'product_line_name': product_line,
                            'product_name': f"Nike {product_line} {colorway.split('/')[0]}",
                            'product_description': f"Nike {product_line} {category} {subcategory}",
                            'style_code': f"STY-{np.random.randint(100000, 999999)}",
                            'colorway': colorway,
                            'size': size,
                            'gender_target': gender,
                            'age_group': age_group,
                            'material_composition': 'Synthetic/Textile' if div_code != 'EQ' else 'Mixed Materials',
                            'technology_features': np.random.choice(['Air Cushioning', 'Flyknit', 'React Foam', 'ZoomX', 'Dri-FIT', None]),
                            'sport_occasion': category,
                            'is_hero_product': is_hero,
                            'is_exclusive': np.random.random() < 0.05,
                            'is_collaboration': np.random.random() < 0.03,
                            'collab_partner': np.random.choice(['Off-White', 'Travis Scott', 'Sacai', None]) if np.random.random() < 0.03 else None,
                            'standard_cost': standard_cost,
                            'standard_retail_price': standard_retail,
                            'launch_date_sk': date_to_sk(launch_date),
                            'discontinue_date_sk': None,
                            'is_active': True,
                            'effective_date': launch_date,
                            'expiry_date': date(9999, 12, 31),
                            'is_current_flag': True,
                            'created_timestamp': datetime.now(),
                            'updated_timestamp': datetime.now()
                        })
                        product_sk += 1
                        
                        if product_sk > NUM_PRODUCTS:
                            break
                    if product_sk > NUM_PRODUCTS:
                        break
                if product_sk > NUM_PRODUCTS:
                    break
            if product_sk > NUM_PRODUCTS:
                break
        if product_sk > NUM_PRODUCTS:
            break
    
    df = pd.DataFrame(products)
    print(f"  Generated {len(df)} product records")
    return df


def generate_dim_customer() -> pd.DataFrame:
    """
    Generate dim_customer table with SCD Type 2 support.
    """
    print("Generating dim_customer...")
    
    # customer_sk=0 is the sentinel for guest checkout / anonymous sessions
    customers = [{
        'customer_sk': 0,
        'customer_nk': 'GUEST-ANONYMOUS',
        'first_name': 'Guest',
        'last_name': 'Customer',
        'email_address_hash': generate_hash('guest@unknown.com'),
        'phone_hash': generate_hash('unknown'),
        'date_of_birth_sk': None,
        'gender_code': 'U',
        'loyalty_member_flag': False,
        'loyalty_tier': None,
        'loyalty_points_balance': 0,
        'loyalty_enroll_date_sk': None,
        'customer_segment': 'Guest',
        'preferred_sport': None,
        'lifetime_value_band': 'Unknown',
        'acquisition_channel': 'Unknown',
        'city': None,
        'state_province': None,
        'country_code': None,
        'postal_code': None,
        'effective_date': START_DATE,
        'expiry_date': date(9999, 12, 31),
        'is_current_flag': True,
        'record_checksum': generate_hash('guest_customer_0'),
        'created_timestamp': datetime.now(),
        'updated_timestamp': datetime.now()
    }]

    for customer_sk in range(1, NUM_CUSTOMERS + 1):
        # Select random country/region
        region_code = np.random.choice(list(REGIONS.keys()), p=[0.45, 0.30, 0.25])
        country_code = np.random.choice(REGIONS[region_code]['countries'])
        country_info = COUNTRY_INFO[country_code]
        
        # Loyalty attributes
        loyalty_member = np.random.random() < 0.65  # 65% are loyalty members
        loyalty_tier = np.random.choice(LOYALTY_TIERS, p=[0.50, 0.30, 0.15, 0.05]) if loyalty_member else None
        loyalty_enroll_date = START_DATE - timedelta(days=np.random.randint(30, 1500)) if loyalty_member else None
        
        # Customer segmentation - weighted towards realistic distribution
        segment_probs = [0.25, 0.20, 0.30, 0.15, 0.05, 0.05]
        customer_segment = np.random.choice(CUSTOMER_SEGMENTS, p=segment_probs)
        
        # LTV band correlated with loyalty tier
        if loyalty_tier == 'Elite':
            ltv_band = np.random.choice(LTV_BANDS, p=[0.0, 0.1, 0.3, 0.6])
        elif loyalty_tier == 'Gold':
            ltv_band = np.random.choice(LTV_BANDS, p=[0.05, 0.25, 0.50, 0.20])
        elif loyalty_tier == 'Silver':
            ltv_band = np.random.choice(LTV_BANDS, p=[0.15, 0.45, 0.30, 0.10])
        else:
            ltv_band = np.random.choice(LTV_BANDS, p=[0.40, 0.40, 0.15, 0.05])
        
        dob = date(np.random.randint(1960, 2005), np.random.randint(1, 13), np.random.randint(1, 28))
        
        customers.append({
            'customer_sk': customer_sk,
            'customer_nk': f"CRM-{generate_uuid()[:8].upper()}",
            'first_name': f"FirstName{customer_sk}",  # Hashed in real scenario
            'last_name': f"LastName{customer_sk}",
            'email_address_hash': generate_hash(f"customer{customer_sk}@email.com"),
            'phone_hash': generate_hash(f"+1{np.random.randint(1000000000, 9999999999, dtype=np.int64)}"),
            'date_of_birth_sk': date_to_sk(dob),
            'gender_code': np.random.choice(['M', 'F', 'N', 'U'], p=[0.48, 0.48, 0.02, 0.02]),
            'loyalty_member_flag': loyalty_member,
            'loyalty_tier': loyalty_tier,
            'loyalty_points_balance': np.random.randint(0, 50000) if loyalty_member else 0,
            'loyalty_enroll_date_sk': date_to_sk(loyalty_enroll_date) if loyalty_enroll_date else None,
            'customer_segment': customer_segment,
            'preferred_sport': np.random.choice(PREFERRED_SPORTS),
            'lifetime_value_band': ltv_band,
            'acquisition_channel': np.random.choice(ACQUISITION_CHANNELS),
            'city': np.random.choice(country_info['cities']),
            'state_province': np.random.choice(country_info['states']),
            'country_code': country_code,
            'postal_code': f"{np.random.randint(10000, 99999)}",
            'effective_date': START_DATE - timedelta(days=np.random.randint(0, 730)),
            'expiry_date': date(9999, 12, 31),
            'is_current_flag': True,
            'record_checksum': generate_hash(f"customer_{customer_sk}_{datetime.now()}"),
            'created_timestamp': datetime.now(),
            'updated_timestamp': datetime.now()
        })
    
    df = pd.DataFrame(customers)
    print(f"  Generated {len(df)} customer records")
    return df


def generate_dim_promotion(dim_date: pd.DataFrame) -> pd.DataFrame:
    """
    Generate dim_promotion table with campaign and discount metadata.
    """
    print("Generating dim_promotion...")
    
    promotions = []
    date_sks = dim_date['date_sk'].tolist()
    
    for promo_sk in range(1, NUM_PROMOTIONS + 1):
        promo_type = np.random.choice(PROMOTION_TYPES)
        discount_type = np.random.choice(DISCOUNT_TYPES)
        
        # Set discount value based on type
        if discount_type == 'PERCENT':
            discount_value = np.random.choice([10, 15, 20, 25, 30, 40, 50])
        elif discount_type == 'FIXED_AMOUNT':
            discount_value = np.random.choice([5, 10, 15, 20, 25, 50])
        else:
            discount_value = 0
        
        # Promotion duration
        start_date_sk = np.random.choice(date_sks)
        duration_days = np.random.randint(7, 45)
        start_d = datetime.strptime(str(start_date_sk), '%Y%m%d').date()
        end_d = min(start_d + timedelta(days=duration_days), END_DATE)
        end_date_sk = date_to_sk(end_d)
        
        campaign_category = np.random.choice(CAMPAIGN_CATEGORIES)
        
        # Estimated vs actual cost
        estimated_cost = round(np.random.uniform(10000, 500000), 2)
        actual_cost = round(estimated_cost * np.random.uniform(0.85, 1.15), 2)
        
        promotions.append({
            'promotion_sk': promo_sk,
            'promotion_nk': f"PROMO-{promo_sk:05d}",
            'promotion_name': f"{campaign_category.replace('_', ' ').title()} {promo_type.title()} {promo_sk}",
            'promotion_type': promo_type,
            'discount_type': discount_type,
            'discount_value': discount_value,
            'min_purchase_amount': np.random.choice([0, 50, 75, 100, 150]) if promo_type != 'MARKDOWN' else 0,
            'max_discount_cap': np.random.choice([None, 50, 100, 200]) if discount_type == 'PERCENT' else None,
            'campaign_name': f"{campaign_category} Campaign {promo_sk // 10 + 1}",
            'campaign_category': campaign_category,
            'marketing_channel': np.random.choice(MARKETING_CHANNELS),
            'start_date_sk': start_date_sk,
            'end_date_sk': end_date_sk,
            'applicable_product_category': np.random.choice(['Running', 'Basketball', 'Lifestyle', 'All', 'Training']),
            'applicable_store_region': np.random.choice(['AMER', 'EMEA', 'APAC', 'Global']),
            'applicable_channel': np.random.choice(['All', 'DIRECT', 'DIGITAL', 'STORE']),
            'stackable_flag': np.random.random() < 0.2,
            'estimated_cost': estimated_cost,
            'actual_cost': actual_cost,
            'funded_by': np.random.choice(['NIKE', 'RETAILER', 'CO_FUND']),
            'promotion_status': 'ACTIVE' if end_date_sk >= date_to_sk(END_DATE) else 'COMPLETED',
            'created_timestamp': datetime.now(),
            'updated_timestamp': datetime.now()
        })
    
    df = pd.DataFrame(promotions)
    print(f"  Generated {len(df)} promotion records")
    return df


def generate_fact_sales_transactions(
    dim_date: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_store: pd.DataFrame,
    dim_channel: pd.DataFrame,
    dim_promotion: pd.DataFrame,
    dim_employee: pd.DataFrame
) -> pd.DataFrame:
    """
    Generate fact_sales_transactions table - core revenue fact table.
    """
    print("Generating fact_sales_transactions...")
    
    # Get valid keys
    date_sks = dim_date['date_sk'].tolist()
    customer_sks = dim_customer[dim_customer['customer_sk'] > 0]['customer_sk'].tolist()  # exclude guest sentinel
    product_data = dim_product[['product_sk', 'standard_retail_price', 'standard_cost', 'division_code', 'category_name']].to_dict('records')
    store_data = dim_store[['store_sk', 'region_code', 'is_digital', 'channel_type']].to_dict('records')
    promotion_sks = [None] + dim_promotion['promotion_sk'].tolist()  # None for no promotion
    store_employee_map = dim_employee[dim_employee['is_active']].groupby('store_sk')['employee_sk'].apply(list).to_dict()
    
    # Promotion lookup for discount calculation
    promo_lookup = dim_promotion.set_index('promotion_sk')[['discount_type', 'discount_value', 'start_date_sk', 'end_date_sk']].to_dict('index')
    
    transactions = []
    transaction_sk = 1
    
    # Create realistic seasonality weights
    date_weights = []
    for date_sk in date_sks:
        date_row = dim_date[dim_date['date_sk'] == date_sk].iloc[0]
        weight = 1.0
        
        # Higher sales in Q4 (holiday season)
        if date_row['quarter_num'] == 4:
            weight *= 1.5
        
        # Black Friday / Cyber Monday boost
        if date_row['is_black_friday'] or date_row['is_cyber_monday']:
            weight *= 3.0
        
        # Weekend boost
        if date_row['is_weekend']:
            weight *= 1.2
        
        # Back to school boost
        if date_row['is_back_to_school']:
            weight *= 1.3
        
        date_weights.append(weight)
    
    date_weights = np.array(date_weights)
    date_weights = date_weights / date_weights.sum()
    
    # Generate transactions in batches
    num_unique_transactions = NUM_TRANSACTIONS // 3  # Average 3 line items per transaction
    
    for _ in range(num_unique_transactions):
        transaction_id = generate_uuid()
        transaction_date_sk = np.random.choice(date_sks, p=date_weights)
        
        # Select store with regional weighting (AMER gets more transactions)
        store = random.choice(store_data)
        store_sk = store['store_sk']
        
        # Select appropriate channel consistent with the store's channel_type
        if store['is_digital']:
            channel_sk = np.random.choice([2, 3, 4])        # Nike.com, Nike App, SNKRS
        elif store['channel_type'] == 'WHOLESALE':
            channel_sk = np.random.choice([6, 7, 8])        # Foot Locker, Dick's, JD Sports
        else:                                                # BRICK_MORTAR
            channel_sk = np.random.choice([1, 5])           # Nike Store, Factory Store
        
        # Customer selection (20% guest checkout → sentinel 0, 80% known customer)
        customer_sk = int(np.random.choice(customer_sks)) if np.random.random() > 0.2 else 0
        
        # Employee (only for physical stores, picked from that store's staff)
        if not store['is_digital']:
            store_emps = store_employee_map.get(store_sk, [])
            employee_sk = random.choice(store_emps) if store_emps else None
        else:
            employee_sk = None
        
        # Promotion selection (30% of transactions have promotions)
        promotion_sk = np.random.choice(promotion_sks) if np.random.random() < 0.3 else None
        
        # Validate promotion is active for transaction date
        if promotion_sk and promotion_sk in promo_lookup:
            promo = promo_lookup[promotion_sk]
            if not (promo['start_date_sk'] <= transaction_date_sk <= promo['end_date_sk']):
                promotion_sk = None
        
        # Generate 1-5 line items per transaction
        num_lines = np.random.choice([1, 2, 3, 4, 5], p=[0.35, 0.30, 0.20, 0.10, 0.05])
        
        for line_num in range(1, num_lines + 1):
            product = random.choice(product_data)
            product_sk = product['product_sk']
            
            quantity = np.random.choice([1, 2, 3, 4], p=[0.70, 0.20, 0.07, 0.03])
            
            # Get pricing
            unit_retail = product['standard_retail_price']
            unit_cost = product['standard_cost']
            
            gross_revenue = round(quantity * unit_retail, 4)
            
            # Calculate discount
            discount_amount = 0
            if promotion_sk and promotion_sk in promo_lookup:
                promo = promo_lookup[promotion_sk]
                if promo['discount_type'] == 'PERCENT':
                    discount_amount = round(gross_revenue * (promo['discount_value'] / 100), 4)
                elif promo['discount_type'] == 'FIXED_AMOUNT':
                    discount_amount = min(promo['discount_value'], gross_revenue)
            
            net_revenue = round(gross_revenue - discount_amount, 4)
            tax_rate = np.random.choice([0.0, 0.05, 0.0725, 0.08, 0.10])
            tax_amount = round(net_revenue * tax_rate, 4)
            total_amount = round(net_revenue + tax_amount, 4)
            
            cogs = round(quantity * unit_cost, 4)
            gross_margin = round(net_revenue - cogs, 4)
            gross_margin_pct = round((gross_margin / net_revenue * 100) if net_revenue > 0 else 0, 4)
            
            # Currency handling
            currency_code = 'USD'
            exchange_rate = 1.0
            if store['region_code'] == 'EMEA':
                currency_code = np.random.choice(['EUR', 'GBP', 'USD'])
                exchange_rate = {'EUR': 1.08, 'GBP': 1.27, 'USD': 1.0}[currency_code]
            elif store['region_code'] == 'APAC':
                currency_code = np.random.choice(['CNY', 'JPY', 'USD'])
                exchange_rate = {'CNY': 0.14, 'JPY': 0.0067, 'USD': 1.0}[currency_code]
            
            net_revenue_usd = round(net_revenue * exchange_rate, 4)
            
            transactions.append({
                'transaction_sk': transaction_sk,
                'transaction_id': transaction_id,
                'line_number': line_num,
                'transaction_date_sk': transaction_date_sk,
                'customer_sk': customer_sk,
                'product_sk': product_sk,
                'store_sk': store_sk,
                'channel_sk': channel_sk,
                'promotion_sk': promotion_sk,
                'employee_sk': employee_sk,
                'quantity_sold': quantity,
                'unit_retail_price': unit_retail,
                'unit_cost_price': unit_cost,
                'gross_revenue': gross_revenue,
                'discount_amount': discount_amount,
                'net_revenue': net_revenue,
                'tax_amount': tax_amount,
                'total_transaction_amount': total_amount,
                'cost_of_goods_sold': cogs,
                'gross_margin': gross_margin,
                'gross_margin_pct': gross_margin_pct,
                'return_flag': 'N',
                'currency_code': currency_code,
                'exchange_rate': exchange_rate,
                'net_revenue_usd': net_revenue_usd,
                'load_timestamp': datetime.now(),
                'source_system': 'POS_SYSTEM' if not store['is_digital'] else 'ECOM_PLATFORM'
            })
            transaction_sk += 1
        
        if transaction_sk > NUM_TRANSACTIONS:
            break
    
    df = pd.DataFrame(transactions)
    print(f"  Generated {len(df)} sales transaction records")
    return df


def generate_fact_inventory_snapshot(
    dim_date: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_store: pd.DataFrame
) -> pd.DataFrame:
    """
    Generate fact_inventory_snapshot table - daily inventory levels.
    """
    print("Generating fact_inventory_snapshot...")
    
    # Sample subset for reasonable data size
    sample_products = dim_product.sample(min(500, len(dim_product)))['product_sk'].tolist()
    physical_stores = dim_store[dim_store['is_digital'] == False]
    sample_stores = physical_stores.sample(min(100, len(physical_stores)))['store_sk'].tolist()
    
    # Get every 7th date for weekly snapshots (to reduce data volume)
    date_sks = dim_date['date_sk'].tolist()[::7]
    
    product_costs = dim_product.set_index('product_sk')[['standard_cost', 'standard_retail_price']].to_dict('index')
    
    inventory_records = []
    inventory_sk = 1
    
    for date_sk in date_sks:
        for store_sk in sample_stores:
            for product_sk in sample_products:
                # Base inventory levels with some randomness
                on_hand = np.random.randint(0, 150)
                in_transit = np.random.randint(0, 30) if on_hand < 50 else 0
                on_order = np.random.randint(0, 50) if on_hand < 30 else 0
                reserved = min(np.random.randint(0, 10), on_hand)
                available = max(0, on_hand - reserved)
                
                # Daily flow measures
                units_received = np.random.randint(0, 20) if in_transit > 0 else 0
                units_sold = np.random.randint(0, min(15, available) + 1) if available > 0 else 0
                units_returned = np.random.randint(0, 3) if units_sold > 0 else 0
                units_shrinkage = np.random.randint(0, 2) if np.random.random() < 0.05 else 0
                
                # Valuation
                product_info = product_costs.get(product_sk, {'standard_cost': 50, 'standard_retail_price': 100})
                cost_value = round(on_hand * product_info['standard_cost'], 4)
                retail_value = round(on_hand * product_info['standard_retail_price'], 4)
                
                # Calculate metrics
                avg_daily_sales = 2.5  # Assumed average
                days_of_supply = round(on_hand / avg_daily_sales, 2) if avg_daily_sales > 0 else 999
                stock_turn_rate = round(365 / days_of_supply, 4) if days_of_supply > 0 else 0
                
                stockout_flag = 'Y' if available == 0 else 'N'
                reorder_point = 20  # Standard reorder point
                
                inventory_records.append({
                    'inventory_sk': inventory_sk,
                    'snapshot_date_sk': date_sk,
                    'product_sk': product_sk,
                    'store_sk': store_sk,
                    'on_hand_qty': on_hand,
                    'in_transit_qty': in_transit,
                    'on_order_qty': on_order,
                    'reserved_qty': reserved,
                    'available_to_sell_qty': available,
                    'units_received_today': units_received,
                    'units_sold_today': units_sold,
                    'units_returned_today': units_returned,
                    'units_shrinkage_today': units_shrinkage,
                    'inventory_cost_value': cost_value,
                    'inventory_retail_value': retail_value,
                    'reorder_point': reorder_point,
                    'days_of_supply': days_of_supply,
                    'stock_turn_rate': stock_turn_rate,
                    'stockout_flag': stockout_flag,
                    'load_timestamp': datetime.now()
                })
                inventory_sk += 1
    
    df = pd.DataFrame(inventory_records)
    print(f"  Generated {len(df)} inventory snapshot records")
    return df


def generate_fact_returns(
    fact_sales: pd.DataFrame,
    dim_date: pd.DataFrame
) -> pd.DataFrame:
    """
    Generate fact_returns table - product returns and refunds.
    """
    print("Generating fact_returns...")
    
    # Select ~8% of transactions for returns (realistic return rate)
    return_candidates = fact_sales.sample(frac=0.08)
    
    returns = []
    return_sk = 1
    
    for _, sale in return_candidates.iterrows():
        # Return happens 5-30 days after purchase
        return_delay = np.random.randint(5, 31)
        sale_date = dim_date[dim_date['date_sk'] == sale['transaction_date_sk']]['full_date'].iloc[0]
        return_date = sale_date + timedelta(days=return_delay)
        
        if return_date > END_DATE:
            continue
        
        return_date_sk = date_to_sk(return_date)
        
        # Return reason
        reason_code = np.random.choice(list(RETURN_REASONS.keys()), 
                                        p=[0.30, 0.25, 0.10, 0.08, 0.07, 0.10, 0.05, 0.05])
        
        # Return condition based on reason
        if reason_code in ['DEFECT', 'DAMAGED']:
            condition = np.random.choice(RETURN_CONDITIONS, p=[0.1, 0.4, 0.5])
        else:
            condition = np.random.choice(RETURN_CONDITIONS, p=[0.85, 0.10, 0.05])
        
        quantity_returned = min(sale['quantity_sold'], np.random.randint(1, sale['quantity_sold'] + 1))
        
        # Calculate return amounts
        return_retail = round(quantity_returned * sale['unit_retail_price'], 4)
        return_cost = round(quantity_returned * sale['unit_cost_price'], 4)
        
        # Refund may be less than purchase if restocking fee applies
        restocking_fee = 0
        if condition != 'SALEABLE' or reason_code == 'CHANGED_MIND':
            restocking_fee = round(return_retail * 0.15, 4)  # 15% restocking fee
        
        refund_amount = round(return_retail - restocking_fee, 4)
        return_shipping = round(np.random.uniform(0, 12), 4) if np.random.random() < 0.3 else 0
        
        within_window = return_delay <= 30
        
        returns.append({
            'return_sk': return_sk,
            'return_id': f"RET-{generate_uuid()[:8].upper()}",
            'return_line_number': 1,
            'return_date_sk': return_date_sk,
            'original_transaction_sk': sale['transaction_sk'],
            'original_transaction_date_sk': sale['transaction_date_sk'],
            'customer_sk': sale['customer_sk'],
            'product_sk': sale['product_sk'],
            'store_sk': sale['store_sk'],
            'channel_sk': sale['channel_sk'],
            'return_reason_code': reason_code,
            'return_reason_desc': RETURN_REASONS[reason_code],
            'return_condition': condition,
            'quantity_returned': quantity_returned,
            'return_retail_amount': return_retail,
            'return_cost_amount': return_cost,
            'refund_amount': refund_amount,
            'restocking_fee': restocking_fee,
            'return_shipping_cost': return_shipping,
            'days_since_purchase': return_delay,
            'is_within_return_window': within_window,
            'load_timestamp': datetime.now()
        })
        return_sk += 1
    
    df = pd.DataFrame(returns)
    print(f"  Generated {len(df)} return records")
    return df


def generate_fact_web_sessions(
    dim_date: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_channel: pd.DataFrame,
    fact_sales: pd.DataFrame
) -> pd.DataFrame:
    """
    Generate fact_web_sessions table - digital analytics data.
    """
    print("Generating fact_web_sessions...")
    
    # Get digital sales for conversion tracking
    digital_sales = fact_sales[fact_sales['channel_sk'].isin([2, 3, 4])]
    digital_sales_by_date = digital_sales.groupby('transaction_date_sk')['transaction_sk'].apply(list).to_dict()
    
    date_sks = dim_date['date_sk'].tolist()
    customer_sks = dim_customer[dim_customer['customer_sk'] > 0]['customer_sk'].tolist()  # exclude guest sentinel
    digital_channel_sks = [2, 3, 4]  # Nike.com, Nike App, SNKRS
    
    sessions = []
    session_sk = 1
    
    # Calculate sessions per day based on realistic web traffic patterns
    sessions_per_day = NUM_WEB_SESSIONS // len(date_sks)
    
    for date_sk in date_sks:
        # Daily session volume varies
        daily_sessions = int(sessions_per_day * np.random.uniform(0.7, 1.3))
        
        # Get purchases for this date for conversion attribution
        daily_purchases = digital_sales_by_date.get(date_sk, [])
        purchase_idx = 0
        
        for _ in range(daily_sessions):
            session_id = generate_uuid()
            
            # 40% of sessions are from known customers, 60% anonymous → sentinel 0
            customer_sk = int(np.random.choice(customer_sks)) if np.random.random() < 0.4 else 0
            
            channel_sk = np.random.choice(digital_channel_sks, p=[0.50, 0.35, 0.15])
            
            device_type = np.random.choice(DEVICE_TYPES, p=[0.25, 0.55, 0.10, 0.10])
            
            # OS based on device
            if device_type == 'APP':
                os_platform = np.random.choice(['iOS', 'Android'], p=[0.55, 0.45])
            elif device_type == 'MOBILE':
                os_platform = np.random.choice(['iOS', 'Android'], p=[0.50, 0.50])
            elif device_type == 'TABLET':
                os_platform = np.random.choice(['iOS', 'Android'], p=[0.70, 0.30])
            else:
                os_platform = np.random.choice(['Windows', 'MacOS', 'Linux'], p=[0.65, 0.30, 0.05])
            
            browser = np.random.choice(BROWSERS, p=[0.60, 0.20, 0.10, 0.07, 0.03]) if device_type != 'APP' else None
            
            traffic_source = np.random.choice(TRAFFIC_SOURCES, p=[0.25, 0.20, 0.15, 0.15, 0.15, 0.05, 0.05])
            
            # Session behavior
            bounce = np.random.random() < 0.35  # 35% bounce rate
            
            if bounce:
                session_duration = np.random.randint(5, 30)
                pages_viewed = 1
                products_viewed = 0
                products_added = 0
                cart_value = 0
                checkout_initiated = False
                purchase_completed = False
                purchase_sk = None
                purchase_revenue = None
            else:
                session_duration = np.random.randint(60, 1200)  # 1-20 minutes
                pages_viewed = np.random.randint(2, 15)
                products_viewed = np.random.randint(1, 10)
                
                # Funnel conversion
                add_to_cart = np.random.random() < 0.25  # 25% add to cart
                products_added = np.random.randint(1, 4) if add_to_cart else 0
                cart_value = round(products_added * np.random.uniform(50, 200), 4) if add_to_cart else 0
                
                checkout_initiated = add_to_cart and np.random.random() < 0.6  # 60% proceed to checkout
                
                # 70% of checkouts complete (overall ~2.5% conversion rate)
                purchase_completed = checkout_initiated and np.random.random() < 0.7
                
                if purchase_completed and purchase_idx < len(daily_purchases):
                    purchase_sk = daily_purchases[purchase_idx]
                    purchase_revenue = cart_value
                    purchase_idx += 1
                else:
                    purchase_completed = False  # No transaction to link — flag must match
                    purchase_sk = None
                    purchase_revenue = None
            
            entry_page = np.random.choice(['/home', '/products', '/sale', '/new-releases', '/collections/running'])
            exit_page = np.random.choice(['/checkout/complete', '/cart', '/products', '/home', '/404']) if not bounce else entry_page
            
            campaign_id = f"CMP-{np.random.randint(1000, 9999)}" if traffic_source in ['PAID', 'EMAIL', 'SOCIAL'] else None
            
            sessions.append({
                'session_sk': session_sk,
                'session_id': session_id,
                'session_date_sk': date_sk,
                'customer_sk': customer_sk,
                'channel_sk': channel_sk,
                'device_type': device_type,
                'os_platform': os_platform,
                'browser_name': browser,
                'entry_page': entry_page,
                'exit_page': exit_page,
                'traffic_source': traffic_source,
                'campaign_id': campaign_id,
                'session_duration_seconds': session_duration,
                'pages_viewed': pages_viewed,
                'products_viewed': products_viewed,
                'products_added_to_cart': products_added,
                'cart_value': cart_value,
                'checkout_initiated_flag': checkout_initiated,
                'purchase_completed_flag': purchase_completed,
                'purchase_transaction_sk': purchase_sk,
                'purchase_revenue': purchase_revenue,
                'bounce_flag': bounce,
                'load_timestamp': datetime.now()
            })
            session_sk += 1
            
            if session_sk > NUM_WEB_SESSIONS:
                break
        
        if session_sk > NUM_WEB_SESSIONS:
            break
    
    df = pd.DataFrame(sessions)
    print(f"  Generated {len(df)} web session records")
    return df


# ============================================================
# MAIN EXECUTION
# ============================================================

def save_to_csv(dataframes: Dict[str, pd.DataFrame], output_dir: str):
    """Save all dataframes to CSV files."""
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\nSaving CSV files to {output_dir}/")
    for table_name, df in dataframes.items():
        filepath = os.path.join(output_dir, f"{table_name}.csv")
        df.to_csv(filepath, index=False)
        print(f"  Saved {table_name}.csv ({len(df):,} rows)")


def validate_data_consistency(dataframes: Dict[str, pd.DataFrame]):
    """Validate PK/FK relationships and data consistency."""
    print("\n" + "="*60)
    print("DATA CONSISTENCY VALIDATION")
    print("="*60)
    
    # Check FK relationships
    validations = [
        ('fact_sales_transactions', 'transaction_date_sk', 'dim_date', 'date_sk'),
        ('fact_sales_transactions', 'product_sk', 'dim_product', 'product_sk'),
        ('fact_sales_transactions', 'store_sk', 'dim_store', 'store_sk'),
        ('fact_sales_transactions', 'channel_sk', 'dim_channel', 'channel_sk'),
        ('fact_sales_transactions', 'customer_sk', 'dim_customer', 'customer_sk'),      # nullable: guest checkout
        ('fact_sales_transactions', 'employee_sk', 'dim_employee', 'employee_sk'),      # nullable: digital orders
        ('fact_sales_transactions', 'promotion_sk', 'dim_promotion', 'promotion_sk'),   # nullable: no promo
        ('fact_inventory_snapshot', 'snapshot_date_sk', 'dim_date', 'date_sk'),
        ('fact_inventory_snapshot', 'product_sk', 'dim_product', 'product_sk'),
        ('fact_inventory_snapshot', 'store_sk', 'dim_store', 'store_sk'),
        ('fact_returns', 'return_date_sk', 'dim_date', 'date_sk'),
        ('fact_returns', 'product_sk', 'dim_product', 'product_sk'),
        ('fact_returns', 'store_sk', 'dim_store', 'store_sk'),
        ('fact_returns', 'channel_sk', 'dim_channel', 'channel_sk'),
        ('fact_returns', 'customer_sk', 'dim_customer', 'customer_sk'),                 # nullable: guest checkout
        ('fact_web_sessions', 'session_date_sk', 'dim_date', 'date_sk'),
        ('fact_web_sessions', 'channel_sk', 'dim_channel', 'channel_sk'),
        ('fact_web_sessions', 'customer_sk', 'dim_customer', 'customer_sk'),            # nullable: anonymous sessions
    ]
    
    all_valid = True
    for fact_table, fk_col, dim_table, pk_col in validations:
        if fact_table in dataframes and dim_table in dataframes:
            fact_keys = set(dataframes[fact_table][fk_col].dropna().unique())
            dim_keys = set(dataframes[dim_table][pk_col].unique())
            
            orphans = fact_keys - dim_keys
            if orphans:
                print(f"  ❌ {fact_table}.{fk_col} has {len(orphans)} orphan keys not in {dim_table}.{pk_col}")
                all_valid = False
            else:
                print(f"  ✓ {fact_table}.{fk_col} -> {dim_table}.{pk_col}")
    
    if all_valid:
        print("\n✅ All FK relationships validated successfully!")
    
    return all_valid


def print_kpi_summary(dataframes: Dict[str, pd.DataFrame]):
    """Print summary KPIs to verify data quality."""
    print("\n" + "="*60)
    print("KPI SUMMARY (Data Quality Check)")
    print("="*60)
    
    fact_sales = dataframes['fact_sales_transactions']
    fact_returns = dataframes['fact_returns']
    fact_sessions = dataframes['fact_web_sessions']
    
    # Revenue KPIs
    total_gross_revenue = fact_sales['gross_revenue'].sum()
    total_net_revenue = fact_sales['net_revenue'].sum()
    total_discount = fact_sales['discount_amount'].sum()
    total_cogs = fact_sales['cost_of_goods_sold'].sum()
    total_margin = fact_sales['gross_margin'].sum()
    
    print(f"\n📊 REVENUE METRICS (Additive)")
    print(f"  Total Gross Revenue:    ${total_gross_revenue:,.2f}")
    print(f"  Total Discounts:        ${total_discount:,.2f}")
    print(f"  Total Net Revenue:      ${total_net_revenue:,.2f}")
    print(f"  Total COGS:             ${total_cogs:,.2f}")
    print(f"  Total Gross Margin:     ${total_margin:,.2f}")
    
    # Non-additive metrics (computed from aggregates)
    gm_pct = (total_margin / total_net_revenue * 100) if total_net_revenue > 0 else 0
    discount_rate = (total_discount / total_gross_revenue * 100) if total_gross_revenue > 0 else 0
    
    num_transactions = fact_sales['transaction_id'].nunique()
    aov = total_net_revenue / num_transactions if num_transactions > 0 else 0
    
    print(f"\n📈 PROFITABILITY METRICS (Non-Additive)")
    print(f"  Gross Margin %:         {gm_pct:.2f}%")
    print(f"  Discount Rate %:        {discount_rate:.2f}%")
    print(f"  Avg Order Value:        ${aov:.2f}")
    print(f"  Total Transactions:     {num_transactions:,}")
    
    # Returns metrics
    total_returns = len(fact_returns)
    total_refunds = fact_returns['refund_amount'].sum()
    return_rate = (total_returns / len(fact_sales) * 100)
    
    print(f"\n🔄 RETURNS METRICS")
    print(f"  Total Return Lines:     {total_returns:,}")
    print(f"  Total Refunds:          ${total_refunds:,.2f}")
    print(f"  Return Rate:            {return_rate:.2f}%")
    
    # Digital metrics
    total_sessions = len(fact_sessions)
    bounced = fact_sessions['bounce_flag'].sum()
    purchases = fact_sessions['purchase_completed_flag'].sum()
    bounce_rate = (bounced / total_sessions * 100) if total_sessions > 0 else 0
    conversion_rate = (purchases / total_sessions * 100) if total_sessions > 0 else 0
    
    print(f"\n💻 DIGITAL METRICS")
    print(f"  Total Web Sessions:     {total_sessions:,}")
    print(f"  Bounce Rate:            {bounce_rate:.2f}%")
    print(f"  Conversion Rate:        {conversion_rate:.2f}%")
    
    # Inventory snapshot
    fact_inv = dataframes['fact_inventory_snapshot']
    latest_date = fact_inv['snapshot_date_sk'].max()
    latest_inv = fact_inv[fact_inv['snapshot_date_sk'] == latest_date]
    
    total_on_hand = latest_inv['on_hand_qty'].sum()
    stockout_count = (latest_inv['stockout_flag'] == 'Y').sum()
    stockout_rate = (stockout_count / len(latest_inv) * 100)
    
    print(f"\n📦 INVENTORY METRICS (Latest Snapshot)")
    print(f"  Total On-Hand Units:    {total_on_hand:,}")
    print(f"  Stockout SKU Count:     {stockout_count:,}")
    print(f"  Stockout Rate:          {stockout_rate:.2f}%")


def main():
    """Main function to orchestrate data generation."""
    print("="*60)
    print("NIKE GLOBAL RETAIL - SAMPLE DATA GENERATOR")
    print("="*60)
    print(f"Start Date: {START_DATE}")
    print(f"End Date: {END_DATE}")
    print(f"Output Directory: {OUTPUT_DIR}")
    print("="*60 + "\n")
    
    # Generate dimension tables first (they have no dependencies)
    print("PHASE 1: Generating Dimension Tables")
    print("-"*40)
    
    dim_date = generate_dim_date()
    dim_geography = generate_dim_geography()
    dim_channel = generate_dim_channel()
    dim_store = generate_dim_store(dim_channel)
    dim_employee = generate_dim_employee(dim_store)

    # Back-fill store_manager_employee_sk now that employees exist
    store_mgr_map = dim_employee[dim_employee['is_active']].groupby('store_sk')['employee_sk'].first()
    dim_store['store_manager_employee_sk'] = dim_store['store_sk'].map(store_mgr_map)

    dim_product = generate_dim_product()
    dim_customer = generate_dim_customer()
    dim_promotion = generate_dim_promotion(dim_date)
    
    # Generate fact tables (depend on dimensions)
    print("\nPHASE 2: Generating Fact Tables")
    print("-"*40)
    
    fact_sales = generate_fact_sales_transactions(
        dim_date, dim_customer, dim_product, dim_store, 
        dim_channel, dim_promotion, dim_employee
    )
    
    fact_inventory = generate_fact_inventory_snapshot(
        dim_date, dim_product, dim_store
    )
    
    fact_returns = generate_fact_returns(fact_sales, dim_date)

    # Back-fill return_flag in fact_sales for all transactions that have a return
    returned_sks = set(fact_returns['original_transaction_sk'].unique())
    fact_sales.loc[fact_sales['transaction_sk'].isin(returned_sks), 'return_flag'] = 'Y'

    fact_web_sessions = generate_fact_web_sessions(
        dim_date, dim_customer, dim_channel, fact_sales
    )
    
    # Collect all dataframes
    dataframes = {
        'dim_date': dim_date,
        'dim_geography': dim_geography,
        'dim_channel': dim_channel,
        'dim_store': dim_store,
        'dim_employee': dim_employee,
        'dim_product': dim_product,
        'dim_customer': dim_customer,
        'dim_promotion': dim_promotion,
        'fact_sales_transactions': fact_sales,
        'fact_inventory_snapshot': fact_inventory,
        'fact_returns': fact_returns,
        'fact_web_sessions': fact_web_sessions
    }
    
    # Validate data consistency
    validate_data_consistency(dataframes)
    
    # Print KPI summary
    print_kpi_summary(dataframes)
    
    # Save to CSV files
    save_to_csv(dataframes, OUTPUT_DIR)
    
    print("\n" + "="*60)
    print("✅ DATA GENERATION COMPLETE!")
    print("="*60)
    print(f"\nAll CSV files saved to: {OUTPUT_DIR}/")
    print("\nTable Summary:")
    print("-"*40)
    for table_name, df in dataframes.items():
        print(f"  {table_name}: {len(df):,} rows, {len(df.columns)} columns")
    
    return dataframes


if __name__ == "__main__":
    dataframes = main()