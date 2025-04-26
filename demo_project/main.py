import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json

# Static category lists
CATEGORIES = {
    "Equity": 1,
    "Debt": 2,
    "Hybrid": 3,
    "Solution Oriented": 4,
    "Other": 5
}

MATURITY_TYPES = {
    "Open Ended": 1,
    "Close Ended": 2
}

# Sub-categories for each category type
EQUITY_SUB_CATEGORIES = {
    "Large Cap": 1,
    "Large & Mid Cap": 2,
    "Flexi Cap": 3,
    "Multi Cap": 4,
    "Mid Cap": 5,
    "Small Cap": 6,
    "Value": 7,
    "ELSS": 8,
    "Contra": 9,
    "Dividend Yield": 10,
    "Focused": 11,
    "Sectoral / Thematic": 12
}

DEBT_SUB_CATEGORIES = {
    "Long Duration": 13,
    "Medium to Long Duration": 14,
    "Short Duration": 15,
    "Medium Duration": 16,
    "Money Market": 17,
    "Low Duration": 18,
    "Ultra Short Duration": 19,
    "Liquid": 20,
    "Overnight": 21,
    "Dynamic Bond": 22,
    "Corporate Bond": 23,
    "Credit Risk": 24,
    "Banking and PSU": 25,
    "Floater": 26,
    "FMP": 27,
    "Gilt": 28,
    "Gilt with 10 year constant duration": 29
}

HYBRID_SUB_CATEGORIES = {
    "Aggressive Hybrid": 30,
    "Conservative Hyrbid": 31,
    "Equity Savings": 32,
    "Arbitrage": 33,
    "Multi Asset Allocation": 34,
    "Dynamic Asset Allocation or Balanced Advantage": 35,
    "Balanced Hybrid": 40
}

SOLUTION_ORIENTED_SUB_CATEGORIES = {
    "Children's": 36,
    "Retirement": 37
}

OTHER_SUB_CATEGORIES = {
    "Index Funds ETFs": 38,
    "FoFs (Overseas/Domestic)": 39
}

# Map categories to their respective sub-categories
CATEGORY_TO_SUB_CATEGORIES = {
    "Equity": EQUITY_SUB_CATEGORIES,
    "Debt": DEBT_SUB_CATEGORIES,
    "Hybrid": HYBRID_SUB_CATEGORIES,
    "Solution Oriented": SOLUTION_ORIENTED_SUB_CATEGORIES,
    "Other": OTHER_SUB_CATEGORIES
}

MUTUAL_FUNDS = {
    "360 ONE Mutual Fund": 1,
    "Aditya Birla Sun Life Mutual Fund": 2,
    "Angel One Mutual Fund": 3,
    "Axis Mutual Fund": 4,
    "Bajaj Finserv Mutual Fund": 5,
    "Bandhan Mutual Fund": 6,
    "Bank of India Mutual Fund": 7,
    "Baroda BNP Paribas Mutual Fund": 8,
    "Canara Robeco Mutual Fund": 9,
    "DSP Mutual Fund": 10,
    "Edelweiss Mutual Fund": 11,
    "Franklin Templeton Mutual Fund": 12,
    "Groww Mutual Fund": 13,
    "HDFC Mutual Fund": 14,
    "Helios Mutual Fund": 15,
    "HSBC Mutual Fund": 16,
    "ICICI Prudential Mutual Fund": 17,
    "Invesco Mutual Fund": 20,
    "ITI Mutual Fund": 21,
    "JM Financial Mutual Fund": 22,
    "Kotak Mahindra Mutual Fund": 23,
    "LIC Mutual Fund": 24,
    "Mahindra Manulife Mutual Fund": 25,
    "Mirae Asset Mutual Fund": 26,
    "Motilal Oswal Mutual Fund": 27,
    "Navi Mutual Fund": 28,
    "Nippon India Mutual Fund": 29,
    "NJ Mutual Fund": 30,
    "Old Bridge Mutual Fund": 31,
    "PGIM India Mutual Fund": 32,
    "PPFAS Mutual Fund": 33,
    "Quant Mutual Fund": 34,
    "Quantum Mutual Fund": 35,
    "Samco Mutual Fund": 37,
    "SBI Mutual Fund": 38,
    "Shriram Mutual Fund": 39,
    "Sundaram Mutual Fund": 40,
    "Tata Mutual Fund": 41,
    "Taurus Mutual Fund": 42,
    "Trust Mutual Fund": 43,
    "Unifi Mutual Fund": 44,
    "Union Mutual Fund": 45,
    "UTI Mutual Fund": 46,
    "WhiteOak Capital Mutual Fund": 47,
    "Zerodha Mutual Fund": 48
}

def fetch_data(url, headers, payload):
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,  # number of retries
        backoff_factor=1,  # wait 1, 2, 4 seconds between retries
        status_forcelist=[500, 502, 503, 504]  # HTTP status codes to retry on
    )
    
    # Create a session with retry strategy
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    
    try:
        print(f"\nAttempting to fetch data with payload: {payload}")
        # Try with SSL verification first
        response = session.post(url, headers=headers, json=payload, verify=True)
        print(f"Response status code: {response.status_code}")
        print(f"Raw response text: {response.text}")  # Print raw response
        data = response.json()
        print(f"Response data: {data}")
        
        # Check if data exists and is not empty
        if data and 'data' in data:
            if data['data'] and len(data['data']) > 0:
                print(f"Successfully fetched {len(data['data'])} records")
                return data['data']
            else:
                print("Empty data array received")
        else:
            print("No data found in response")
    except requests.exceptions.SSLError:
        try:
            # If SSL verification fails, try without verification
            print("SSL verification failed, retrying without verification...")
            response = session.post(url, headers=headers, json=payload, verify=False)
            print(f"Response status code (without SSL): {response.status_code}")
            print(f"Raw response text (without SSL): {response.text}")  # Print raw response
            data = response.json()
            print(f"Response data (without SSL): {data}")
            
            # Check if data exists and is not empty
            if data and 'data' in data:
                if data['data'] and len(data['data']) > 0:
                    print(f"Successfully fetched {len(data['data'])} records (without SSL)")
                    return data['data']
                else:
                    print("Empty data array received (without SSL)")
            else:
                print("No data found in response (without SSL)")
        except Exception as e:
            print(f"Error fetching data (without SSL verification): {str(e)}")
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
    
    return []

def process_and_save_data(category_name, category_id, maturity_type_name, maturity_type_id, 
                         sub_category_name, sub_category_id, fund_name, fund_id, 
                         base_dir, date_str, timestamp_str):
    url = "https://polling.crisil.com/gateway/pollingsebi/api/amfi/fundperformance"
    
    # Create payload first to calculate Content-Length
    payload = {
        "maturityType": maturity_type_id,  # Fixed at 1
        "category": category_id,
        "subCategory": sub_category_id,
        "mfid": fund_id,  # Fixed at 0
        "reportDate": "24-Apr-2024"  # Fixed date
    }
    
    # Calculate Content-Length
    content_length = len(json.dumps(payload))
    
    # Updated headers to exactly match browser request and include security headers
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Content-Length': str(content_length),
        'Content-Type': 'application/json',
        'Host': 'polling.crisil.com',
        'Origin': 'https://polling.crisil.com',
        'Referer': 'https://polling.crisil.com/polling/amfi/fund-performance',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Storage-Access': 'active',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        # Security headers
        'Cache-Control': 'no-cache, no-store, max-age=0, must-revalidate',
        'Pragma': 'no-cache',
        'Strict-Transport-Security': 'max-age=31536000',
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'SAMEORIGIN',
        'X-XSS-Protection': '1; mode=block',
        'Content-Security-Policy': "default-src 'self' 'unsafe-inline';font-src 'self' data:; img-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'",
        'Content-Security-Policy': "frame-ancestors 'self' https://www.amfiindia.com/ https://www.mutualfundssahihai.com https://revamp-stage.mutualfundssahihai.com"
    }
    
    print("\n" + "="*50)
    print(f"PROCESSING: {category_name} - {sub_category_name}")
    print(f"Using fixed values:")
    print(f"- Maturity Type: Open Ended (1)")
    print(f"- MFID: All Funds (0)")
    print(f"- Date: 24-Apr-2024")
    print(f"Content-Length: {content_length}")
    print("="*50)
    
    try:
        response = requests.post(url, headers=headers, json=payload, verify=False)
        print(f"Response status code: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        print(f"Full response text: {response.text}")
        
        if response.status_code == 401:
            print("\n✗ Authorization failed - Check the authorization token")
            return
        elif response.status_code == 403:
            print("\n✗ Access forbidden - Check if the token has expired")
            return
        
        data = response.json()
        if data and 'data' in data:
            if data['data'] and len(data['data']) > 0:
                print(f"\n✓ Data received successfully from API ({len(data['data'])} records)")
                print(f"First record sample: {json.dumps(data['data'][0], indent=2)}")
                save_category_data(category_name, data['data'], base_dir, date_str, timestamp_str, 
                                maturity_type_name, sub_category_name, fund_name)
            else:
                print("\n✗ Empty data array received")
                print(f"Response structure: {json.dumps(data, indent=2)}")
        else:
            print("\n✗ No data in response")
            print(f"Response: {json.dumps(data, indent=2)}")
    except Exception as e:
        print(f"\n✗ Error making request: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        print(f"Error details: {str(e)}")
    
    print("="*50 + "\n")

def save_category_data(category_name, category_data, base_mutual_funds_dir, date_str, timestamp_str,
                      maturity_type, sub_category, fund_name):
    try:
        # Convert to DataFrame first to extract actual values
        df = pd.DataFrame(category_data)
        
        # Extract actual values from the data
        primary_category = df['primaryCategory'].iloc[0] if 'primaryCategory' in df.columns else category_name
        actual_category = df['category'].iloc[0] if 'category' in df.columns else sub_category
        
        # Create directory path with the correct structure using actual values
        # <primary_category>/<category>/<date>/<processed_timestamp>
        base_dir = os.path.join(
            base_mutual_funds_dir,
            primary_category,  # Use actual primary category from data
            actual_category,   # Use actual category from data
            date_str,         # date
            timestamp_str     # processed_timestamp
        )
        
        print(f"\nCreating directory: {base_dir}")
        os.makedirs(base_dir, exist_ok=True)
        
        # Create filename using actual values
        excel_filename = f"fund_performance_{primary_category}_{actual_category}_{timestamp_str}.xlsx"
        excel_path = os.path.join(base_dir, excel_filename)
        
        # Remove specified columns
        columns_to_remove = ['maturityType', 'category', 'subCategory', 'reportDate', 'primaryCategory']
        for col in columns_to_remove:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        print(f"Saving to Excel file: {excel_path}")
        df.to_excel(excel_path, index=False)
        
        # Print success message
        print("\n" + "="*50)
        print("✓ FILE SAVED SUCCESSFULLY!")
        print(f"Location: {excel_path}")
        print(f"Records saved: {len(category_data)}")
        print(f"Primary Category: {primary_category}")
        print(f"Category: {actual_category}")
        print(f"Columns removed: {', '.join(columns_to_remove)}")
        print("="*50 + "\n")
            
    except Exception as e:
        print("\n" + "="*50)
        print("✗ ERROR SAVING FILE!")
        print(f"Error: {str(e)}")
        print(f"Directory path: {base_dir}")
        print(f"Excel path: {excel_path}")
        print(f"Data length: {len(category_data) if category_data else 0}")
        print("="*50 + "\n")

def main():
    start_time = time.time()
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_mutual_funds_dir = os.path.join(script_dir)
        os.makedirs(base_mutual_funds_dir, exist_ok=True)

        current_date = datetime.now()
        date_str = current_date.strftime("%Y-%m-%d")
        timestamp_str = current_date.strftime("%Y%m%d_%H%M%S")
        
        # Fixed values
        maturity_type_id = 1  # Always use Open Ended
        mfid = 0  # All funds
        report_date = "24-Apr-2024"  # Fixed date
        
        # Calculate total combinations for progress tracking
        total_combinations = sum(len(sub_cats) for sub_cats in CATEGORY_TO_SUB_CATEGORIES.values())
        processed = 0
        
        print(f"\nTotal combinations to process: {total_combinations}")
        print("Categories and their sub-categories:")
        for category, sub_cats in CATEGORY_TO_SUB_CATEGORIES.items():
            print(f"- {category}: {len(sub_cats)} sub-categories")
        
        # Process all categories
        for category_name, category_id in CATEGORIES.items():
            print(f"\n{'='*20} Processing Category: {category_name} {'='*20}")
            
            # Get the correct sub-categories for this category
            sub_categories = CATEGORY_TO_SUB_CATEGORIES.get(category_name, {})
            if not sub_categories:
                print(f"\n✗ No sub-categories found for {category_name}")
                continue
                
            print(f"\nFound {len(sub_categories)} sub-categories for {category_name}")
            
            # Process all sub-categories for this category
            for sub_category_name, sub_category_id in sub_categories.items():
                processed += 1
                print(f"\nProcessing {processed}/{total_combinations}:")
                print(f"Category: {category_name} (ID: {category_id})")
                print(f"Sub Category: {sub_category_name} (ID: {sub_category_id})")
                print(f"Using fixed values:")
                print(f"- Maturity Type: Open Ended (1)")
                print(f"- MFID: All Funds (0)")
                print(f"- Date: {report_date}")
                
                process_and_save_data(
                    category_name, category_id,
                    "Open Ended", maturity_type_id,  # Fixed maturity type
                    sub_category_name, sub_category_id,
                    "All Funds", mfid,  # Fixed mfid
                    base_mutual_funds_dir, date_str, timestamp_str
                )
        
        print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")
        
    except Exception as e:
        print(f"Program failed with error: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        print(f"Error details: {str(e)}")

if __name__ == '__main__':
    main() 