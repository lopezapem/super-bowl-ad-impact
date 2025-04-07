# src/data_acquisition.py

import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import os
from io import StringIO
import sys

# --- Print environment info ---
print(f"DEBUG: Running script using Python executable: {sys.executable}")
print(f"DEBUG: Using Pandas version: {pd.__version__}")
# ---

print("--- Starting Data Acquisition ---")

# --- Configuration ---
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_Super_Bowl_commercials"
REQUEST_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
FINAL_COLS = ['Product_Type', 'Advertiser_Product_Title', 'Title', 'Plot_Notes', 'Decade', 'Year', 'SuperBowlNum']
OUTPUT_DIR = 'data/processed'
OUTPUT_FILENAME = 'wiki_super_bowl_commercials_extracted.csv'

# --- Fetch HTML Content ---
print(f"Fetching data from: {WIKI_URL}")
try:
    response = requests.get(WIKI_URL, headers=REQUEST_HEADERS, timeout=15)
    response.raise_for_status()
    print("Successfully fetched page content.")
except requests.exceptions.RequestException as e:
    print(f"ERROR: Failed to fetch URL: {e}")
    exit()

# --- Parse HTML and Extract Data (Removed dtype='object') ---
advertisers_df = None
all_data = []

try:
    soup = BeautifulSoup(response.text, 'lxml')
    content_div = soup.find(id='mw-content-text').find('div', class_='mw-parser-output')

    if not content_div:
        print("ERROR: Could not find main content div ('div.mw-parser-output'). Scraping cannot proceed.")
        exit()
    else:
        print("DEBUG: Found content_div successfully.")

    current_decade = None
    current_year = None
    current_sb_num = None

    print("\nScanning relevant elements (H2, H3, Table)...")
    relevant_elements = content_div.find_all(['h2', 'h3', 'table'])
    print(f"DEBUG: Found {len(relevant_elements)} relevant elements (H2, H3, TABLE).")

    for element in relevant_elements:
        # Process H2...
        if element.name == 'h2':
            h2_text = element.get_text(strip=True).replace('[edit]', '')
            if re.match(r'^\d{4}s$', h2_text):
                current_decade = h2_text; current_year = None; current_sb_num = None
                print(f"** Switched to Decade: {current_decade}")
            elif h2_text in ["See also", "References", "External links"]:
                print(f"DEBUG: Stopping processing at H2: {h2_text}")
                break
            else: current_decade = None; current_year = None; current_sb_num = None
        # Process H3...
        elif element.name == 'h3':
            if current_decade:
                heading_text = element.get_text(strip=True).replace('[edit]', '')
                match = re.match(r'(\d{4})\s*(?:\((\w+)\))?', heading_text)
                if match:
                    current_year = match.group(1); current_sb_num = match.group(2)
                    print(f"  ** Set Year/SB: {current_year} / {current_sb_num or 'N/A'}")
                else: current_year = None; current_sb_num = None
        # Process Table...
        elif element.name == 'table' and element.has_attr('class') and 'wikitable' in element['class']:
            if current_year:
                print(f"DEBUG: Found wikitable potentially for Year: {current_year}")
                try:
                    print(f"   -> Attempting to parse table with pd.read_html (NO dtype specified)...") # Updated message
                    # --- MODIFICATION: REMOVED dtype='object' ---
                    df_list = pd.read_html(StringIO(str(element)), flavor='bs4', header=0, keep_default_na=True)

                    if df_list:
                        df = df_list[0].copy()
                        print(f"   -> Successfully parsed table. Original Cols: {df.columns.tolist()}")

                        # --- Column Renaming ---
                        rename_map = {}
                        for col in df.columns:
                            col_orig = str(col).strip(); col_norm = col_orig.lower()
                            if col_norm == 'product type': rename_map[col_orig] = 'Product_Type'
                            elif col_norm in ['product/title', 'advertiser/product']: rename_map[col_orig] = 'Advertiser_Product_Title'
                            elif col_norm == 'title': rename_map[col_orig] = 'Title'
                            elif col_norm.startswith('plot/notes'): rename_map[col_orig] = 'Plot_Notes'
                        df.rename(columns=rename_map, inplace=True)
                        print(f"   -> Renamed Cols Present: {list(rename_map.values())}")

                        # Check essential columns
                        if 'Product_Type' in df.columns and ('Advertiser_Product_Title' in df.columns or 'Title' in df.columns):
                            print(f"   -> Essential renamed columns found.")
                            df['Decade'] = current_decade; df['Year'] = current_year; df['SuperBowlNum'] = current_sb_num
                            cols_to_keep = [col for col in FINAL_COLS if col in df.columns]
                            df_processed = df[cols_to_keep].copy()
                            all_data.append(df_processed)
                            print(f"  -> === ADDED Standardized DATA for {current_year} ===")
                        else: print(f"   -> *** Essential Renamed Columns NOT FOUND for {current_year}. Skipping table. ***")

                    else: print(f"   -> pd.read_html returned empty list for {current_year}.")

                # --- RE-ADDED ValueError CATCH specifically ---
                except ValueError as ve:
                     print(f"   -> VALUE ERROR parsing table for year {current_year}: {ve}. Skipping table.")
                except Exception as e:
                    print(f"   -> UNEXPECTED error parsing table for year {current_year}: {e}")

                # Reset year after processing/attempting this table
                current_year = None
                current_sb_num = None


    # --- Combine and Save Results ---
    if all_data:
        final_commercials_df = pd.concat([df.reindex(columns=FINAL_COLS) for df in all_data], ignore_index=True)
        print("\n--- Finished Extraction ---")
        print(f"Total commercials extracted: {len(final_commercials_df)}")
        print("\n--- Preview of Final Combined DataFrame ---")
        # ... (rest of print/save logic) ...
        print(final_commercials_df.head())
        output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
        final_commercials_df.to_csv(output_path, index=False)
        print(f"\nData successfully saved to: {output_path}")
    else:
        print("\nNo commercial data was successfully extracted and processed.")
        # ... (rest of no data message) ...

except Exception as e:
    print(f"\nAn unexpected error occurred during parsing or processing: {e}")

print("\n--- Data Acquisition Script Finished ---")