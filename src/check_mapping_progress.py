# src/check_mapping_progress.py

import pandas as pd
import os
import sys

print(f"--- Mapping Progress Check ---")
print(f"DEBUG: Running script using Python executable: {sys.executable}")
print(f"DEBUG: Using Pandas version: {pd.__version__}")

# --- Configuration (Revised Paths) ---
TICKER_MAP_FILENAME = 'advertiser_ticker_mapping.csv'
COMMERCIALS_FILENAME = 'wiki_super_bowl_commercials_extracted.csv'
# Define paths directly from project root (assuming script is run from there)
RAW_DATA_DIR = 'data/raw'
PROCESSED_DIR = 'data/processed'
ticker_map_path = os.path.join(RAW_DATA_DIR, TICKER_MAP_FILENAME)
commercials_path = os.path.join(PROCESSED_DIR, COMMERCIALS_FILENAME)
# --- End of Revised Configuration ---

# --- Load Ticker Map ---
ticker_map_df = pd.DataFrame()
original_case_map = {}
known_brands_sorted = []
try:
    ticker_map_df = pd.read_csv(ticker_map_path)
    print(f"\nTicker mapping loaded successfully from '{ticker_map_path}'. Shape: {ticker_map_df.shape}")
    if not ticker_map_df.empty and 'BrandName' in ticker_map_df.columns and ticker_map_df['BrandName'].notna().any():
        lc_brands = ticker_map_df.dropna(subset=['BrandName'])['BrandName'].str.lower()
        unique_lc_indices = lc_brands.drop_duplicates(keep='first').index
        lc_map_temp = ticker_map_df.loc[unique_lc_indices]
        original_case_map = pd.Series(
            lc_map_temp.BrandName.values,
            index=lc_map_temp.BrandName.str.lower()
        ).to_dict()
        known_brands_set = set(original_case_map.keys())
        known_brands_sorted = sorted(list(known_brands_set), key=len, reverse=True)
        print(f"Prepared {len(known_brands_sorted)} unique known brands for matching.")
    else:
        print("Ticker map is empty or invalid.")
except FileNotFoundError:
    print(f"ERROR: Ticker mapping file not found at '{ticker_map_path}'")
except Exception as e:
     print(f"Error loading or processing ticker map: {e}")

# --- Load Commercials Data ---
commercials_df = pd.DataFrame()
try:
    commercials_df = pd.read_csv(commercials_path)
    print(f"\nCommercials data loaded successfully from '{commercials_path}'. Shape: {commercials_df.shape}")
except FileNotFoundError:
    print(f"ERROR: Commercials data file not found at '{commercials_path}'")
except Exception as e:
    print(f"An error occurred loading the commercials CSV: {e}")


# --- Define Extraction Function ---
def get_primary_advertiser_final(adv_prod_title, brands_sorted_list, lc_to_orig_map):
    if pd.isna(adv_prod_title): return None
    text_to_search = str(adv_prod_title).lower()
    match_found_lc = None
    for brand_lower in brands_sorted_list:
        if brand_lower in text_to_search:
             match_found_lc = brand_lower
             break
    if match_found_lc:
         return lc_to_orig_map.get(match_found_lc)
    else:
         return None

# --- Apply Function and Check Progress ---
if not commercials_df.empty and known_brands_sorted and original_case_map:
    print("\nApplying extraction function...")
    try:
        commercials_df['Primary_Advertiser'] = commercials_df['Advertiser_Product_Title'].apply(
            get_primary_advertiser_final,
            args=(known_brands_sorted, original_case_map)
        )
        print("Extraction complete.")

        # --- Summary Statistics ---
        total_rows = len(commercials_df)
        mapped_rows = commercials_df['Primary_Advertiser'].notna().sum()
        unmapped_rows = commercials_df['Primary_Advertiser'].isna().sum()
        percent_mapped = (mapped_rows / total_rows) * 100 if total_rows > 0 else 0

        print("\n--- Mapping Summary ---")
        print(f"Total Commercials: {total_rows}")
        print(f"Mapped to Primary Advertiser: {mapped_rows} ({percent_mapped:.1f}%)")
        print(f"Could NOT be mapped:         {unmapped_rows}")

        if mapped_rows > 0:
             print("\nTop 30 Mapped Primary Advertisers:")
             display_df = pd.DataFrame(commercials_df['Primary_Advertiser'].value_counts().head(30))
             print(display_df) # Use print for scripts, display is for notebooks

        if unmapped_rows > 0:
            print("\nTop 50 UNMAPPED Original 'Advertiser_Product_Title' Entries:")
            unmapped_subset = commercials_df[commercials_df['Primary_Advertiser'].isnull()]
            display_df_unmapped = pd.DataFrame(unmapped_subset['Advertiser_Product_Title'].value_counts().head(50))
            print(display_df_unmapped) # Use print

    except Exception as e:
        print(f"\nAn error occurred during function application: {e}")

else:
    print("\nSkipping extraction check because data or map could not be loaded.")

print("\n--- Check Script Finished ---")