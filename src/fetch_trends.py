# src/fetch_trends.py

import pandas as pd
from pytrends.request import TrendReq
import time
import os
import sys
from math import ceil # For calculating number of batches

print(f"--- Google Trends Data Acquisition ---")
print(f"DEBUG: Running script using Python executable: {sys.executable}")
print(f"DEBUG: Using Pandas version: {pd.__version__}")

# --- Configuration ---
TARGET_YEAR = 2024 # <<< SET THE YEAR YOU WANT TO FETCH DATA FOR
DAYS_BEFORE_SB = 30 # How many days before SB Sunday to start
DAYS_AFTER_SB = 30  # How many days after SB Sunday to end
KEYWORDS_PER_BATCH = 5 # Google Trends limit for interest_over_time
SLEEP_BETWEEN_REQUESTS = 60 # Seconds to wait between Google requests

# Paths relative to project root (assuming script run from project root)
RAW_DATA_DIR = 'data/raw'
PROCESSED_DIR = 'data/processed'
TICKER_MAP_FILENAME = 'advertiser_ticker_mapping.csv'
ticker_map_path = os.path.join(RAW_DATA_DIR, TICKER_MAP_FILENAME)
OUTPUT_FILENAME = f'google_trends_{TARGET_YEAR}.csv'
trends_output_path = os.path.join(PROCESSED_DIR, OUTPUT_FILENAME)

# Super Bowl Dates Dictionary (copied from notebook)
super_bowl_sundays = {
    1969: '1969-01-12', 1970: '1970-01-11', 1971: '1971-01-17', 1972: '1972-01-16',
    1973: '1973-01-14', 1974: '1974-01-13', 1975: '1975-01-12', 1976: '1976-01-18',
    1977: '1977-01-09', 1978: '1978-01-15', 1979: '1979-01-21', 1980: '1980-01-20',
    1981: '1981-01-25', 1982: '1982-01-24', 1983: '1983-01-30', 1984: '1984-01-22',
    1985: '1985-01-20', 1986: '1986-01-26', 1987: '1987-01-25', 1988: '1988-01-31',
    1989: '1989-01-22', 1990: '1990-01-28', 1991: '1991-01-27', 1992: '1992-01-26',
    1993: '1993-01-31', 1994: '1994-01-30', 1995: '1995-01-29', 1996: '1996-01-28',
    1997: '1997-01-26', 1998: '1998-01-25', 1999: '1999-01-31', 2000: '2000-01-30',
    2001: '2001-01-28', 2002: '2002-02-03', 2003: '2003-01-26', 2004: '2004-02-01',
    2005: '2005-02-06', 2006: '2006-02-05', 2007: '2007-02-04', 2008: '2008-02-03',
    2009: '2009-02-01', 2010: '2010-02-07', 2011: '2011-02-06', 2012: '2012-02-05',
    2013: '2013-02-03', 2014: '2014-02-02', 2015: '2015-02-01', 2016: '2016-02-07',
    2017: '2017-02-05', 2018: '2018-02-04', 2019: '2019-02-03', 2020: '2020-02-02',
    2021: '2021-02-07', 2022: '2022-02-13', 2023: '2023-02-12', 2024: '2024-02-11',
    2025: '2025-02-09'
}

def main():
    # --- Load Keywords ---
    try:
        ticker_map_df = pd.read_csv(ticker_map_path)
        if 'BrandName' not in ticker_map_df.columns:
            print(f"ERROR: 'BrandName' column missing in {ticker_map_path}")
            return # Exit if column missing
        # Use unique brand names from the map as keywords
        keywords_all = ticker_map_df['BrandName'].dropna().unique().tolist()
        print(f"Loaded {len(keywords_all)} unique potential keywords from ticker map.")
        if not keywords_all:
             print("No keywords found in ticker map. Exiting.")
             return
    except FileNotFoundError:
        print(f"ERROR: Ticker mapping file not found at '{ticker_map_path}'. Cannot get keywords.")
        return
    except Exception as e:
         print(f"ERROR: Could not load or process ticker map: {e}")
         return

    # --- Determine Timeframe ---
    sb_sunday_str = super_bowl_sundays.get(TARGET_YEAR)
    if not sb_sunday_str:
        print(f"ERROR: Super Bowl Sunday date not found for year {TARGET_YEAR} in dictionary.")
        return

    try:
        sb_date = pd.Timestamp(sb_sunday_str)
        start_date = sb_date - pd.Timedelta(days=DAYS_BEFORE_SB)
        end_date = sb_date + pd.Timedelta(days=DAYS_AFTER_SB)
        timeframe = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"
        print(f"Calculated timeframe for {TARGET_YEAR}: {timeframe}")
    except Exception as e:
        print(f"ERROR: Could not calculate timeframe: {e}")
        return

    # --- Initialize PyTrends ---
    # hl='en-US': language/host; tz=360: US Pacific Timezone approx (adjust if needed, affects daily boundary)
    pytrends = TrendReq(hl='en-US', tz=360)

    # --- Fetch data in batches ---
    all_trends_data = [] # List to store results from each batch
    num_batches = ceil(len(keywords_all) / KEYWORDS_PER_BATCH)
    print(f"\nStarting fetch for {len(keywords_all)} keywords in {num_batches} batches...")

    for i in range(1):
        start_index = i * KEYWORDS_PER_BATCH
        end_index = start_index + KEYWORDS_PER_BATCH
        keyword_batch = keywords_all[start_index:end_index]

        print(f"  Fetching Batch {i+1}/{num_batches}: {keyword_batch}")

        try:
            pytrends.build_payload(
                kw_list=keyword_batch,
                cat=0, timeframe=timeframe, geo='US', gprop=''
            )
            # Get interest over time for this batch
            batch_data = pytrends.interest_over_time()

            if not batch_data.empty:
                if 'isPartial' in batch_data.columns:
                     batch_data.drop(columns=['isPartial'], inplace=True)
                # Note: Data is scaled 0-100 *within this batch* relative to the batch's peak.
                # Combining directly might require rescaling later if comparing across batches.
                all_trends_data.append(batch_data)
                print(f"  -> Batch {i+1} fetched successfully ({len(batch_data)} rows).")
            else:
                 print(f"  -> Batch {i+1} returned no data.")

            # Be polite to Google's API - wait between requests
            print(f"  -> Waiting {SLEEP_BETWEEN_REQUESTS} seconds...")
            time.sleep(SLEEP_BETWEEN_REQUESTS)

        except Exception as e:
            print(f"  -> ERROR fetching Batch {i+1}: {e}")
            print("     Skipping this batch. May be rate limited or API issue.")
            # Optionally wait longer before next batch
            time.sleep(SLEEP_BETWEEN_REQUESTS * 2)


    # --- Combine and Save Results ---
    if all_trends_data:
        # Concatenate along columns - assumes date index aligns
        # Need to handle cases where some keywords return no data (missing columns)
        # pd.concat(..., join='outer') can handle this
        try:
             final_trends_df = pd.concat(all_trends_data, axis=1)
             # Remove duplicate columns if keyword appeared in multiple batches (shouldn't happen with this logic)
             final_trends_df = final_trends_df.loc[:, ~final_trends_df.columns.duplicated()]

             print("\n--- Finished Fetching ---")
             print(f"Combined trends data shape: {final_trends_df.shape}")
             print("Preview:")
             print(final_trends_df.head())

             # Save the data
             os.makedirs(PROCESSED_DIR, exist_ok=True)
             final_trends_df.to_csv(trends_output_path)
             print(f"\nGoogle Trends data saved successfully to: {trends_output_path}")

        except Exception as e:
             print(f"\nERROR combining or saving trends data: {e}")
             print("Individual batch dataframes were stored in 'all_trends_data' list in memory if needed.")

    else:
        print("\nNo Google Trends data was successfully fetched for any batch.")


# --- Main Execution Guard ---
if __name__ == "__main__":
    main()
    print("\n--- Google Trends Script Finished ---")