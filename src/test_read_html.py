# test_read_html.py
import pandas as pd
from io import StringIO
import sys

print(f"--- Test Script ---")
print(f"Python Executable: {sys.executable}")
print(f"Pandas Version: {pd.__version__}")

# Simple HTML table string with mixed types
html_string = """
<table>
  <thead>
    <tr><th>ColA</th><th>ColB</th></tr>
  </thead>
  <tbody>
    <tr><td>1</td><td>Apple</td></tr>
    <tr><td>2</td><td>Banana</td></tr>
    <tr><td>10""</td><td>Cherry</td></tr>
  </tbody>
</table>
"""

try:
    print("\nAttempting pd.read_html with dtype='object'...")
    # Using StringIO just like in the main script
    df_list = pd.read_html(StringIO(html_string), dtype='object', header=0)

    if df_list:
        df = df_list[0]
        print("SUCCESS: pd.read_html with dtype='object' worked.")
        print("Data types found:")
        print(df.dtypes)
        print("DataFrame content:")
        print(df)
    else:
        print("FAILURE: pd.read_html returned an empty list.")

except TypeError as e:
    print(f"\nFAILURE: Caught TypeError: {e}")
    print("This confirms the dtype argument is not being accepted.")
except Exception as e:
    print(f"\nFAILURE: Caught unexpected error: {e}")

print("\n--- Test Script Finished ---")