import urllib.request
import zipfile
import io
import pandas as pd


HISTORY_DATA_URL="https://www.data.gouv.fr/api/1/datasets/r/1ae6c731-991f-4441-9663-adc99005fac5"

def fetch_history_data() -> pd.DataFrame:
    """Fetch historical data from HISTORY_DATA_URL and return as Pandas DataFrame."""
    with urllib.request.urlopen(HISTORY_DATA_URL) as resp:
        zip_bytes = resp.read()
            
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        # find the first xls/xlsx file
        xls_filename = None
        for name in z.namelist():
            if name.lower().endswith(('.xls', '.xlsx')):
                xls_filename = name
                break
        if not xls_filename:
            raise ValueError("No XLS/XLSX file found in history zip")

        # read file bytes and pass to pandas
        content_bytes = z.read(xls_filename)
        # choose engine based on extension (.xls uses xlrd, .xlsx uses openpyxl)
        ext = xls_filename.lower().rsplit('.', 1)[-1]
        if ext == 'xls':
            # pandas requires the 'xlrd' package to read .xls files.
            # Ensure 'xlrd' is installed in the environment (requirements.txt may need updating).
            engine = 'xlrd'
        else:
            engine = 'openpyxl'
        try:
            df = pd.read_excel(io.BytesIO(content_bytes), engine=engine, index_col=False, header=0)
        except Exception:
            # Some providers label a text/TSV file with .xls extension.
            # Fall back to parsing as tab-separated text using latin-1 encoding.
            text = content_bytes.decode('latin-1')
            df = pd.read_csv(
                io.StringIO(text),
                sep='\t',
                index_col=False,
                header=0,
                low_memory=False,
            )
            # print(df.columns)
        
    df = df[df['Nature'].notnull()]
    return df
        
        
if __name__ == "__main__":
    df = fetch_history_data()
    # show which columns have null values
    null_counts = df.isnull().sum()
    print("Null value counts per column:")
    print(null_counts)  
    print(f"Total rows fetched: {len(df)}")