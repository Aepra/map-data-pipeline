import pandas as pd
import pyodbc
import os

# Mengambil data dari GitHub Secrets
host = os.getenv('DB_HOST')
db = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASS')

# String koneksi (Driver 'ODBC Driver 17 for SQL Server' tersedia di GitHub Runner)
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={db};UID={user};PWD={password}'

def fetch_and_save():
    try:
        print("Menghubungkan ke database...")
        conn = pyodbc.connect(conn_str)
        
        # Menggunakan fungsi select yang kamu berikan
        query = "SELECT * FROM rpt.Pendaftaran_Looker_func1()"
        
        print("Mengambil data...")
        df = pd.read_sql(query, conn)
        
        # Pastikan folder data ada
        os.makedirs('data', exist_ok=True)
        
        # Konversi ke Parquet
        output_path = 'data/peta_murid.parquet'
        df.to_parquet(output_path, engine='pyarrow', compression='snappy')
        
        print(f"Berhasil! {len(df)} baris disimpan ke {output_path}")
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        exit(1)

if __name__ == "__main__":
    fetch_and_save()