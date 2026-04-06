import os
# Сначала отключаем проверку CPU, потом импорты
os.environ["POLARS_SKIP_CPU_CHECK"] = "1"

import polars as pl
import sqlite3
import glob
from concurrent.futures import ThreadPoolExecutor
import time


# Настройки
folder_path_xlsx = r"C:\Users\Matwey\Desktop\AutomizeParse\test_data\*.xlsx"
# Теперь работаем с Parquet вместо CSV
folder_path_parquet = r"C:\Users\Matwey\Desktop\AutomizeParse\test_data\*.parquet"
workers = 8  # На конвертацию можно больше воркеров
db_uri = "sqlite://db.db"

def init_db():
    with sqlite3.connect("db.db") as conn:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER, name TEXT, city TEXT,
            price REAL, quantity INTEGER,
            date_created TEXT, last_update TEXT
        )
        """)

def excel_to_parquet(excel_path):
    """Excel -> Parquet (в 10-50 раз быстрее и компактнее CSV)"""
    try:
        parquet_path = os.path.splitext(excel_path)[0] + ".parquet"
        # Читаем Excel и сразу пишем в бинарный Parquet
        pl.read_excel(excel_path).write_parquet(parquet_path)
        os.remove(excel_path)
        return parquet_path
    except Exception as e:
        print(f"Ошибка Excel {os.path.basename(excel_path)}: {e}")

def upload_parquet_to_db(parquet_path):
    """Прямая запись из Polars в SQLite (минуя циклы Python)"""
    try:
        df = pl.read_parquet(parquet_path)
        
        if df.is_empty():
            os.remove(parquet_path)
            return

        df.select([
            pl.col("id").alias("product_id"),
            pl.col("product").alias("name"),
            pl.col("city"),
            pl.col("price"),
            pl.col("quantity"),
            pl.col("date_created"),
            pl.col("last_updated").alias("last_update")
        ]).write_database(
            table_name="products",
            connection=db_uri,
            if_table_exists="append",
            engine="adbc"
        )
        
        os.remove(parquet_path)
        print(f"Загружен: {os.path.basename(parquet_path)}")
    except Exception as e:
        print(f"Ошибка БД {os.path.basename(parquet_path)}: {e}")

def main():
    init_db()

    # 1. Конвертация (Здесь многопоточность ОСТАВЛЯЕМ, это быстро)
    start_con = time.time()
    xlsx_files = glob.glob(folder_path_xlsx)
    if xlsx_files:
        print(f"Конвертация {len(xlsx_files)} файлов...")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            executor.map(excel_to_parquet, xlsx_files)
    success_con = time.time() - start_con

    # 2. Загрузка (ЗАМЕНЯЕМ СТАРЫЙ БЛОК НА ЭТОТ - СТРОГО В ОДИН ПОТОК)
    start_db = time.time()
    all_parquet = glob.glob(folder_path_parquet)
    if all_parquet:
        print(f"Запись в БД через ADBC (последовательно)...")
        # Вместо executor.map используем обычный цикл for
        for parquet_file in all_parquet:
            upload_parquet_to_db(parquet_file)
    
    success_db = time.time() - start_db
    
    print("\n--- Отчет ---")
    print(f"Конвертация: {success_con:.2f} сек.")
    print(f"Запись в БД: {success_db:.2f} сек.")
    print(f"Общее время: {(success_con + success_db):.2f} сек.")



if __name__ == "__main__":
    main()