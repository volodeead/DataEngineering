import duckdb
import pandas as pd

# Функція для створення таблиці в DuckDB
def create_duckdb_table(conn):
    conn.execute("""
        CREATE TABLE electric_cars (
            VIN VARCHAR(10),
            County VARCHAR,
            City VARCHAR,
            State VARCHAR,
            Postal_Code INTEGER,
            Model_Year INTEGER,
            Make VARCHAR,
            Model VARCHAR,
            Electric_Vehicle_Type VARCHAR,
            CAFV_Eligibility VARCHAR,
            Electric_Range INTEGER,
            Base_MSRP INTEGER,
            Legislative_District INTEGER,
            DOL_Vehicle_ID VARCHAR,  
            Vehicle_Location VARCHAR,
            Electric_Utility VARCHAR,
            Census_Tract VARCHAR
        )
    """)

# Функція для читання CSV файлу і вставки даних в DuckDB таблицю
def insert_data_into_duckdb(conn, csv_file):
    df = pd.read_csv(csv_file)
    conn.register('electric_cars_temp', df)  # Змінено ім'я об'єкта на 'electric_cars_temp'
    conn.execute("INSERT INTO electric_cars SELECT * FROM electric_cars_temp")  # Змінено ім'я таблиці

# Функція для підрахунку кількості електромобілів у кожному місті
def count_cars_by_city(conn):

    # Запис у Parquet файл
    parquet_path = 'count_cars_by_city_by_duckdb.parquet'

    # Оновлення на використання COPY TO
    conn.execute("""
        COPY (SELECT City, COUNT(*) as Car_Count
        FROM electric_cars
        GROUP BY City)
        TO '{parquet_path}'
        (FORMAT PARQUET, CODEC 'SNAPPY', ROW_GROUP_SIZE 100000)
    """.format(parquet_path=parquet_path))

# Функція для знаходження 3 найпопулярніших електромобілів
def top_three_models(conn):

# Запис у Parquet файл
    parquet_path = 'top_three_models_by_duckdb.parquet'

    conn.execute("""
        COPY (SELECT Model, COUNT(*) as Model_Count
        FROM electric_cars
        GROUP BY Model
        ORDER BY COUNT(*) DESC
        LIMIT 3)
        TO '{parquet_path}'
        (FORMAT PARQUET, CODEC 'SNAPPY', ROW_GROUP_SIZE 100000)
    """.format(parquet_path=parquet_path))
    
# Функція для знаходження найпопулярніших електромобілів у кожному поштовому індексі
def top_model_by_postal_code(conn):
   
    # Запис у Parquet файл
    parquet_path = 'top_model_by_postal_code_by_duckdb.parquet'
    conn.execute("""
        COPY (SELECT Postal_Code, Model, Model_Count
        FROM (
            SELECT Postal_Code, Model, COUNT(*) as Model_Count,
                   ROW_NUMBER() OVER (PARTITION BY Postal_Code ORDER BY COUNT(*) DESC) as RowNum
            FROM electric_cars
            GROUP BY Postal_Code, Model
        ) ranked
        WHERE RowNum = 1)
        TO '{parquet_path}'
        (FORMAT PARQUET, CODEC 'SNAPPY', ROW_GROUP_SIZE 100000)
    """.format(parquet_path=parquet_path))

# Функція для підрахунку кількості електромобілів за роками випуску та запису результату у Parquet файли
def count_cars_by_year(conn):
    result = conn.execute("""
        SELECT Model_Year, COUNT(*) FROM electric_cars GROUP BY Model_Year
    """)
    
    # Запис у Parquet файли, розділені за роками
    for year in result.fetchall():
        parquet_path = f'cars_by_year_{year[0]}.parquet'
        conn.execute("""
            COPY (SELECT {year} as Model_Year, COUNT(*) as Car_Count FROM electric_cars WHERE Model_Year = {year})
            TO '{parquet_path}'
            (FORMAT PARQUET, CODEC 'SNAPPY', ROW_GROUP_SIZE 100000)
        """.format(year=year[0], parquet_path=parquet_path))


# Основна функція для виклику інших функцій
def main():
    conn = duckdb.connect(database=':memory:', read_only=False)
    
    create_duckdb_table(conn)
    insert_data_into_duckdb(conn, 'data/electric-cars.csv')

    cities_count = count_cars_by_city(conn)

    top_models = top_three_models(conn)

    top_models_by_postal_code = top_model_by_postal_code(conn)

    count_cars_by_year(conn)

    conn.close()

if __name__ == "__main__":
    main()