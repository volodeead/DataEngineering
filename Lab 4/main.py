import psycopg2
import os
import csv

def create_table(conn, cursor, table_name, csv_path):
    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        fields = next(csv_reader)

    # Отримуємо перший рядок зі значеннями, щоб визначити типи даних
    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Пропускаємо заголовок
        sample_row = next(csv_reader)

    # Визначаємо типи даних для кожної колонки
    data_types = []
    for value in sample_row:
        if value.isdigit():
            data_types.append("INTEGER")
        elif value.replace(".", "", 1).isdigit():
            data_types.append("REAL")
        else:
            data_types.append("VARCHAR(255)")

    # Створюємо таблицю
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {", ".join([f"{field} {data_type}" for field, data_type in zip(fields, data_types)])}
    );
    """)
    conn.commit()

    print(f"Table {table_name} created successfully.")

def create_tables(conn, cursor):
    # Визначаємо шляхи до CSV файлів
    accounts_path = os.path.join("data", "accounts.csv")
    products_path = os.path.join("data", "products.csv")
    transactions_path = os.path.join("data", "transactions.csv")

    # Викликаємо функцію для створення кожної таблиці
    create_table(conn, cursor, "accounts", accounts_path)
    create_table(conn, cursor, "products", products_path)
    create_table(conn, cursor, "transactions", transactions_path)

    print("Tables created successfully.")

def insert_data(conn, cursor):

    # Очищаємо дані у таблиці перед вставкою нових
    cursor.execute("DELETE FROM transactions;")
    cursor.execute("DELETE FROM accounts;")
    cursor.execute("DELETE FROM products;")
    conn.commit()

    # Вставляємо дані у таблицу accounts з accounts.csv
    accounts_path = os.path.join("data", "accounts.csv")
    with open(accounts_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            cursor.execute(f"""
            INSERT INTO accounts ({", ".join(row.keys())})
            VALUES ({", ".join(["%s"] * len(row))});
            """, tuple(row.values()))

    # Вставляємо дані у таблицу products з products.csv
    products_path = os.path.join("data", "products.csv")
    with open(products_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            cursor.execute(f"""
            INSERT INTO products ({", ".join(row.keys())})
            VALUES ({", ".join(["%s"] * len(row))});
            """, tuple(row.values()))


    # Вставляємо дані у таблицю transactions з transactions.csv
    transactions_path = os.path.join("data", "transactions.csv")
    with open(transactions_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            cursor.execute(f"""
            INSERT INTO transactions ({", ".join(row.keys())})
            VALUES ({", ".join(["%s"] * len(row))});
            """, tuple(row.values()))

    conn.commit()

    print("Data inserted successfully.")

def display_tables_info(conn, cursor):
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]

        # Виводимо назву таблиці
        print(f"\nTable: {table_name}")

        # Отримуємо інформацію про стовпці та їхні типи
        cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';")
        columns_info = cursor.fetchall()

        # Виводимо перелік стовпців та їхніх типів
        print("\nColumns and their types:")
        for column_info in columns_info:
            print(f"{column_info[0]}: {column_info[1]}")

        # Отримуємо дані з таблиці
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()

        # Виводимо перелік рядків із значеннями
        print("\nRows:")
        for row in rows:
            print(row)

        # Отримуємо інформацію про ключі та зв'язки
        cursor.execute(f"""
            SELECT conname, conrelid::regclass, confrelid::regclass
            FROM pg_constraint
            WHERE confrelid = '{table_name}'::regclass;
        """)
        constraints_info = cursor.fetchall()

        # Виводимо перелік ключів та зв'язків
        print("\nKeys and relationships:")
        for constraint_info in constraints_info:
            print(f"{constraint_info[0]}: {constraint_info[1]} -> {constraint_info[2]}")


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cursor = conn.cursor()

    # Створюємо таблиці
    create_tables(conn, cursor)

    # Вставляємо дані
    insert_data(conn, cursor)

    # Виводимо інформацію про таблиці
    display_tables_info(conn, cursor)

    # Закриваємо з'єднання
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
