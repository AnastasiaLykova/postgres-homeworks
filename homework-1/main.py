"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import os
import psycopg2


def csv_to_list(path):
    """
    Возвращает файл csv в виде списка
    """
    with open(path, "r", encoding="utf-8") as file:
        data_list = []
        reader = csv.reader(file)
        for row in reader:
            data_list.append(row)
    return data_list[1:]


postgres_key = os.getenv('POSTGRES_KEY')
with psycopg2.connect(host="localhost", database="north", user="postgres", password=postgres_key) as conn:

    with conn.cursor() as cur:
        cur.executemany("INSERT INTO customers_data VALUES (%s, %s, %s)",
                        csv_to_list(os.path.join("north_data", ".", "customers_data.csv")))

        cur.executemany("INSERT INTO employees_data VALUES (%s, %s, %s, %s, %s, %s)",
                        csv_to_list(os.path.join("north_data", ".", "employees_data.csv")))

        cur.executemany("INSERT INTO orders_data VALUES (%s, %s, %s, %s, %s)",
                        csv_to_list(os.path.join("north_data", ".", "orders_data.csv")))

conn.close()
