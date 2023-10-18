import json

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")
    #
    #             add_foreign_keys(cur, json_file)
    #             print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE {db_name}")
    cur.execute(f"CREATE DATABASE {db_name}")

    conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, 'r', encoding='utf-8') as file:
        sql_text = file.read()
        cur.execute(sql_text)


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    cur.execute('''
    CREATE TABLE suppliers (
        supplier_id serial PRIMARY KEY,
        company_name varchar,
        contact varchar,
        address varchar,
        phone varchar,
        fax varchar,
        homepage varchar,
        products varchar
        );
        ''')


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, 'r', encoding='utf-8') as file:
        return json.load(file)


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    for supp in suppliers:
        cur.execute(
            """
            INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage, products)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (supp['company_name'], supp['contact'], supp['address'],
             supp['phone'], supp['fax'], supp['homepage'], supp['products'])
        )


def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    pass


if __name__ == '__main__':
    main()
