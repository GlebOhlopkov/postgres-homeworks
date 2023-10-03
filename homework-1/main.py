"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2


def transfer_customers_data():
    with open('north_data/customers_data.csv', newline='', encoding='utf-8') as csvfile:
        customers_data = csv.DictReader(csvfile)
        for data in customers_data:
            with psycopg2.connect(host='localhost', database='north', user='postgres', password='24061990') as conn:
                with conn.cursor() as cur:
                    cur.execute('INSERT INTO customers_data VALUES (%s, %s, %s)', (data['customer_id'],
                                                                                   data['company_name'],
                                                                                   data['contact_name']))


def transfer_employees_data():
    with open('north_data/employees_data.csv', newline='', encoding='utf-8') as csvfile:
        employees_data = csv.DictReader(csvfile)
        for data in employees_data:
            with psycopg2.connect(host='localhost', database='north', user='postgres', password='24061990') as conn:
                with conn.cursor() as cur:
                    cur.execute('INSERT INTO employees_data VALUES (%s, %s, %s, %s, %s, %s)', (data['employee_id'],
                                                                                               data['first_name'],
                                                                                               data['last_name'],
                                                                                               data['title'],
                                                                                               data['birth_date'],
                                                                                               data['notes']))


def transfer_orders_data():
    with open('north_data/orders_data.csv', newline='', encoding='utf-8') as csvfile:
        orders_data = csv.DictReader(csvfile)
        for data in orders_data:
            with psycopg2.connect(host='localhost', database='north', user='postgres', password='24061990') as conn:
                with conn.cursor() as cur:
                    cur.execute('INSERT INTO orders_data VALUES (%s, %s, %s, %s, %s)', (data['order_id'],
                                                                                        data['customer_id'],
                                                                                        data['employee_id'],
                                                                                        data['order_date'],
                                                                                        data['ship_city']))


if __name__ == '__main__':
    transfer_customers_data()
    transfer_employees_data()
    transfer_orders_data()
