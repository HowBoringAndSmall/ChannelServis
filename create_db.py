import psycopg2
from config import host, user, password, db_name


try:
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name
    )
    connection.autocommit = True
    # создание новой таблицы
    with connection.cursor() as cursor:
        cursor.execute(
            """CREATE TABLE test(
            number smallserial,
            order_number serial,
            cost_dollars serial,
            delivery_time date,
            cost_rubles numeric,
            CONSTRAINT number_unique UNIQUE (number)
            )"""
        )
        print("[INFO] table created successfuly")
finally:
    connection.close()
    print("[INFO] PostgreSQL connection close")