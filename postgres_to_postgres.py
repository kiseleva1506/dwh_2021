import psycopg2


def save_file(conn_string, table_name):
    with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
        q = f"COPY {table_name} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open(f'{table_name}.csv', 'w') as f:
            cursor.copy_expert(q, f)


def load_file(conn_string, table_name):
    with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
        q = f"COPY {table_name} from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open(f'{table_name}.csv', 'r') as f:
            cursor.copy_expert(q, f)


if __name__ == '__main__':

    tables = ["customer", "lineitem", "nation", "orders",
              "part", "partsupp", "region", "supplier"]

    # saving
    conn_string_source = "host='localhost' port=54320 dbname='my_database' user='root' password='postgres'"
    for table in tables:
        save_file(conn_string_source, table)

    # loading
    conn_string_sink = "host='localhost' port=5433 dbname='my_database' user='root' password='postgres'"
    for table in tables:
        load_file(conn_string_sink, table)
