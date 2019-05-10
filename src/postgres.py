import psycopg2


def py_connection():
    global _connection

    try:
        _connection
    except NameError:
        _connection = psycopg2.connect(
            host='localhost',
            database='database_name',
            user='username',
            password='password'
        )

    return _connection


def py_cursor(func):
    def wrapped(*args, **kwargs):
        with py_connection() as connection:
            with connection.cursor() as cursor:
                return func(cursor=cursor, *args, **kwargs)

    return wrapped
