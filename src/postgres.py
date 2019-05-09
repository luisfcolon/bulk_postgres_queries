import psycopg2


connection = psycopg2.connect(
    host='localhost',
    database='database_name',
    user='username',
    password='password'
)

cursor = connection.cursor()
