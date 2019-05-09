from io import StringIO

from postgres import cursor, connection


def generate_insert_placeholders(keys):
    return ', '.join(list(map(lambda key: f'%s', keys)))


def generate_update_setters(keys):
    return ', '.join(list(map(lambda key: f'{key} = %s', keys)))


class BulkQueries(object):
    def __init__(self, columns, table):
        self.table = table
        self.columns = columns
        self.insert_rows = []
        self.update_rows = []
        self.upsert_rows = []

    @staticmethod
    def close_connection():
        connection.close()

    def insert_row(self, row):
        self.insert_rows.append(row)

    def update_row(self, row):
        self.update_rows.append(row)

    def upsert_row(self, row):
        self.upsert_rows.append(row)

    def run(self):
        if self.insert_rows:
            self.run_inserts()

        if self.update_rows:
            self.run_updates()

        if self.upsert_rows:
            self.run_upserts()

    def run_inserts(self):
        values = str()

        for row in self.insert_rows:
            values += f"{chr(29).join(list(row.values()))}\n"

        buffer = StringIO(values)
        cursor.copy_from(buffer, self.table, sep=chr(29), columns=self.columns)
        connection.commit()

    def run_updates(self):
        bulk_queries = str()
        bulk_values = []

        for row in self.update_rows:
            record_id = row.pop('id')
            values = list(row.values())
            values.append(record_id)
            setters = generate_update_setters([*row])

            query = f'UPDATE {self.table} SET {setters} WHERE id = %s;'

            bulk_queries += query
            bulk_values += values

        cursor.execute(bulk_queries, bulk_values)
        connection.commit()

    def run_upserts(self):
        bulk_queries = str()
        bulk_values = []

        for row in self.upsert_rows:
            update_columns = row.pop('update_columns')
            values = list(row.values())
            placeholders = generate_insert_placeholders([*row])

            if update_columns is not None:
                setters = generate_update_setters([*update_columns])
                values += list(map(lambda key: row[key], update_columns))

                query = f'INSERT INTO {self.table} VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET {setters};'
            else:
                query = f'INSERT INTO {self.table} VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING;'

            bulk_queries += query
            bulk_values += values

        cursor.execute(bulk_queries, bulk_values)
        connection.commit()
