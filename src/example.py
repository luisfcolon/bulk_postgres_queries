PERSONS_TABLE = 'users'

PERSONS_COLUMNS = [
    'id',
    'firstname',
    'lastname',
    'dob',
    'ssn',
    'age',
    'gender'
]

PERSONS_UPDATABLE_COLUMNS = [
    'firstname',
    'lastname',
    'age'
]

# insert or update dictionary format
person = {
    'id': 1,
    'firstname': 'John',
    'lastname': 'Smith',
    'dob': '01/10/1976',
    'ssn': '222-00-3333',
    'age': 42,
    'gender': 'fluid'
}

# upsert dictionary format
person_upsert = {
    'id': 1,
    'firstname': 'John',
    'lastname': 'Smith',
    'dob': '01/10/1976',
    'ssn': '222-00-3333',
    'age': 42,
    'gender': 'fluid',
    'update_columns': PERSONS_UPDATABLE_COLUMNS
}

"""
persons_bulk_queries = BulkQueries(table=PERSONS_TABLE, columns=PERSONS_UPDATABLE_COLUMNS)
persons_bulk_queries.insert_row(person)
persons_bulk_queries.update_row(person)
persons_bulk_queries.upsert_row(person_upsert)

persons_bulk_queries.run()
persons_bulk_queries.close_connection()
"""
