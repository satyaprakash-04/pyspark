import duckdb
from faker import Faker
from random import randrange
from datetime import datetime


def generate_timestamp():
    now = datetime.now()
    return datetime.strftime(now, '%Y-%m-%d %H:%M:%S.%f')


class InsertQueryGenerator:
    accepted_data_types = ('PRIMARY KEY', 'DATE', 'FLOAT', 'DATETIME', 'VARCHAR', 'INTEGER')

    def __init__(self, table_name: str, columns: list, sequence: list, data: list[list]):
        self.table_name = table_name
        self.columns = columns
        self.data = data
        self.sequence = sequence

    def generate_query(self):
        query = """INSERT INTO %s (%s) values
        %s;
        """ % (self.table_name, self._get_columns(), self._values_str())
        return query

    def _get_columns(self):
        return ', '.join(tuple(self.columns))

    def _values_str(self):
        value_str_list = []
        for value in self.data:
            value.insert(self.sequence[1], 'nextval(\'{}\')'.format(str(self.sequence[0])))
            value_str_list.append(str(tuple(value)).replace('"', ''))
        return ',\n'.join(value_str_list)

    def execute_query(self):
        conn = duckdb.connect('test.db')
        query = self.generate_query()
        conn.sql(str(query))
        conn.sql('select * from employees;').show()
        conn.close()


if __name__ == '__main__':
    fake = Faker()
    row_length = int(input('Enter number of records you want to insert: '))
    main_data = []
    for _ in range(row_length):
        lis_data = [fake.name(), fake.address().replace('\n', ', '), fake.phone_number(), fake.email(), randrange(10000, 100000),
                    generate_timestamp()]
        main_data.append(lis_data)
    query_gen = InsertQueryGenerator(
        table_name='employees',
        columns=['id', 'name', 'address', 'phone', 'email', 'salary', 'last_login'],
        sequence=['emp_id', 0],
        data=main_data
    )
    # print(query_gen.generate_query())
    query_gen.execute_query()
