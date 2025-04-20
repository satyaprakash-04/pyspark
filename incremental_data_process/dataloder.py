import duckdb


class InsertQueryGenerator:
    accepted_data_types = ('PRIMARY KEY', 'DATE', 'FLOAT', 'DATETIME', 'VARCHAR', 'INTEGER')

    def __init__(self, table_name: str, columns: list, data: list[list]):
        self.table_name = table_name
        self.columns = columns
        self.data = data

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
            value_str_list.append(str(tuple(value)))
        return ',\n'.join(value_str_list)

    def execute_query(self):
        with duckdb.connect('test.db') as conn:
            resp = conn.execute(self.generate_query())
            return resp


if __name__ == '__main__':

    query_gen = InsertQueryGenerator(
        table_name='employees',
        columns=['name', 'address', 'phone', 'email', 'salary', 'createddate', 'updateddate'],
        data=[
            ['satya', 'balia', '7787881916', 'sahoosatyaprakash152@gmail.com', 20000],
            ['bapu', 'balia', '7787881916', 'sahoosatyaprakash152@gmail.com', 3000]
        ]
    )
    print(query_gen.generate_query())

