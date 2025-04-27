import duckdb
duckdb.sql('select 24').show()

# with duckdb.connect('test.db') as con:
#     con.sql("CREATE TABLE test (i INTEGER)")
#     con.sql("INSERT INTO test VALUES (42)")
#     con.table("test").show()


new_conn = duckdb.connect('test.db')
# new_conn.sql("INSERT INTO test VALUES (42), (43), (43), (44)")
# new_conn.sql('select * from test;').show()
#

# new_conn.sql('CREATE SEQUENCE emp_id INCREMENT 1 START 1;')

# new_conn.sql("""CREATE TABLE employees (
#     id INTEGER PRIMARY KEY,
#     name VARCHAR(54),
#     address VARCHAR(244),
#     phone VARCHAR(15),
#     email VARCHAR(244),
#     salary DECIMAL(10, 2),
#     last_login TIMESTAMP
# );""")
# new_conn.sql("""INSERT INTO employees (id, name, address, phone, email, salary, last_login) values
#         (nextval('emp_id'), 'satya', 'balia', '7787881916', 'sahoosatyaprakash152@gmail.com', 20000, '2025-04-26 12:34:56'),
# (nextval('emp_id'), 'bapu', 'balia', '7787881916', 'sahoosatyaprakash152@gmail.com', 3000, '2025-04-26 12:34:56');""")
new_conn.sql('show tables;').show()
new_conn.sql('select * from employees;').show()
# new_conn.sql('delete from employees;')
new_conn.close()
