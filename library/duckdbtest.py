import duckdb
duckdb.sql('select 24').show()

# with duckdb.connect('test.db') as con:
#     con.sql("CREATE TABLE test (i INTEGER)")
#     con.sql("INSERT INTO test VALUES (42)")
#     con.table("test").show()


new_conn = duckdb.connect('test.db')
new_conn.sql("INSERT INTO test VALUES (42), (43), (43), (44)")
new_conn.sql('select * from test;').show()

new_conn.close()
