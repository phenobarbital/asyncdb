from clickhouse_driver import Client

# Connect to ClickHouse server
client = Client(host='localhost', user='default', password='u69ebsZQ')

# Check ClickHouse version
version = client.execute('SELECT version()')
print(f'ClickHouse version: {version[0][0]}')

# Create a table
client.execute('CREATE TABLE IF NOT EXISTS test_table (id UInt32, name String) ENGINE = Memory')

# Insert data into the table
client.execute('INSERT INTO test_table VALUES', [(1, 'Alice'), (2, 'Bob')])

# Query the data
rows = client.execute('SELECT * FROM test_table')

# Print the rows
for row in rows:
    print(row)

# Close the connection
client.disconnect()
