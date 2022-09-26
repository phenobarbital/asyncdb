import hazelcast

client = hazelcast.HazelcastClient()

# Get and fill a map with some integers
integers = client.get_map("integers").blocking()
for i in range(100):
    integers.set(i, i)

# Create mapping for the integers. This needs to be done only once per map.
client.sql.execute(
    """
CREATE MAPPING IF NOT EXISTS integers
TYPE IMap
OPTIONS (
  'keyFormat' = 'int',
  'valueFormat' = 'int'
)
    """
).result()

# Fetch values in between (40, 50)
result = client.sql.execute("SELECT * FROM integers WHERE this > ? AND this < ?", 10, 50).result()
for row in result:
    print(row)

# print(result_future)

# def on_response(sql_result_future):
#     print('HERE ')
#     it = sql_result_future.result().iterator()
#     print(it)

#     def on_next_row(row_future):
#         try:
#             row = row_future.result()
#             # Process the row.
#             print(row)

#             # Iterate over the next row.
#             next(it).add_done_callback(on_next_row)
#         except StopIteration:
#             # Exhausted the iterator. No more rows are left.
#             pass

#     next(it).add_done_callback(on_next_row)


# # Request the iterator over rows and add a callback to
# # run, when the response comes
# result_future.add_done_callback(on_response)
