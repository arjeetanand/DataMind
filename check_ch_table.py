import clickhouse_connect
client = clickhouse_connect.get_client(host="localhost", port=8123)
try:
    res = client.command("SHOW CREATE TABLE retail_events_hot")
    print(f"Table DDL:\n{res}")
    # Also check if it's empty now
    count = client.command("SELECT count() FROM retail_events_hot")
    print(f"Current Count: {count}")
finally:
    client.close()
