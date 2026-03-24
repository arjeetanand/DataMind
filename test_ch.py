import clickhouse_connect
try:
    client = clickhouse_connect.get_client(host="localhost", port=8123)
    print("Connected to ClickHouse!")
    res = client.command("SELECT 1")
    print(f"Query Result: {res}")
    client.close()
except Exception as e:
    print(f"ClickHouse Connection Failed: {e}")
