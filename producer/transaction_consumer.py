from kafka import KafkaConsumer
import psycopg2
import json

consumer = KafkaConsumer(
    "transactions_raw",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

conn = psycopg2.connect(
    host="localhost",
    database="payment_db",
    user="admin",
    password="admin123"
)

cursor = conn.cursor()

print("Consumer Started...")

for message in consumer:

    data = message.value

    cursor.execute(
    """
    INSERT INTO transactions
    (
        transaction_id,
        user_id,
        merchant_id,
        amount,
        payment_type,
        city,
        transaction_timestamp
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """,
    (
        data["transaction_id"],
        data["user_id"],
        data["merchant_id"],
        data["amount"],
        data["payment_type"],
        data["city"],
        data["transaction_timestamp"]
    )
)
    conn.commit()

    print(
        f"Inserted Transaction: "
        f"{data['transaction_id']}"
    )