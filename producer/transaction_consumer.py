from kafka import KafkaConsumer
import psycopg2
import json

# Kafka Consumer
consumer = KafkaConsumer(
    "transactions_raw",
    bootstrap_servers="localhost:9092",
    group_id="postgres-consumer",
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# PostgreSQL Connection
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

    try:
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
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id)
            DO NOTHING
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

    except Exception as e:
        print(f"Error processing transaction: {e}")
        conn.rollback()