from kafka import KafkaConsumer
import psycopg2
import json

from fraud_detector import check_fraud

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

    is_fraud, fraud_score, reason = check_fraud(data)

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

    inserted = cursor.rowcount

    conn.commit()

    if inserted:

        print(
            f"Inserted Transaction: "
            f"{data['transaction_id']}"
        )

        if is_fraud:

            cursor.execute(
                """
                INSERT INTO fraud_alerts
                (
                    transaction_id,
                    fraud_score,
                    reason
                )
                VALUES (%s,%s,%s)
                """,
                (
                    data["transaction_id"],
                    fraud_score,
                    reason
                )
            )

            conn.commit()

            print(
                f"FRAUD ALERT -> "
                f"{data['transaction_id']} "
                f"Score={fraud_score} "
                f"Reason={reason}"
            )

    else:

        print(
            f"Skipped Duplicate: "
            f"{data['transaction_id']}"
        )