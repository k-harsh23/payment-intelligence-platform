from kafka import KafkaProducer
from faker import Faker
import json
import random
import time

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:

    transaction = {
    "transaction_id": fake.uuid4(),
    "user_id": str(random.randint(1000, 5000)),
    "merchant_id": str(random.randint(100, 999)),
    "amount": round(random.uniform(10, 5000), 2),
    "payment_type": random.choice(
        ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING"]
    ),
    "city": fake.city(),
    "transaction_timestamp": fake.iso8601()
}

    producer.send(
        "transactions_raw",
        value=transaction
    )

    print(f"Sent: {transaction}")

    time.sleep(1)