from datetime import datetime


def check_fraud(transaction):

    amount = float(transaction["amount"])

    timestamp = datetime.fromisoformat(
        transaction["transaction_timestamp"]
    )

    hour = timestamp.hour

    if amount > 4500:
        return (
            True,
            95,
            "Suspicious Very High Amount"
        )

    elif amount > 4000:
        return (
            True,
            80,
            "High Amount Transaction"
        )

    elif 0 <= hour <= 4:
        return (
            True,
            70,
            "Transaction During Unusual Hours"
        )

    return (
        False,
        0,
        None
    )
    