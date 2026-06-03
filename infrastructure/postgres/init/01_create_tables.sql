CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    merchant_id VARCHAR(50),
    amount DECIMAL(12,2),
    payment_type VARCHAR(20),
    city VARCHAR(100),
    transaction_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50),
    fraud_score DECIMAL(5,2),
    reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);c