Write-Host ""
Write-Host "======================================="
Write-Host "Starting Payment Intelligence Platform"
Write-Host "======================================="
Write-Host ""

# Producer

Start-Process powershell `
-ArgumentList "-NoExit", "-Command", `
"cd '$PSScriptRoot\..'; python producer\transaction_producer.py"

Start-Sleep -Seconds 5

# Bronze

Start-Process powershell `
-ArgumentList "-NoExit", "-Command", `
"docker exec -it payment-spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/spark_jobs/kafka_to_bronze.py"

Start-Sleep -Seconds 10

# Silver

Start-Process powershell `
-ArgumentList "-NoExit", "-Command", `
"docker exec -it payment-spark /opt/spark/bin/spark-submit /app/spark_jobs/bronze_to_silver.py"

Start-Sleep -Seconds 10

# Gold Payment

Start-Process powershell `
-ArgumentList "-NoExit", "-Command", `
"docker exec -it payment-spark /opt/spark/bin/spark-submit /app/spark_jobs/gold_payment_metrics.py"

Start-Sleep -Seconds 5

# Gold Risk

Start-Process powershell `
-ArgumentList "-NoExit", "-Command", `
"docker exec -it payment-spark /opt/spark/bin/spark-submit /app/spark_jobs/gold_risk_metrics.py"

Start-Sleep -Seconds 5

# Gold City

Start-Process powershell `
-ArgumentList "-NoExit", "-Command", `
"docker exec -it payment-spark /opt/spark/bin/spark-submit /app/spark_jobs/gold_city_metrics.py"

Start-Sleep -Seconds 5

# Gold To PostgreSQL

Start-Process powershell `
-ArgumentList "-NoExit", "-Command", `
"docker exec -it payment-spark /opt/spark/bin/spark-submit /app/spark_jobs/gold_to_postgres.py"

Write-Host ""
Write-Host "======================================="
Write-Host "Platform Startup Complete"
Write-Host "======================================="