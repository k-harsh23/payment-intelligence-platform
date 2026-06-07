FROM python:3.11-slim

WORKDIR /app

COPY dashboard/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "/app/dashboard/app.py", "--server.address=0.0.0.0"]