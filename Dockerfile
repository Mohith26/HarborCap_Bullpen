FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY agents/ agents/
COPY scheduler.py .
COPY run_agent.py .

CMD ["python", "scheduler.py"]
