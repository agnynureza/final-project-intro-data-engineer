FROM python:3-alpine

WORKDIR /app

COPY requirements.txt .

RUN python3 -m venv venv

ENV PATH="/app/venv/bin:$PATH"

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "main.py"]