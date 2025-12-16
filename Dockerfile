FROM python:3.12-slim

WORKDIR /app/worker

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./ /app/

CMD ["python", "-u", "main.py"]