FROM python:3.10

WORKDIR /assessment

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY wait-for-it.sh ./
COPY ./assessment /assessment
