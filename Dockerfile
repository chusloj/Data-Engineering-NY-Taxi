FROM python:latest

RUN apt-get install curl
RUN pip install pyarrow pandas sqlalchemy psycopg2

WORKDIR /usr/app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py"]
