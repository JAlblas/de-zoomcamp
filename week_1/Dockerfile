FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_trip_data.py ingest_trip_data.py

ENTRYPOINT [ "python", "ingest_trip_data.py"]