FROM python:3.12.8

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 loguru

WORKDIR /app

#Copy from local to docker image
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]