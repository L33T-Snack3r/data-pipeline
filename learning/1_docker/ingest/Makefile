URL=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
#URL=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv

docker-build:
	docker build -t taxi_ingest:v001 .
ingest-run:
	winpty docker run -it --network=db_default taxi_ingest:v001 --table_name=green_taxi_trips --host=pgdatabase --url=$(URL)