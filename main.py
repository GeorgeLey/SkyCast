from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# In hdfs it is organized as follows:
#     skycast-data/
#     |-- Aeolus/
#         |-- Flight_Tab
#         |-- Flight_chain
#         |-- Flight_network
#     |-- daily_weather.csv

DATA = "skycast-data"

spark = SparkSession.builder.appName("SkyCast").getOrCreate()

FLIGHT_SCHEMA = (
    "FL_DATE TIMESTAMP, OP_CARRIER STRING, OP_CARRIER_FL_NUM DOUBLE, ORIGIN STRING, DEST STRING, "
    "CRS_DEP_TIME TIMESTAMP, DEP_TIME TIMESTAMP, DEP_DELAY DOUBLE, TAXI_OUT DOUBLE, "
    "WHEELS_OFF TIMESTAMP, WHEELS_ON TIMESTAMP, TAXI_IN DOUBLE, CRS_ARR_TIME TIMESTAMP, "
    "ARR_TIME TIMESTAMP, ARR_DELAY DOUBLE, CRS_ELAPSED_TIME DOUBLE, ACTUAL_ELAPSED_TIME DOUBLE, "
    "AIR_TIME DOUBLE, FLIGHTS DOUBLE, MONTH INT, DAY_OF_MONTH INT, DAY_OF_WEEK INT, "
    "ORIGIN_INDEX INT, DEST_INDEX INT, O_TEMP DOUBLE, O_PRCP DOUBLE, O_WSPD DOUBLE, "
    "D_TEMP DOUBLE, D_PRCP DOUBLE, D_WSPD DOUBLE, O_LATITUDE DOUBLE, O_LONGITUDE DOUBLE, "
    "D_LATITUDE DOUBLE, D_LONGITUDE DOUBLE"
)

flights_df = spark.read.csv(f"{DATA}/Aeolus/Flight_Tab/*.csv", header=True, schema=FLIGHT_SCHEMA).withColumn("FL_DATE", F.to_date("FL_DATE"))
weather_df = spark.read.csv(f"{DATA}/daily_weather.csv", header=True, inferSchema=True).withColumn("date", F.to_date("date"))

# Add noaa_ in front of every column except date
weather_df = weather_df.toDF(*[c if c == "date" else f"noaa_{c}" for c in weather_df.columns])





