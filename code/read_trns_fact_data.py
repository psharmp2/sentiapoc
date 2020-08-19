from pyspark.sql.types import StructType,StructField,StringType,LongType,TimestampType,IntegerType
from pyspark.sql.functions import col
from code.load_transform_FHV_taxi import process_fhv_taxi_data
from code.load_transform_Green_taxi import process_green_taxi_data
from code.load_transform_Yellow_taxi import process_yellow_taxi_data

def read_facts_data(processed_month):
    process_fhv_taxi_data(processed_month)
    process_green_taxi_data(processed_month)
    process_yellow_taxi_data(processed_month)



   


    