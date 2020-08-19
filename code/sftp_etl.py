from pyspark.sql.types import StructType
from pyspark.sql.functions import col

def sftp_etl():
    database = 'sftp_source'
    table_name = 'Movies_DB'
    df_movies_data = spark\
                    .read\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .csv("/mnt/sftp/movies_DB.csv")

    print("Extracted movies data")
    spark.sql("create database if not exists adb_{}".format(database))
    spark.sql("use adb_{}".format(database))
    df_transformed = df_movies_data.select("movie","title","genres","year","Rating")
    df_transformed.write.mode('overwrite').saveAsTable(table_name)
    print("Saved data from sftp source and unmanaged table")
