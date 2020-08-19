from pyspark.sql.types import StructType,StructField,StringType,LongType,TimestampType,IntegerType
from pyspark.sql.functions import col

def process_fhv_taxi_data(processed_month):
     #Define schema for FHV taxi trip file
    fhvTaxiTripSchema = StructType([
                    
                          StructField("Pickup_DateTime", TimestampType(), True),
                          StructField("DropOff_datetime", TimestampType(), True),
                          StructField("PUlocationID", IntegerType(), True),
                          StructField("DOlocationID", IntegerType(), True),
                          StructField("SR_Flag", IntegerType(), True),
                          StructField("Dispatching_base_number", StringType(), True),
                          StructField("Dispatching_base_num", StringType(), True)
                        ]
                    )
    print("Defined schema for FHV Taxi data")

    print("Starting to extract FHV Taxi data from multiple files")

# Extract data from multiple FHV files
    df_fhvTaxiTripData = spark\
            .option("header", "true")\
            .schema(fhvTaxiTripSchema)\
            .csv("/mnt/taxisource/FHVTaxiTripData_{}_*.csv".format(processed_month))
    print("Extracted FHV Taxi data")
    print("Starting cleanup and transformation on FHV Taxi data")

    #  Apply transformations to FHV trip data
    df_fhvTaxiTripData = df_fhvTaxiTripData\

                        # Clean and filter the data
                            .na.drop(Seq("PULocationID", "DOLocationID"))\
                        .dropDuplicates()  \                      

                        # Select only limited columns
                        .select(col("Pickup_DateTime".alias("PickupTime")\                        
                                  ,col("DropOff_DateTime")\
                                  ,col("PUlocationID")\ 
                                  ,col("DOlocationID")\ 
                                  ,col("SR_Flag")\
                                  ,col("Dispatching_base_number")\
                               )\

                        #  Rename the columns
                        .withColumnRenamed("DropOff_DateTime", "DropTime")\
                        .withColumnRenamed("PUlocationID", "PickupLocationId")\
                        .withColumnRenamed("DOlocationID", "DropLocationId")\
                        .withColumnRenamed("Dispatching_base_number", "BaseLicenseNumber")\

                        # Create derived columns for year, month and day
                        .withColumn("TripYear", year(col("PickupTime")))\
                        .withColumn("TripMonth", month(col("PickupTime")))\
                        .select(\
                                  "*", \
                                  dayofmonth(col("PickupTime").)alias("TripDay")\
                               )\

                        #  Create a derived column - Trip time in minutes
                        .withColumn("TripTimeInMinutes", \
                                        round(\
                                                (unix_timestamp(col("DropTime")) - unix_timestamp(col("PickupTime")))\
                                                    / 60\
                                             )\
                                   )\

                        #  Create a derived column - Trip type, and drop SR_Flag column
                        .withColumn("TripType", \
                                        when(\
                                                col("SR_Flag" === 1)\
                                                  ,"SharedTrip"\
                                            )\
                                        .otherwise("SoloTrip")\
                                   )\
                        .drop("SR_Flag")\
    print("Cleaned up and applied transformations on FHV Taxi data")

    df_fhvTaxiTripData.createOrReplaceGlobalTempView("FactFhvTaxiTripData")
    print("Saved FHV Taxi fact as a global temp view")

    print("Starting to save FHV Taxi dataframe as a fact and unmanaged table")

    #  Store the DataFrame as an Unmanaged Table
    fhvTaxiTripDataDF\
        .write\
        .mode(SaveMode.Overwrite)\
        .option("path", "/mnt/taxisource/DimensionalModel/Facts/FhvTaxiFact.parquet")\
        .saveAsTable("TaxiServiceWarehouse.FactFhvTaxiTripData") 

    println("Saved FHV Taxi dataframe as a fact and unmanaged table")

