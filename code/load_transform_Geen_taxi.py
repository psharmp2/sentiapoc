from pyspark.sql.types import StructType,StructField,StringType,LongType,TimestampType,IntegerType
from pyspark.sql.functions import col,create_map

def process_green_taxi_data(processed_month):
    println("Starting to extract Green Taxi data")

    #  Extract and clean Green Taxi Data
    defaultValueMapForGreenTaxi = create_map(lit("payment_type") ,lit(5),lit("RatecodeID"),lit( 1 ))

    df_greenTaxiTripData = spark\
                                .read\
                                .option("header", "true")\
                                .option("inferSchema", "true")  \                            
                                .option("delimiter", "\t")  \  
                                .csv(s"/mnt/taxisource/lake/GreenTaxiTripData_{}.csv".format(processed_month))

    df_greenTaxiTripData = df_greenTaxiTripData\
                                .where("passenger_count > 0")\
                                .filter(col("trip_distance" > 0.0))\

                                .dropna(Seq("PULocationID", "DOLocationID"))\

                                .fillna(defaultValueMapForGreenTaxi)\

                                .dropDuplicates()                              
    println("Extracted and cleaned Green Taxi data")

    println("Starting transformation on Green Taxi data")
    # Apply transformations to Green taxi data
    df_greenTaxiTripData = df_reenTaxiTripData\
                            # Select only limited columns
                            .select(\
                                    col("VendorID")\
                                    ,col("passenger_count".alias("PassengerCount"))\
                                    ,col("trip_distance".alias("TripDistance"))\
                                    ,col("lpep_pickup_datetime".alias("PickupTime"))  \                        
                                    ,col("lpep_dropoff_datetime".alias("DropTime"))\
                                    ,col("PUlocationID".alias("PickupLocationId"))\
                                    ,col("DOlocationID".alias("DropLocationId"))\
                                    ,col("RatecodeID"))\ 
                                    ,col("total_amount".alias("TotalAmount"))\
                                    ,col("payment_type".alias("PaymentType"))\
                                )\

                            # Create derived columns for year, month and day
                            .withColumn("TripYear", year(col("PickupTime"))))\
                            .withColumn("TripMonth", month(col("PickupTime"))\
                            .withColumn("TripDay", dayofmonth(col("PickupTime")))\
                            
                            # Create a derived column - Trip time in minutes
                            .withColumn("TripTimeInMinutes", \
                                            round(\
                                                    (unix_timestamp(col("DropTime") )- unix_timestamp(col("PickupTime")) )
                                                        / 60\
                                                )\
                                    )\
                            #  Create a derived column - Trip type, and drop SR_Flag column
                            .withColumn("TripType", \
                                            when(\
                                                    col("RatecodeID") === 6,\
                                                    "SharedTrip"\
                                                )\
                                            .otherwise("SoloTrip")\
                                    )\
                            .drop("RatecodeID")
    print("Applied transformations on Green Taxi data")

    df_greenTaxiTripData.createOrReplaceGlobalTempView("FactGreenTaxiTripData")
    print("Saved Green Taxi fact as a global temp view")

    println("Starting to save Green Taxi dataframe as a fact and unmanaged table")

# // Store the DataFrame as an Unmanaged Table
    df_greenTaxiTripData\
        .write\
        .mode(SaveMode.Overwrite)\
        .option("path", "/mnt/taxisource/DimensionalModel/Facts/GreenTaxiFact.parquet")   \ 
        .saveAsTable("TaxiServiceWarehouse.FactGreenTaxiTripData") 

    println("Saved Green Taxi dataframe as a fact and unmanaged table")