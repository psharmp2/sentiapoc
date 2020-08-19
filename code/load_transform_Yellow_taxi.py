from pyspark.sql.types import StructType,StructField,StringType,LongType,TimestampType,IntegerType
from pyspark.sql.functions import col,create_map

def process_yellow_taxi_data(processed_month):
     #  Extract and clean Green Taxi Data
    defaultValueMapForYellowTaxi = create_map(lit("payment_type") ,lit(5),lit("RatecodeID"),lit( 1 ))

    df_yellowTaxiTripData = spark\
                                .read\
                                .option("header", "true")\
                                .option("inferSchema", "true")  \                            
                                .option("delimiter", "\t")  \  
                                .csv(s"/mnt/taxisource/lake/YellowTaxiTripData_{}.csv".format(processed_month))

    df_yellowTaxiTripData = df_yellowTaxiTripData\
                                .where("passenger_count > 0")\
                                .filter(col("trip_distance" > 0.0))\

                                .dropna(Seq("PULocationID", "DOLocationID"))\

                                .fillna(defaultValueMapForGreenTaxi)\

                                .dropDuplicates()                              
    println("Extracted and cleaned Yellow Taxi data")
    println("Starting transformation on Yellow Taxi data")

# Apply transformations to Yellow taxi data
    df_yellowTaxiTripData = df_yellowTaxiTripData\

                            # // Select only limited columns
                            .select(col("VendorID")\
                                    ,col("passenger_count".alias("PassengerCount"))\
                                    ,col("trip_distance".alias("TripDistance"))\
                                    ,col("tpep_pickup_datetime".alias("PickupTime")) \                       
                                    ,col("tpep_dropoff_datetime".alias("DropTime")) \
                                    ,col("PUlocationID".alias("PickupLocationId"))\
                                    ,col("DOlocationID".alias("DropLocationId"))\
                                    ,col("RatecodeID") \
                                    ,col("total_amount".alias("TotalAmount"))\
                                    ,col("payment_type".alias("PaymentType"))\
                                )

                            # // Create derived columns for year, month and day
                            .withColumn("TripYear", year(col("PickupTime"))\
                            .withColumn("TripMonth", month(col("PickupTime"))\
                            .withColumn("TripDay", dayofmonth(col("PickupTime"))\
                            
                            # // Create a derived column - Trip time in minutes
                            .withColumn("TripTimeInMinutes", \
                                            round(\
                                                    (unix_timestamp(col("DropTime") - unix_timestamp(col("PickupTime")) \
                                                        / 60\
                                                )\
                                    )\

                            # // Create a derived column - Trip type, and drop SR_Flag column
                            .withColumn("TripType", \
                                            when(\
                                                    col("RatecodeID") === 6,\
                                                    "SharedTrip"\
                                                )\
                                            .otherwise("SoloTrip")\
                                    )\
                            .drop("RatecodeID")
    print("Applied transformations on Yellow Taxi data")

    df_yellowTaxiTripData.createOrReplaceGlobalTempView("FactYellowTaxiTripData")
    println("Saved Yellow Taxi fact as a global temp view")

    println("Starting to save Yellow Taxi dataframe as a fact and unmanaged table")

    # // Store the DataFrame as an Unmanaged Table
    df_yellowTaxiTripData\
        .write\
        .mode(SaveMode.Append)\
        .option("path", "/mnt/taxisource/DimensionalModel/Facts/YellowTaxiFact.parquet")
        .saveAsTable("TaxiServiceWarehouse.FactYellowTaxiTripData") 

    println("Saved Yellow Taxi dataframe as a fact and unmanaged table")