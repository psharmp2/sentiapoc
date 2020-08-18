from pyspark.sql.types import StructType,StructField,StringType,LongType,TimestampType,IntegerType

def read_facts_data(processed_month):


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

    