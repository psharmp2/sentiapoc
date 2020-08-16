from pyspark.sql.types import StructType
from pyspark.sql.functions import col


def streaming_etl(namespaceConnectionString,eventHubName):
    
    # Event Hub Connection String
    eventHubConnectionString = namespaceConnectionString + ";EntityPath=" + eventHubName

    # Event Hub Configuration
    eventHubConfiguration = {
    'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventHubConnectionString)  
    }

    print("Event hub connection string = " + namespaceConnectionString)
    print("Event hub name = " + eventHubName)


    # Create a Streaming DataFrame
    inputDF = (
                spark
                    .readStream
                    .format("eventhubs")
                    .options(**eventHubConfiguration)
                    .load()
            )

    # Defining schema
    schema = (
                StructType()
                .add("Id", "integer")
                .add("VendorId", "integer")
                .add("PickupTime", "timestamp")
                .add("CabLicense", "string")
                .add("DriverLicense", "string")
                .add("PickupLocationId", "integer")
                .add("PassengerCount", "integer")
                .add("RateCodeId", "integer")
            )

    # Extract TaxiZones static data
    taxiZones = (
                    spark
                        .read
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .csv("/mnt/datalake/StaticData/TaxiZones.csv")
                )

    taxiZones.createOrReplaceTempView("TaxiZones")

    #Create Raw DataFrame
    rawDF = (
                inputDF
            
                    # Convert body of event to string format
                    .withColumn(
                                "rawdata",
                                col("body").cast("string")
                            )
    
                    # Convert JSON data from string to defined schema
                    .select(
                            from_json(
                                        col("rawdata"),
                                        schema
                                    )
                            .alias("taxidata")
                        )
    
                    # Extract individual columns from JSON structure
                    .select(
                            "taxidata.Id",
                            "taxidata.VendorId",
                            "taxidata.PickupTime",
                            "taxidata.CabLicense",
                            "taxidata.DriverLicense",
                            "taxidata.PickupLocationId",
                            "taxidata.PassengerCount",
                            "taxidata.RateCodeId",
                        )
            )
    #Create Processed DataFrame
    transformedDF = (
                        rawDF
    
                            # Add TripType column and drop RateCodeId column
                            .withColumn("TripType",
                                            when(
                                                    col("RateCodeId") == "6",
                                                        "SharedTrip"
                                                )
                                            .otherwise("SoloTrip")
                                    )  
                            .drop("RateCodeId")
    
                            # Filter by PassengerCount
                            .where("PassengerCount > 0")
                    )

    transformedDF.createOrReplaceTempView("ProcessedTaxiData")

    # Running stream pipeline for Raw data
    rawStreamingFileQuery = (
                                rawDF                             
                                    .writeStream
                                    .queryName("RawTaxiQuery")
                                    .format("csv")
                                    .option("path", "/mnt/datalake/Raw/")
                                    .option("checkpointLocation", "/mnt/datalake/checkpointRaw")
                                    .trigger(processingTime = '10 seconds')                                
                                    .start()  
                            )

    # Running stream pipeline for Processed data
    processedStreamingFileQuery = (
                                    transformedDF                             
                                        .writeStream
                                        .queryName("ProcessedTaxiQuery")
                                        .format("parquet")
                                        .option("path", "/mnt/datalake/Processed/")
                                        .option("checkpointLocation", "/mnt/datalake/checkpointProcessed")
                                        .trigger(processingTime = '3 seconds')
                                        .start()  
                                )
