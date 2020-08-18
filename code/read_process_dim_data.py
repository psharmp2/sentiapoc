from pyspark.sql.types import StructType,StructField,StringType,LongType
from pyspark.sql.functions import col

def read_dimension_data():

    df_paymentTypes = spark.read.json("/mnt/taxisource/rawdata/PaymentTypes.json")
    df_paymentTypes.createOrReplaceGlobalTempView("DimPaymentTypes")
    
    #Store the DataFrame as an Unmanaged Table
    df_paymentTypes\
        .write\
        .mode(SaveMode.Overwrite)\
        .option("path", "/mnt/taxisource/DimensionalModel/Dimensions/PaymentTypesDimension.parquet")\  
        .saveAsTable("TaxiServiceWarehouse.DimPaymentTypes") 
    print("Saved Payment Types dataframe as a dimension and unmanaged table")

    # creating schema
    fhvBasesSchema = StructType([
    StructField("License Number", StringType(), True),
    StructField("Entity Name", StringType(), True),
    StructField("Telephone Number", LongType(), True),
    StructField("SHL Endorsed", StringType(), True),
    StructField("Type of Base", StringType(), True),
    
    StructField("Address", 
                StructType([
                    StructField("Building", StringType(), True),
                    StructField("Street", StringType(), True), 
                    StructField("City", StringType(), True), 
                    StructField("State", StringType(), True), 
                    StructField("Postcode", StringType(), True)]),
                True
               ),
                
    StructField("GeoLocation", 
                StructType([
                    StructField("Latitude", StringType(), True),
                    StructField("Longitude", StringType(), True), 
                    StructField("Location", StringType(), True)]),
                True
              )  
          ]
        )
    df_fhvBases = spark.read\
    .schema(fhvBasesSchema)\
    .option("multiline", "true")\
    .json("/mnt/taxisource/rawdata/FhvBases.json")

    # Flatten out structure
    df_flatfhvBases = df_fhvBases\
                        .select(
                                  col("License Number".alias("BaseLicenseNumber")),
                                  col("Entity Name".alias("EntityName")),
                                  col("Telephone Number".alias("TelephoneNumber")),
                                  col("SHL Endorsed".alias("ShlEndorsed")),
                                  col("Type of Base".alias("BaseType")),

                                  col("Address.Building".alias("AddressBuilding")),
                                  col("Address.Street".alias("AddressStreet")),
                                  col("Address.City".alias("AddressCity")),
                                  col("Address.State".alias("AddressState")),
                                  col("Address.Postcode".alias("AddressPostCode")),
                          
                                  col("GeoLocation.Latitude".alias("GeoLocationLatitude")),
                                  col("GeoLocation.Longitude".alias("GeoLocationLongitude")),
                                  col("GeoLocation.Location".alias("GeoLocationLocation"))
                               )

    print("Extracted FHV Bases data")
    df_flatfhvBases.createOrReplaceGlobalTempView("DimFHVBases")
#todo: add the correct adls location and mount it
    # Store the DataFrame as an Unmanaged Table
    df_flatfhvBases\
    .write\
    .mode(SaveMode.Overwrite)\
    .option("path", "/mnt/taxisource/DimensionalModel/Dimensions/FHVBasesDimension.parquet") \   
    .saveAsTable("TaxiServiceWarehouse.DimFHVBases") 
    print("Saved FHVBases dataframe as a dimension and unmanaged table")

    #read Rate Codes data
    df_rateCodes = spark.read.json("/mnt/taxisource/rawdata/RateCodes.json")
    df_rateCodes.createOrReplaceGlobalTempView("DimRateCodes")
    
    #Store the DataFrame as an Unmanaged Table
    df_rateCodes\
        .write\
        .mode(SaveMode.Overwrite)\
        .option("path", "/mnt/taxisource/DimensionalModel/Dimensions/RateCodesDimension.parquet")\  
        .saveAsTable("TaxiServiceWarehouse.DimPaymentTypes") 
    print("Saved Rate Codes dataframe as a dimension and unmanaged table")

    df_taxiZones = spark\
        .read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/mnt/taxisource/TaxiZones.csv")
    print("Extracted Taxi Zones data")

    df_taxiZones.createOrReplaceGlobalTempView("DimTaxiZones")
    print("Saved Taxi Zones dimension as a global temp view")

    #  Store the DataFrame as an Unmanaged Table
    df_taxiZones\
    .write\
    .mode(SaveMode.Overwrite)\
    .option("path", "/mnt/dataxisourcetalake/DimensionalModel/Dimensions/TaxiZonesDimension.parquet") \  
    .saveAsTable("TaxiServiceWarehouse.DimTaxiZones") 
    print("Saved Taxi Zones dataframe as a dimension and unmanaged table")