import sys
from pyspark.context import SparkContext
from code.streaming_etl import streaming_etl
from code.sftp_etl import sftp_etl
from code.taxisource_etl import taxisource_etl

try:
    # Namespace Connection String
    namespaceConnectionString = dbutils.widgets.get("EventHubNamespaceConnectionString")

    # Event Hub Name
    eventHubName = dbutils.widgets.get("EventHubName")

    dbutils.widgets.get("source_files")
    dbutils.widgets.get("processed_month")

    processed_month = getArgument("processed_month")
    source = getArgument("source_files")
    #Authenticate
    blob_container_name = get_blob_container()
    storage_account_name = get_storage_account()
    key_scope_name = get_key_scope()
    branch = get_branch()
    mount_folder= ''
    
    secret_key = dbutils.secrets.get(scope = "{}".format(key_scope_name),key = "sentia-storageacckey1-secret")
    spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(storage_account_name),secret_key)

    #Initialize
    spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
    dbutils.fs.ls("abfss://{}@{}.dfs.core.windows.net/".format(blob_container_name,storage_account_name))

    try:
        dbutils.fs.ls("/mnt/{}".format(blob_container_name))
        print("Blob container is mounted")
    except Exception as err:
        dbutils.fs.mount( source = "wasbs://{}@{}.blob.core.windows.net/{}".format(blob_container_name,storage_account_name,mount_folder),mount_point = "/mnt/{}/{}".format(blob_container_name,mount_folder), extra_configs = {"fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name):secret_key})

    if source == 'Streaming_data':
        streaming_etl(namespaceConnectionString,eventHubName)

    if source == 'sftp':
        sftp_etl()  

    if source == 'taxisource' :
        taxisource_etl(processed_month)

