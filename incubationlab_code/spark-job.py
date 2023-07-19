import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, current_date, lit, concat

# Create a Spark session
spark = SparkSession.builder \
    .appName("SparkJob") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .getOrCreate()

spark.sparkContext.addPyFile("s3://jeevithalandingzonebucket/delta-core_2.12-0.8.0.jar")
from delta import *

from delta.tables import DeltaTable

class SparkJob:
    def __init__(self, spark):
        self.spark = spark

    def read_data(self, source_path):
        return self.spark.read.format("parquet").load(source_path)

    def write_data(self, data, destination_path, partition_cols=None):
        if partition_cols:
            data.write.format("parquet").mode("overwrite").partitionBy(partition_cols).save(destination_path)
        else:
            data.write.format("parquet").mode("overwrite").save(destination_path)

    def mask_columns(self, data, columns):
        for column in columns:
            if column in data.columns:
                newcolumn = 'masked_'+column
                data = data.withColumn(newcolumn, sha2(col(column), 256))
        return data

    def cast_columns(self, data, columns, cast_type):
        for column in columns:
            if cast_type == "decimal":
                data = data.withColumn(column, col(column).cast("decimal(7, 2)"))
            elif cast_type == "string":
                data = data.withColumn(column, col(column).cast("string"))
                data = data.withColumn(column, col(column).cast("string").cast("string"))
        return data

    def run_job(self, config):
        landing_zone_path = config["paths"]["landing_zone_path"]
        raw_zone_path = config["paths"]["raw_zone_path"]
        staging_zone_path = config["paths"]["staging_zone_path"]
        mask_fields = config["transformations"]["actives"]["mask_fields"]
        cast_fields = config["transformations"]["viewership"]["cast_fields"]
        casting_type = config["transformations"]["viewership"]["cast_fields"]["casting_type"]

        landing_data_actives = self.read_data(landing_zone_path["actives"])
        landing_data_viewership = self.read_data(landing_zone_path["viewership"])

        self.write_data(landing_data_actives, raw_zone_path["actives"], partition_cols=["month", "date"])
        self.write_data(landing_data_viewership, raw_zone_path["viewership"], partition_cols=["month", "date"])

        transformed_data_actives = self.read_data(raw_zone_path["actives"])
        transformed_data_viewership = self.read_data(raw_zone_path["viewership"])

        transformed_data_actives = self.mask_columns(transformed_data_actives, mask_fields)
        transformed_data_viewership = self.mask_columns(transformed_data_viewership, mask_fields)

        transformed_data_actives = self.cast_columns(transformed_data_actives, cast_fields, casting_type)
        transformed_data_viewership = self.cast_columns(transformed_data_viewership, cast_fields, casting_type)

        self.write_data(transformed_data_actives, staging_zone_path["actives"], partition_cols=["month", "date"])
        self.write_data(transformed_data_viewership, staging_zone_path["viewership"], partition_cols=["month", "date"])

        # Read the source table (df_source)
        df_source = transformed_data_actives\
        .select ("advertising_id","user_id","masked_advertising_id","masked_user_id")\
        .withColumn("end_date", lit("0000"))\
        .withColumn("start_date" ,current_date())
        
        # Specify the Delta table path
        lookup_location = "s3://jeevithastagingzonebucket/staging_data/Delta_Table/"
        delta_table = None 

        #delta_table = None  # Initialize delta_table with None
        #delta_table = DeltaTable.forPath(spark, lookup_location)

        try:
            targetTable = DeltaTable.forPath(spark,lookup_location)
            delta_df = targetTable.toDF()
        except:
            print('Table does not exist')
            df_source = df_source.withColumn("flag_active",lit("true"))
            df_source.write.format("delta").mode("overwrite").save(lookup_location)
            print('Table Created Sucessfully!')
            targetTable = DeltaTable.forPath(spark,lookup_location)
            delta_table = DeltaTable.forPath(spark, lookup_location)
            delta_df = delta_table.toDF()
            delta_df.createOrReplaceTempView("delta_table")
            #delta_df = targetTable.toDF()
            delta_df.show(100)

            try:
                merge_builder = delta_table.alias("target").merge(
                    df_source.alias("source"),
                    "target.advertising_id = source.advertising_id and target.user_id = source.user_id"
                ).whenMatchedUpdate(
                    condition="target.flag_active = true",
                    set={
                        "end_date": "source.start_date",
                        "flag_active": "false"
                    }
                ).whenNotMatchedInsert(
                    values={
                        "advertising_id": "source.advertising_id",
                        "user_id": "source.user_id",
                        "masked_advertising_id": "source.masked_advertising_id",
                        "masked_user_id": "source.masked_user_id",
                        "start_date": "source.start_date",
                        "end_date": "source.end_date",
                        "flag_active": "source.flag_active"
                    }
                )
            except:
                print('Merge operation failed!')
                raise
            else:
                merge_builder.execute()
                print('Merge operation executed successfully!')
            # Overwrite the Delta table with the merged records
            delta_table.write.format("delta").mode("overwrite").save(lookup_location)  # Fixed variable name


configData = spark.sparkContext.textFile("s3://jeevithalandingzonebucket/config.json").collect()
data = ''.join(configData)
config_details = json.loads(data)

# Run the Spark job
job = SparkJob(spark)
job.run_job(config_details)
