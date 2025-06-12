import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load trusted customer data
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted"
)

# Load trusted accelerometer data
accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted"
)

# Convert to Spark DataFrames
customer_df = customer_trusted.toDF()
accel_df = accelerometer_trusted.toDF()

# Create temp views for SQL
customer_df.createOrReplaceTempView("customers")
accel_df.createOrReplaceTempView("accels")

# SQL join to find customers with accelerometer data
joined_df = spark.sql("""
    SELECT DISTINCT c.*
    FROM customers c
    INNER JOIN accels a ON c.email = a.user
""")

# Reduce to one partition
joined_df_single_file = joined_df.coalesce(1)

# Convert back to DynamicFrame
customers_curated = DynamicFrame.fromDF(joined_df_single_file, glueContext, "customers_curated")

# Write to S3 as one JSON file
glueContext.write_dynamic_frame.from_options(
    frame=customers_curated,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-zainababbas/curated/customers/",
        "partitionKeys": []
    },
    transformation_ctx="customers_curated_output"
)

job.commit()
