import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

# Helper to run SQL
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Glue boilerplate
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Data quality rules
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Load dynamic frames
accelerometer_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="accelerometer _landingaccelerometer",
    transformation_ctx="accelerometer_df"
)

customer_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="customer_landingcustomers",
    transformation_ctx="customer_df"
)

# SQL Query with deduplication
query = """
SELECT accel.*
FROM accel
JOIN (
    SELECT DISTINCT email
    FROM cust
    WHERE sharewithresearchasofdate IS NOT NULL
) AS deduped_cust
ON accel.user = deduped_cust.email
"""

filtered_df = sparkSqlQuery(
    glueContext,
    query=query,
    mapping={"accel": accelerometer_df, "cust": customer_df},
    transformation_ctx="filtered_df"
)

# Data Quality Evaluation (optional)
EvaluateDataQuality().process_rows(
    frame=filtered_df,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# Coalesce to one partition
coalesced_df = DynamicFrame.fromDF(
    filtered_df.toDF().coalesce(1), glueContext, "coalesced_df"
)

# Write to S3 as single JSON file
glueContext.write_dynamic_frame.from_options(
    frame=coalesced_df,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-zainababbas/trusted/accelerometer/",
        "partitionKeys": []
    },
    transformation_ctx="s3_output"
)

job.commit()