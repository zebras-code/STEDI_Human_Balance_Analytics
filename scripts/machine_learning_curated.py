import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1749699727391 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="accelerometer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1749699727391"
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1749699762531 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="step_trainer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1749699762531"
)

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
    s.sensorreadingtime, 
    a.x, 
    a.y, 
    a.z, 
    a.user 
FROM 
    step AS s 
JOIN 
    accel AS a 
ON 
    s.sensorreadingtime = a.timestamp
'''

SQLQuery_node1749699874700 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step": AWSGlueDataCatalog_node1749699762531,
        "accel": AWSGlueDataCatalog_node1749699727391
    },
    transformation_ctx="SQLQuery_node1749699874700"
)

# Coalesce to 1 file
SQLQuery_node1749699874700 = DynamicFrame.fromDF(
    SQLQuery_node1749699874700.toDF().coalesce(1),
    glueContext,
    "SQLQuery_node1749699874700"
)

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(
    frame=SQLQuery_node1749699874700,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1749700551825",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

AmazonS3_node1749700977681 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1749699874700,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-zainababbas/curated/",
        "partitionKeys": []
    },
    transformation_ctx="AmazonS3_node1749700977681"
)

job.commit()
