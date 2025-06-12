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
AWSGlueDataCatalog_node1749616148891 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_landingcustomers", transformation_ctx="AWSGlueDataCatalog_node1749616148891")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT * 
FROM myDataSource
WHERE sharewithresearchasofdate IS NOT NULL

'''
SQLQuery_node1749616161218 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AWSGlueDataCatalog_node1749616148891}, transformation_ctx = "SQLQuery_node1749616161218")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1749616161218, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749616137759", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749616189821 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1749616161218, connection_type="s3", format="json", connection_options={"path": "s3://stedi-zainababbas/customers/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1749616189821")

job.commit()