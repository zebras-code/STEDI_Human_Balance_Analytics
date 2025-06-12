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

# Default DQ rules
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Source: step_trainer landing table
step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="step_trainer_landing_step_trainer",
    transformation_ctx="step_trainer_landing"
)

#  Source: curated customer table
customers_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="customers_curated",
    transformation_ctx="customers_curated"
)

#  SQL Join on serialnumber
sql_query = """
SELECT step.*
FROM step AS step
JOIN cust AS cust
ON step.serialnumber = cust.serialnumber
"""

joined_df = sparkSqlQuery(
    glueContext,
    query=sql_query,
    mapping={"step": step_trainer_landing, "cust": customers_curated},
    transformation_ctx="joined_df"
)

#  Evaluate Data Quality
EvaluateDataQuality().process_rows(
    frame=joined_df,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={"dataQualityEvaluationContext": "dq_step_trainer", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

#  Write as ONE JSON file
glueContext.write_dynamic_frame.from_options(
    frame=joined_df.coalesce(1),  # Ensures single output file
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://stedi-zainababbas/trusted/step_trainer/"},
    transformation_ctx="write_step_trainer_trusted"
)

job.commit()
