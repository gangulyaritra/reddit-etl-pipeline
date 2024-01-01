import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import concat_ws
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3.
AmazonS3_node1701957427805 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://redditdataengineering/raw/reddit_20231207.csv"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1701957427805",
)

# Convert DynamicFrame to DataFrame.
df = AmazonS3_node1701957427805.toDF()

# Concatenate the 3 columns into a single column.
df_combined = df.withColumn(
    "ESS_updated", concat_ws("-", df["edited"], df["spoiler"], df["stickied"])
)

# Drop Unused Columns.
df_combined = df_combined.drop("edited", "spoiler", "stickied")

# Convert DataFrame to DynamicFrame.
S3bucket_node_combined = DynamicFrame.fromDF(
    df_combined, glueContext, "S3bucket_node_combined"
)

# Script generated for node Amazon S3.
AmazonS3_node1701957453376 = glueContext.write_dynamic_frame.from_options(
    frame=S3bucket_node_combined,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://redditdataengineering/transformed/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1701957453376",
)

job.commit()
