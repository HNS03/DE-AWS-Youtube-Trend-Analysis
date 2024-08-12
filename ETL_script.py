import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the base path and folders you want to include
base_path = "s3://de-on-youtube-useast2-raw-dev/youtube/raw_statistics/"
included_regions = ['ca', 'gb', 'us']

# Generate the list of paths to include
paths_to_include = [f"{base_path}region={region}/" for region in included_regions]

# Script generated for node csv_source
csv_source_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": paths_to_include, "recurse": True},
    transformation_ctx="csv_source_node"
)

# Add region column based on folder name
def add_region_column(dynamic_frame, region_value):
    # Convert DynamicFrame to DataFrame
    df = dynamic_frame.toDF()
    # Add the region column
    from pyspark.sql.functions import lit
    df_with_region = df.withColumn('region', lit(region_value))
    # Convert back to DynamicFrame
    return DynamicFrame.fromDF(df_with_region, glueContext, "df_with_region")

# Apply region column for each region
frames_with_region = [add_region_column(csv_source_node, region) for region in included_regions]

# Union the frames
from pyspark.sql.functions import col
combined_df = frames_with_region[0].toDF()
for frame in frames_with_region[1:]:
    combined_df = combined_df.union(frame.toDF())

# Convert back to DynamicFrame
combined_dynamic_frame = DynamicFrame.fromDF(combined_df, glueContext, "combined_dynamic_frame")

# Script generated for node transform_change_schema
transform_change_schema_node = ApplyMapping.apply(
    frame=combined_dynamic_frame,
    mappings=[
        ("video_id", "string", "video_id", "bigint"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "string", "category_id", "bigint"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "string", "views", "bigint"),
        ("likes", "string", "likes", "bigint"),
        ("dislikes", "string", "dislikes", "bigint"),
        ("comment_count", "string", "comment_count", "bigint"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "string", "comments_disabled", "string"),
        ("ratings_disabled", "string", "ratings_disabled", "string"),
        ("video_error_or_removed", "string", "video_error_or_removed", "string"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string")  # Include the region column in mapping
    ],
    transformation_ctx="transform_change_schema_node"
)

# Write the data to the target S3 bucket with partitioning by region
csv_target_node = glueContext.write_dynamic_frame.from_options(
    frame=transform_change_schema_node,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://de-on-youtube-useast2-cleansed-data-dev/youtube/raw_statistics/", "partitionKeys": ["region"]},
    format_options={"compression": "snappy"},
    transformation_ctx="csv_target_node"
)

job.commit()