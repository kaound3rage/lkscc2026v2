import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

user_profiles = glueContext.create_dynamic_frame.from_catalog(
    database="streamify_database",
    table_name="user_profiles"
)

user_interactions = glueContext.create_dynamic_frame.from_catalog(
    database="streamify_database",
    table_name="user_interactions"
)

content_catalog = glueContext.create_dynamic_frame.from_catalog(
    database="streamify_database",
    table_name="content_catalog"
)

users_df = user_profiles.toDF()
interactions_df = user_interactions.toDF()
content_df = content_catalog.toDF()

# 1. Create user-content matrix from completed streams
user_content_matrix = interactions_df.filter(
    F.col("interaction_type") == "complete"
).groupBy("user_id", "content_id").agg(
    F.count("*").alias("stream_count"),
    F.avg("rating").alias("avg_rating")
)

# 2. Calculate content popularity
content_stats = interactions_df.groupBy("content_id").agg(
    F.countDistinct("user_id").alias("unique_viewers"),
    F.count("*").alias("total_interactions"),
    F.avg("rating").alias("avg_rating")
)

# 3. User behavior features
user_behavior = interactions_df.groupBy("user_id").agg(
    F.count("*").alias("total_interactions"),
    F.countDistinct("content_id").alias("unique_content_viewed"),
    F.avg("watch_duration_seconds").alias("avg_watch_duration")
)

user_features = users_df.join(user_behavior, "user_id", "left")

user_content_matrix.write.mode("overwrite").parquet(
    "s3://streamify-bucket-lks2026/processed-data/user_content_matrix/"
)

content_stats.write.mode("overwrite").parquet(
    "s3://streamify-bucket-lks2026/processed-data/content_stats/"
)

user_features.write.mode("overwrite").parquet(
    "s3://streamify-bucket-lks2026/processed-data/user_features/"
)

job.commit()
