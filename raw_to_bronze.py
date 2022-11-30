# Databricks notebook source
dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

from pyspark.sql.functions import explode, col, to_json
movie_raw = spark.read.json(path = f"/FileStore/movie/*", multiLine = True)
movie_raw = movie_raw.select("movie", explode("movie"))
movie_raw = movie_raw.drop(col("movie")).toDF('movie')

display(raw_movie_DF)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
raw_movie_data_df = (raw_movie_DF
                     .select("movie", lit("files.training.databricks.com").alias("datasource"),
                             current_timestamp().alias("ingesttime"),
                             lit("new").alias("status"), current_timestamp().cast("date").alias("ingestdate")
                            )
                    )

# COMMAND ----------

from pyspark.sql.functions import col
(raw_movie_data_df.select("datasource",
                          "ingesttime",
                          "movie",
                          "status",
                          col("ingestdate").alias("p_ingestdate"))
 .write.format("delta")
 .mode("append")
 .partitionBy("p_ingestdate")
 .save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS movie_bronze;
""")

spark.sql(f"""
CREATE TABLE movie_bronze
USING DELTA
LOCATION "{bronzePath}"
""")

# COMMAND ----------

movies_bronze = spark.read.load(path = bronzePath)
