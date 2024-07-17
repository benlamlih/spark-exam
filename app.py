import streamlit as st
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, first, lag, when, max, round
from pyspark.sql.window import Window
import pandas as pd
from streamlit.type_util import V_co

# Initialize Spark session
spark = SparkSession.builder.appName("StreamlitSparkApp").getOrCreate()

st.title("PySpark Data Processing with Streamlit")

# Exercice 1
st.header("Exercice 1: Top 10 Channels with Most Trending Videos")
file_path = "FRvideos.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

df = df.na.drop()

channel_trend_count = df.groupBy("channel_title").count()
top_channels = channel_trend_count.orderBy(desc("count")).limit(10)

top_channels_list = [row["channel_title"] for row in top_channels.collect()]

filtered_df = df.filter(col("channel_title").isin(top_channels_list))
filtered_df = filtered_df.withColumn(
    "like_dislike_ratio", round((col("likes") / col("dislikes")) * 100, 2)
)
sorted_df = filtered_df.orderBy(col("channel_title"), desc("like_dislike_ratio"))
sorted_df_result = sorted_df.select("channel_title", "title", "like_dislike_ratio")

sorted_df_combined = filtered_df.orderBy(desc("like_dislike_ratio"))

st.subheader("Top 10 Channels")
st.dataframe(top_channels.toPandas())

# V1: Sort en groupant par chaine
st.subheader("Videos of Top 10 Channels Sorted by Like-Dislike Ratio")
st.dataframe(sorted_df_result.toPandas())

# V2: Sort sans groupement
st.subheader(
    "Videos of Top 10 Channels Sorted by Like-Dislike Ratio (All Channels Combined)"
)
st.dataframe(
    sorted_df_combined.select("channel_title", "title", "like_dislike_ratio").toPandas()
)

# Exercice 2
st.header("Exercice 2: Running Total Differences")
file_path = "ex2.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

window_spec = Window.orderBy("time")

df = df.withColumn(
    "previous_running_total", lag(col("running_total")).over(window_spec)
)
df = df.withColumn("difference", col("running_total") - col("previous_running_total"))

# Replace NULL with the actual running_total value for the first row
df = df.withColumn(
    "difference",
    when(col("difference").isNull(), col("running_total")).otherwise(col("difference")),
)

# Replace negative differences with the corresponding positive running_total value
df = df.withColumn(
    "difference",
    when(col("difference") < 0, col("running_total")).otherwise(col("difference")),
)

df_result = df.select("time", "department", "items_sold", "running_total", "difference")
st.subheader("Running Total Differences")
st.dataframe(df_result.toPandas())

# Exercice 3
st.header("Exercice 3: Salary Differences by Department")
file_path = "ex3.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

window_spec = Window.partitionBy("department")

df = df.withColumn("max_salary", max("salary").over(window_spec))
df = df.withColumn("diff", col("max_salary") - col("salary"))

df_result = df.select("id", "name", "department", "salary", "diff")
st.subheader("Salary Differences by Department")
st.dataframe(df_result.toPandas())

# Exercice 4
st.header("Exercice 4: Pivot Data")
file_path = "ex4.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

dates = df.select("date").distinct().collect()
dates = [row["date"] for row in dates]

pivot_columns = [column for column in df.columns if column not in ["key", "date"]]

pivoted_df = df.select("key").distinct()

for pivot_col in pivot_columns:
    pivot_df = df.groupBy("key").pivot("date").agg(first(pivot_col))

    for date in dates:
        pivot_df = pivot_df.withColumnRenamed(date, f"{date}_{pivot_col}")

    pivoted_df = pivoted_df.join(pivot_df, on="key")

st.subheader("Pivoted Data")
st.dataframe(pivoted_df.toPandas())
