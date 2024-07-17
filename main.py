import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, first, lag, when, max, round
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("TrendingVideos").getOrCreate()

# Exercice 1:

file_path = "datasets/FRvideos.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

df = df.na.drop()

channel_trend_count = df.groupBy("channel_title").count()

top_channels = channel_trend_count.orderBy(desc("count")).limit(10)

print("Top 10 Channels with Most Trending Videos:")
top_channels.show(truncate=False)

top_channels_list = [row["channel_title"] for row in top_channels.collect()]

filtered_df = df.filter(col("channel_title").isin(top_channels_list))

filtered_df = filtered_df.withColumn(
    "like_dislike_ratio", round((col("likes") / col("dislikes")) * 100, 2)
)

sorted_df = filtered_df.orderBy(col("channel_title"), desc("like_dislike_ratio"))

sorted_df_result = sorted_df.select("channel_title", "title", "like_dislike_ratio")
sorted_df_combined = filtered_df.orderBy(desc("like_dislike_ratio"))

# Avec groupement
print("Videos of Top 10 Channels Sorted by Like-Dislike Ratio:")
sorted_df_result.show(50, truncate=False)
# Sans groupement
print("Videos of Top 10 Channels Sorted by Like-Dislike Ratio (All Channels Combined):")
sorted_df_combined.select("channel_title", "title", "like_dislike_ratio").show(
    50, truncate=False
)

# Exercice 2:

file_path = "datasets/ex2.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

print("Original DataFrame:")
df.show(truncate=False)

window_spec = Window.orderBy("department")

df = df.withColumn(
    "previous_running_total", lag(col("running_total")).over(window_spec)
)
df = df.withColumn("difference", col("running_total") - col("previous_running_total"))

# Replace NULL with the actual running_total value for the first row
df = df.withColumn(
    "difference",
    when(col("difference").isNull(), col("running_total")).otherwise(col("difference")),
)

# Replace negative differences with the corresponding positive running_total value (parceque c'est ce que j'ai comrpris dans l'exemple ....)
# Sinon on aura des difference négtives chose qui est pas présente dans l'exemple.
df = df.withColumn(
    "difference",
    when(col("difference") < 0, col("running_total")).otherwise(col("difference")),
)

df_result = df.select("time", "department", "items_sold", "running_total", "difference")
df_result.show(truncate=False)

# Exercice 3:

file_path = "datasets/ex3.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

print("Original DataFrame:")
df.show(truncate=False)

window_spec = Window.partitionBy("department")

df = df.withColumn("max_salary", max("salary").over(window_spec))

df = df.withColumn("diff", col("max_salary") - col("salary"))

df_result = df.select("id", "name", "department", "salary", "diff")
df_result.show(truncate=False)

# Exercice 4:

file_path = "datasets/ex4.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

print("Original DataFrame:")
df.show(truncate=False)

dates = df.select("date").distinct().collect()
dates = [row["date"] for row in dates]

pivot_columns = [column for column in df.columns if column not in ["key", "date"]]

pivoted_df = df.select("key").distinct()

for pivot_col in pivot_columns:
    pivot_df = df.groupBy("key").pivot("date").agg(first(pivot_col))

    for date in dates:
        pivot_df = pivot_df.withColumnRenamed(date, f"{date}_{pivot_col}")

    pivoted_df = pivoted_df.join(pivot_df, on="key")

print("Pivoted DataFrame:")
pivoted_df.show(truncate=False)
