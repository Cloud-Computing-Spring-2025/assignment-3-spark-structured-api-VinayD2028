from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
logs = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
metadata = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Task 1: User Favorite Genres
window = Window.partitionBy("user_id").orderBy(desc("count"))
logs.join(metadata, "song_id") \
    .groupBy("user_id", "genre") \
    .count() \
    .withColumn("rank", rank().over(window)) \
    .filter(col("rank") == 1) \
    .drop("rank") \
    .write.csv("output/user_favorite_genres", mode="overwrite")

# Task 2: Average Listen Time
logs.groupBy("song_id") \
    .agg(avg("duration_sec").alias("avg_duration_sec")) \
    .write.csv("output/avg_listen_time_per_song", mode="overwrite")

# Task 3: Top 10 Songs This Week (March 24-28, 2025)
logs.filter((col("timestamp") >= "2025-03-24") 
          & (col("timestamp") < "2025-03-29")) \
    .groupBy("song_id") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10) \
    .write.csv("output/top_songs_this_week", mode="overwrite")

# Task 4: Happy Song Recommendations

# Identify primary mood for each user
user_moods = logs.join(metadata, "song_id") \
    .groupBy("user_id", "mood") \
    .count()

user_primary_mood = user_moods.withColumn("rank", rank().over(Window.partitionBy("user_id").orderBy(desc("count")))) \
    .filter(col("rank") == 1) \
    .select("user_id", "mood")

# Filter users who mostly listen to "Sad" songs
sad_users = user_primary_mood.filter(col("mood") == "Sad").select("user_id")

# Get "Happy" songs that have not been played by the sad users
happy_songs = metadata.filter(col("mood") == "Happy").select("song_id", "title", "artist")
played_songs = logs.select("user_id", "song_id")

recommendations = sad_users.crossJoin(happy_songs) \
    .join(played_songs, ["user_id", "song_id"], "left_anti") \
    .groupBy("user_id") \
    .agg(collect_list(struct("song_id", "title", "artist")).alias("recommended_songs")) \
    .withColumn("recommended_songs", slice("recommended_songs", 1, 3))  # Limit to 3 songs per user

recommendations.write.json("output/happy_recommendations", mode="overwrite")

# Task 5: Genre Loyalty Scores

# Count user plays per genre
user_genre_counts = logs.join(metadata, "song_id") \
    .groupBy("user_id", "genre") \
    .count()

# Determine total plays per user
total_plays = logs.groupBy("user_id").count().withColumnRenamed("count", "total_plays")

# Find the most played genre per user with max count instead of rank
user_top_genre = user_genre_counts.alias("ugc").join(
    user_genre_counts.groupBy("user_id").agg(max("count").alias("max_genre_count")).alias("max_gc"),
    (col("ugc.user_id") == col("max_gc.user_id")) & (col("ugc.count") == col("max_gc.max_genre_count"))
).select(col("ugc.user_id"), col("ugc.genre"), col("ugc.count"))

# Compute loyalty score and filter users above 0.8
loyal_users = user_top_genre.join(total_plays, "user_id") \
    .withColumn("loyalty_score", col("count") / col("total_plays")) \
    .filter(col("loyalty_score") >= 0.8) \
    .select("user_id", "genre", "loyalty_score")

loyal_users.write.csv("output/genre_loyalty_scores", mode="overwrite")

# Task 6: Identify users who listen between 12 AM and 5 AM
# Filter for night hours and count listens per user
(logs.filter(hour("timestamp").between(0, 5))
    .groupBy("user_id")
    .count()
    # Apply minimum play count threshold
    .filter(col("count") > 5)  
    .write.csv("output/night_owl_users", mode="overwrite"))

spark.stop()
