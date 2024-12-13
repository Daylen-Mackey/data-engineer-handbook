"""

# Spark Fundamentals Week

- match_details
  - a row for every players performance in a match
- matches
  - a row for every match
- medals_matches_players
  - a row for every medal type a player gets in a match
- medals
  - a row for every medal type


Your goal is to make the following things happen:

- Build a Spark job that
  - Disabled automatic broadcast join with `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")`
  - Explicitly broadcast JOINs `medals` and `maps`
  - Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets
  - Aggregate the joined data frame to figure out questions like:
    - Which player averages the most kills per game?
    - Which playlist gets played the most?
    - Which map gets played the most?
    - Which map do players get the most Killing Spree medals on?
  - With the aggregated data set
    - Try different `.sortWithinPartitions` to see which has the smallest data size (hint: playlists and maps are both very low cardinality)

Save these as .py files and submit them this way!


"""

"""

Question 1:
Disabled automatic broadcast join with `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")`
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, countDistinct, broadcast

spark = SparkSession.builder.appName("Spark Fundamentals Week").getOrCreate()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
spark.conf.set("spark.sql.iceberg.enabled", "true")
spark.conf.set('spark.sql.iceberg.planning.preserve-data-grouping', 'true')

# Load up our dfs from the CSV files
df_match_details = spark.read.csv("/home/iceberg/data/match_details.csv", header=True, inferSchema=True)
df_matches = spark.read.csv("/home/iceberg/data/matches.csv", header=True, inferSchema=True)
df_medal_matches_players = spark.read.csv("/home/iceberg/data/medals_matches_players.csv", header=True, inferSchema=True)
df_medals = spark.read.csv("/home/iceberg/data/medals.csv", header=True, inferSchema=True)

"""
Question: - Explicitly broadcast JOINs `medals` and `maps`

"""
medals_broadcasted_join = df_medal_matches_players.join(broadcast(df_medals), "medal_id")



"""
Question: Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets
"""
spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")
df_match_details.select("match_id", "player_gamertag", "player_total_kills") \
    .write.mode("append") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.match_details_bucketed")
spark.sql(""" select * from bootcamp.match_details_bucketed""").show()

spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")
df_matches.select("match_id", "mapid", "playlist_id") \
    .write.mode("append") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.matches_bucketed")
spark.sql(""" select * from bootcamp.matches_bucketed""").show()

spark.sql("""DROP TABLE IF EXISTS bootcamp.medal_matches_players_bucketed""")
df_medal_matches_players.select("match_id", "player_gamertag", "medal_id", "count") \
    .write.mode("append") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.medal_matches_players_bucketed")
spark.sql(""" select * from bootcamp.medal_matches_players_bucketed""").show()

# Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets

match_details_bucketed = spark.read.table('bootcamp.match_details_bucketed')
matches_bucketed = spark.read.table('bootcamp.matches_bucketed')
medal_matches_players_bucketed = spark.read.table('bootcamp.medal_matches_players_bucketed')

# Unsure if this is meant to be entirely joined. Overlapping gamer tags might as well be joined to avoid ambiguity
match_details_full_joined = match_details_bucketed.join(medal_matches_players_bucketed, ["match_id", "player_gamertag"])
# Then join the matches to the above table on match_id
master_df_joined = match_details_full_joined.join(matches_bucketed, "match_id",)


"""
Question:
  - Aggregate the joined data frame to figure out questions like:
    - Which player averages the most kills per game?
    - Which playlist gets played the most?
    - Which map gets played the most?
    - Which map do players get the most Killing Spree medals on?
"""
# Which player averages the most kills per game?
# This technically doesn't require the master table, just the match_details_bucketed
# We can globally sort the data by player_total_kills
kills_leaderboard_df = master_df_joined.groupBy("player_gamertag").agg(expr("avg(player_total_kills) as avg_kills")).sort(col("avg_kills").desc())
kills_leaderboard_df.show()


# - Which playlist gets played the most?
# In our master_df_joined we have multiple rows per match due to players and their medals
# But the playlist is the same for all players in a match
# We can group by playlist_id and count the number of distinct matches
playlist_leaderboard_df = master_df_joined \
    .groupBy("playlist_id") \
    .agg(countDistinct("match_id").alias("match_count")) \
    .orderBy(col("match_count").desc())
playlist_leaderboard_df.show()


# - Which map gets played the most?
map_popularity_df = master_df_joined \
    .groupBy("mapid") \
    .agg(countDistinct("match_id").alias("match_count")) \
    .orderBy(col("match_count").desc())
map_popularity_df.show()


# - Which map do players get the most Killing Spree medals on?
# Start by finding the medal_id for Killing Spree -- which should be the Name of the medal
kill_spree_medal_id = df_medals.filter(col("name") == "Killing Spree").select("medal_id").collect()[0][0]
master_df_joined.groupBy("mapid") \
    .agg(expr(f"sum(case when medal_id = {kill_spree_medal_id} then 1 else 0 end) as kill_spree_count")) \
    .orderBy(col("kill_spree_count").desc()).show()


# - With the aggregated data set
#     - Try different `.sortWithinPartitions` to see which has the smallest data size (hint: playlists and maps are both very low cardinality)

sorted_by_playlist = master_df_joined.sortWithinPartitions("playlist_id")
sorted_by_match_id = master_df_joined.sortWithinPartitions("match_id")

sorted_by_playlist.write.mode("overwrite").parquet("/tmp/sorted_by_playlist")

print("Plan for sorting by playlist_id:")
sorted_by_playlist.explain(True)

print("Plan for sorting by match_id:")
sorted_by_match_id.explain(True)
sorted_by_match_id.write.mode("overwrite").parquet("/tmp/sorted_by_match")


reloaded_playlist = spark.read.parquet("/tmp/sorted_by_playlist")
reloaded_match = spark.read.parquet("/tmp/sorted_by_match")

# Trigger an action to materialize the DataFrames
playlist_count = reloaded_playlist.count()
match_count = reloaded_match.count()

print(f"Playlist rows: {playlist_count}")
print(f"Match rows: {match_count}")

# Same datasize, but different performances


