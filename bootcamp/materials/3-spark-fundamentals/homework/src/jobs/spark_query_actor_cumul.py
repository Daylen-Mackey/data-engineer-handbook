from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, when, lit, collect_list, concat

def do_actor_update_transformation(spark, actors_df, actor_films_df):
    # Create temporary views for the input dataframes
    actors_df.createOrReplaceTempView("actors")
    actor_films_df.createOrReplaceTempView("actor_films")

    query = """
    WITH yesterday AS (
        SELECT *
        FROM actors
        WHERE current_year = 1969
    ),
    today AS (
        SELECT
            actorid,
            actor,
            year,
            collect_list(named_struct('film', film, 'votes', votes, 'rating', rating, 'filmid', filmid)) AS films,
            AVG(rating) AS avg_rating
        FROM actor_films
        WHERE year = 1970
        GROUP BY actorid, actor, year
    )
    SELECT
        COALESCE(t.actor, y.actor) AS actor,
        COALESCE(t.actorid, y.actorid) AS actorid,
        -- Concatenate films from yesterday and today
        CASE
            WHEN y.films IS NULL THEN t.films
            WHEN t.films IS NULL THEN y.films
            ELSE concat(y.films, t.films)
        END AS films,
        -- Classify quality based on average rating
        CASE
            WHEN t.avg_rating > 8 THEN 'star'
            WHEN t.avg_rating > 7 THEN 'good'
            WHEN t.avg_rating > 6 THEN 'average'
            ELSE 'bad'
        END AS quality_class,
        -- Determine if the actor is active
        CASE
            WHEN t.films IS NOT NULL THEN true
            ELSE false
        END AS is_active,
        COALESCE(t.year, y.current_year + 1) AS current_year
    FROM
        today t
    FULL OUTER JOIN
        yesterday y
    ON
        t.actorid = y.actorid
    """
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_update") \
        .getOrCreate()

    # Load your DataFrames
    actors_df = spark.table("actors")  # Replace with your actual table or DataFrame
    actor_films_df = spark.table("actor_films")  # Replace with your actual table or DataFrame

    # Perform the transformation
    updated_actors_df = do_actor_update_transformation(spark, actors_df, actor_films_df)

    # Write the updated data back
    updated_actors_df.write.mode("overwrite").insertInto("actors")

# Run the main function
main()
