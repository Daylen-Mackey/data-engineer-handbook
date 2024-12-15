from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode, sequence, date_add

def do_host_cumul_datelist_transformation(spark, dataframe, ds):
    # Ensure the input DataFrame is available as a temporary view
    dataframe.createOrReplaceTempView("device_activity_cumulated")

    # Use Spark SQL to execute the logic
    query = f"""
    -- Filter data for the specific date
    WITH users AS (
        SELECT *
        FROM device_activity_cumulated
        WHERE curr_date = DATE('{ds}')
    ),

    -- Generate the series of dates using Spark SQL
    series AS (
        SELECT
            explode(sequence(
                (SELECT MIN(curr_date) FROM device_activity_cumulated), -- Earliest date
                (SELECT MAX(curr_date) FROM device_activity_cumulated), -- Latest date
                interval 1 day
            )) AS date_series
    ),

    -- Cross join users with the series and compute placeholders
    bits AS (
        SELECT
            CASE
                WHEN array_contains(users.device_activity_datelist, series.date_series) THEN
                    CAST(POW(2, 32 - datediff(users.curr_date, series.date_series)) AS BIGINT)
                ELSE
                    0
            END AS temp_int,
            users.user_id,
            series.date_series,
            users.curr_date
        FROM
            users
        CROSS JOIN
            series
    )

    -- Aggregate the results
    SELECT
        user_id,
        SUM(temp_int) AS device_activity
    FROM
        bits
    GROUP BY
        user_id
    """
    return spark.sql(query)

def main():
    ds = '2023-01-01'
    spark = SparkSession.builder \
      .master("local") \
      .appName("players_scd") \
      .getOrCreate()

    # Input DataFrame: Replace with your actual data source
    input_df = spark.table("device_activity_cumulated")

    # Perform the transformation
    output_df = do_host_cumul_datelist_transformation(spark, input_df, ds)

    # Write the output to the target table
    output_df.write.mode("overwrite").insertInto("hosts_cumulated")
