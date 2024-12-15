from chispa.dataframe_comparer import assert_df_equality
from ..jobs.spark_query_host_cumul import do_host_cumul_datelist_transformation
from collections import namedtuple
import pytest

HostCumulated = namedtuple("HostCumulated", "user_id curr_date device_activity_datelist")
HostOutput = namedtuple("HostOutput", "user_id device_activity")

def test_host_cumul_datelist_transformation(spark):
    # Source data for testing
    source_data = [
        HostCumulated("user_1", "2023-01-01", ["2023-01-01", "2023-01-02"]),
        HostCumulated("user_2", "2023-01-01", ["2023-01-03"])
    ]
    source_df = spark.createDataFrame(source_data)

    # Expected output data
    expected_data = [
        HostOutput("user_1", 3),  # Binary 11 for "2023-01-01" and "2023-01-02"
        HostOutput("user_2", 1)   # Binary 01 for "2023-01-03"
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Run the transformation
    actual_df = do_host_cumul_datelist_transformation(spark, source_df, "2023-01-01")

    # Assert DataFrame equality
    assert_df_equality(actual_df, expected_df)
