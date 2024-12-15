from chispa.dataframe_comparer import assert_df_equality
from ..jobs.spark_query_actor_cumul import do_actor_update_transformation
from collections import namedtuple
import pytest

Actor = namedtuple("Actor", "actorid actor current_year films")
ActorFilm = namedtuple("ActorFilm", "actorid actor year film votes rating filmid")
ActorOutput = namedtuple("ActorOutput", "actor actorid films quality_class is_active current_year")

def test_actor_update_transformation(spark):
    # Source data for `actors`
    actors_data = [
        Actor(1, "Actor A", 1969, [{"film": "Film 1", "votes": 10, "rating": 8.0, "filmid": 101}]),
        Actor(2, "Actor B", 1969, None)
    ]
    actors_df = spark.createDataFrame(actors_data)

    # Source data for `actor_films`
    actor_films_data = [
        ActorFilm(1, "Actor A", 1970, "Film 2", 20, 9.0, 102),
        ActorFilm(2, "Actor B", 1970, "Film 3", 15, 6.5, 103),
        ActorFilm(3, "Actor C", 1970, "Film 4", 30, 7.5, 104)
    ]
    actor_films_df = spark.createDataFrame(actor_films_data)

    # Expected output data
    expected_data = [
        ActorOutput(
            "Actor A", 1,
            [{"film": "Film 1", "votes": 10, "rating": 8.0, "filmid": 101},
             {"film": "Film 2", "votes": 20, "rating": 9.0, "filmid": 102}],
            "star", True, 1970
        ),
        ActorOutput(
            "Actor B", 2,
            [{"film": "Film 3", "votes": 15, "rating": 6.5, "filmid": 103}],
            "bad", True, 1970
        ),
        ActorOutput(
            "Actor C", 3,
            [{"film": "Film 4", "votes": 30, "rating": 7.5, "filmid": 104}],
            "good", True, 1970
        )
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Run the transformation
    actual_df = do_actor_update_transformation(spark, actors_df, actor_films_df)

    # Assert DataFrame equality
    assert_df_equality(actual_df, expected_df)
