import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

from IMDBDataProcesser import spark, retrieve_top_20_movies, retrieve_top_20_movies_credits


# Define a class for unit testing
class IMDBDataProcesserTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        # Create a SparkSession for testing
        self.spark = SparkSession.builder.appName("IMDB Data Processor Test").getOrCreate()

    @classmethod
    def tearDownClass(self):
        # Stop the SparkSession after testing
        self.spark.stop()

    # Test for Question 1
    def test_top_20_movies(self):
        # Load the test dataset into Spark DataFrame
        name_basics_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/name_basics.tsv")
        title_akas_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/title_akas.tsv")
        title_basics_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/title_basics.tsv")
        title_crew_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/title_crew.tsv")
        title_episode_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/title_episode.tsv")
        title_principals_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/title_principals.tsv")
        title_rating_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/title_rating.tsv")

        # Call the function to retrieve the top 20 movies
        top_20_movies = retrieve_top_20_movies(name_basics_df, title_akas_df, title_basics_df, title_crew_df,
                                               title_episode_df, title_principals_df, title_rating_df, 50)

        self.assertIsNotNone(top_20_movies)
        self.assertEqual(len(top_20_movies), 20)

    # Test for Question 2
    def test_top_20_movies_credits(self):
        # Load the test dataset into Spark DataFrame
        name_basics_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/name_basics.tsv")
        title_akas_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/title_akas.tsv")
        title_basics_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/title_basics.tsv")
        title_crew_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/title_crew.tsv")
        title_episode_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/title_episode.tsv")
        title_principals_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/title_principals.tsv")
        title_rating_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            "inputs/title_rating.tsv")
        top_20_movies = retrieve_top_20_movies(name_basics_df, title_akas_df, title_basics_df, title_crew_df,
                                               title_episode_df, title_principals_df, title_rating_df, 50)
        top_credits = retrieve_top_20_movies_credits(top_20_movies, title_principals_df)

        self.assertIsNotNone(top_credits)
        self.assertEqual(len(top_credits), 10)


