import sys
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

from IMDBDataProcesser import spark, retrieve_top_20_movies, retrieve_top_20_movies_credits, validate_file_path


# Define a class for unit testing
class IMDBDataProcessesTest(unittest.TestCase):

    def setUp(self):
        # Create a SparkSession for testing
        self.spark = SparkSession.builder.appName("IMDB Data Processor Test").master("local[*]").getOrCreate()
        self.name_basics_path = sys.argv[1]
        self.title_akas_path = sys.argv[2]
        self.title_basics_path = sys.argv[3]
        self.title_crew_path = sys.argv[4]
        self.title_episode_path = sys.argv[5]
        self.title_principals_path = sys.argv[6]
        self.title_rating_path = sys.argv[7]

    def tearDownClass(self):
        # Stop the SparkSession after testing
        self.spark.stop()

    # Test for Question 1
    def test_top_20_movies(self):

        # Load the test dataset into Spark DataFrame
        name_basics_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.name_basics_path))
        title_akas_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.title_akas_path))
        title_basics_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.title_basics_path))
        title_crew_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.title_crew_path))
        title_episode_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.title_episode_path))
        title_principals_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.title_principals_path))
        title_rating_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.title_rating_path))

        # Call the function to retrieve the top 20 movies
        top_20_movies = retrieve_top_20_movies(name_basics_df, title_akas_df, title_basics_df, title_crew_df,
                                               title_episode_df, title_principals_df, title_rating_df, 50)

        self.assertIsNotNone(top_20_movies)
        self.assertEqual(len(top_20_movies), 20)

    # Test for Question 2
    def test_top_20_movies_credits(self):
        name_basics_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.name_basics_path))
        title_akas_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.title_akas_path))
        title_basics_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.title_basics_path))
        title_crew_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.title_crew_path))
        title_episode_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.title_episode_path))
        title_principals_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.title_principals_path))
        title_rating_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
            validate_file_path(self.title_rating_path))
        top_20_movies = retrieve_top_20_movies(name_basics_df, title_akas_df, title_basics_df, title_crew_df,
                                               title_episode_df, title_principals_df, title_rating_df, 50)
        top_credits = retrieve_top_20_movies_credits(top_20_movies, title_principals_df)

        self.assertIsNotNone(top_credits)
        self.assertEqual(len(top_credits), 10)
