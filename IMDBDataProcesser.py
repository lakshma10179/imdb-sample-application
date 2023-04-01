import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, avg, count
from pyspark.sql.functions import desc, explode, sys
import os

spark = SparkSession.builder.appName('IMDB Data Processor').getOrCreate()

# Check the length of sys.argv
if len(sys.argv) != 8:
    print("Invalid number of arguments. Please provide valid arguments from the console.")
    sys.exit(1)


# Validate the file path
def validate_file_path(file_path):
    # Validate the file path
    if not os.path.exists(file_path):
        print("Invalid file path. The file does not exist.")
        sys.exit(1)

    if not os.path.isfile(file_path):
        print("Invalid file path. Please provide the path to a file, not a directory.")
        sys.exit(1)

    # Return the validated file path
    return file_path


print(f'******** LOADING IMDB DATA SETS INTO DATA FRAMES **************')
name_basics_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
    validate_file_path(sys.argv[1]))
title_akas_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
    validate_file_path(sys.argv[2]))
title_basics_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
    validate_file_path(sys.argv[3]))
title_crew_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
    validate_file_path(sys.argv[4]))
title_episode_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
    validate_file_path(sys.argv[5]))
title_principals_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
    validate_file_path(sys.argv[6]))
title_rating_df = spark.read.option("header", "true").option("sep", "\t").option("multiLine", "true").csv(
    validate_file_path(sys.argv[7]))


def retrieve_top_20_movies(name_basics_df, title_akas_df, title_basics_df, title_crew_df, title_episode_df,
                           title_principals_df, title_rating_df, min_votes):
    # filter out movies with less than 50 votes
    rating_more_then_50_votes_df = title_rating_df.filter(title_rating_df.numVotes >= 50)
    print(f' **** filter out movies with less than 50 votes ****')

    # calculate the average rating and number of votes for each movie
    avg_ratings_df = rating_more_then_50_votes_df.groupBy("tconst").agg(
        {"averageRating": "avg", "numVotes": "avg"}).withColumnRenamed("avg(averageRating)",
                                                                       "avgRating").withColumnRenamed("avg(numVotes)",
                                                                                                      "avgNumVotes")
    print(f' **** calculate the average rating and number of votes for each movie ****')

    # calculate the ranking score for each movie
    ranked_movies_df = rating_more_then_50_votes_df.join(avg_ratings_df, "tconst").withColumn("rankingScore", (
            rating_more_then_50_votes_df.numVotes / avg_ratings_df.avgNumVotes) * avg_ratings_df.avgRating)
    print(f' **** calculate the ranking score for each movie ****')

    top_20_movies_df = ranked_movies_df.join(title_basics_df, ranked_movies_df.tconst == title_basics_df.tconst,
                                             "inner").filter(title_basics_df.titleType == 'movie')
    print(f' **** filtering data based on the given conditions ****')

    # sort the movies based on their ranking score in descending order
    sorted_movies_df = top_20_movies_df.sort(desc("rankingScore"))

    # select the top 20 movies and display their title, ranking score, average rating, and number of votes
    top_20_movies = sorted_movies_df.select("primaryTitle", "rankingScore", "avgRating", "avgNumVotes").take(20)

    return top_20_movies


def retrieve_top_20_movies_credits(top_20_movies, title_principals_df):
    top_20_movie_titles = [movie.primaryTitle for movie in top_20_movies]
    print(f'***** top 20 movie titles ****')
    print(top_20_movie_titles)

    rating_more_then_50_votes_df = title_rating_df.filter(title_rating_df.numVotes >= 50)

    avg_ratings_df = rating_more_then_50_votes_df.groupBy("tconst").agg(
        {"averageRating": "avg", "numVotes": "avg"}).withColumnRenamed("avg(averageRating)",
                                                                       "avgRating").withColumnRenamed("avg(numVotes)",
                                                                                                      "avgNumVotes")

    ranked_movies_df = rating_more_then_50_votes_df.join(avg_ratings_df, "tconst").withColumn("rankingScore", (
            rating_more_then_50_votes_df.numVotes / avg_ratings_df.avgNumVotes) * avg_ratings_df.avgRating)
    print(f'**** calculate the ranking score for each movie ****')

    # join the top 20 movies with the "principals" dataset to get the list of persons credited for each movie
    top_20_movies_principals = ranked_movies_df.join(title_principals_df, "tconst").select("tconst", "nconst")
    print(f'**** join the top 20 movies with the principals dataset to get the list of persons credited for each '
          f'movie ****')

    # count the number of times each person is credited across all top 20 movies
    person_counts = top_20_movies_principals.groupBy("nconst").count().withColumnRenamed("count", "creditsCount")
    print(f'**** count the number of times each person is credited across all top 20 movies ****')

    # sort the persons based on the number of times they are credited in descending order
    sorted_person_counts = person_counts.sort(desc("creditsCount"))
    print(f'**** sort the persons based on the number of times they are credited in descending order ****')

    primary_name_df = name_basics_df.join(sorted_person_counts, "nconst").sort(desc("creditsCount"))

    # display the top 10 persons who are most often credited
    top_10_persons = primary_name_df.select("primaryName", "creditsCount").take(10)
    print(f'**** the top 10 persons who are most often credited ****')

    return top_10_persons


def top_20_movies_titles(top_20_movies):
    # extract the top 20 movies from the previous step
    top_20_movie_titles = [movie.primaryTitle for movie in top_20_movies]
    return top_20_movie_titles


if __name__ == "__main__":
    min_votes = 50
    top_20_movies = retrieve_top_20_movies(name_basics_df, title_akas_df, title_basics_df, title_crew_df,
                                           title_episode_df,
                                           title_principals_df, title_rating_df, min_votes)
    print(f'*********** TOP 20 MOVIES LIST AS BELOW **************')
    print(top_20_movies)

    top_10_persons = retrieve_top_20_movies_credits(top_20_movies, title_principals_df)
    print(f'*********** TOP 10 MOST OFTEN  CREDITED LIST AS BELOW **************')
    print(top_10_persons)

    top_20_movie_titles = top_20_movies_titles(top_20_movies)
    print(f'******* TOP 20 MOVIE TITLES ARE AS BELOW')
    print(top_20_movie_titles)
