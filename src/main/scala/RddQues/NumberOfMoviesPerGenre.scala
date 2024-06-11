package RddQues

import DataframeQues.prepareMoviesData._
import org.apache.spark.sql.functions._
import DataframeQues.LoadingUsersData.spark

object NumberOfMoviesPerGenre extends App {
  val moviesRdd = spark.sparkContext.textFile("/home/knoldus/Desktop/MovieAnalytics/Movielens/movies.dat")

  val genre = moviesRdd.map(lines => lines.split("::")(2))
  val flat_genre = genre.flatMap(lines => lines.split("\\|"))
  val genreKeyValue = flat_genre.map(k => (k, 1))
  val genre_count = genreKeyValue.reduceByKey(_+_)
  genre_count.foreach(println)

  val moviesPerGenre = finalMoviesDf.groupBy(col("Genre")).agg(countDistinct(col("Title")).as("NumOfMovies"))
  moviesPerGenre.show(false)
}
