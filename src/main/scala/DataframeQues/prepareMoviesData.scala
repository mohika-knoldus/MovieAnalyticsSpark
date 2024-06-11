package DataframeQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import DataframeQues.LoadingUsersData.spark

object prepareMoviesData {
  val moviesData = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> "::", "mode" -> "dropMalformed"))
    .csv("/home/knoldus/Desktop/MovieAnalytics/Movielens/movies.dat")

  val moviesDf = moviesData.toDF("MovieID", "Title", "Genre")
//  moviesDf.show(false)

  val getYear = moviesDf.withColumn("year", split(col("Title"), ".*\\((?!.*\\()").getItem(1))
    .withColumn("year", regexp_replace(col("year"), "\\)", ""))

//  getYear.show()
  val getTitle = getYear.withColumn("Title", split(col("Title"), "\\(:?").getItem(0))

  val finalMoviesDf = getTitle.withColumn("genre", explode(split(col("genre"), "\\|")))

// finalMoviesDf.show(false)
//  finalMoviesDf.select(max(col("year"))).show()

}
