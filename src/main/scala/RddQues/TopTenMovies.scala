package RddQues

import DataframeQues.PrepareRatingsData.ratingDf
import DataframeQues.prepareMoviesData._
import  DataframeQues.LoadingUsersData.spark
import org.apache.spark.sql.functions.col

object TopTenMovies extends App {
  import spark.implicits._

  val ratingsRDD = spark.sparkContext.textFile("/home/knoldus/Desktop/MovieAnalytics/Movielens/ratings.dat")
  val moviesOccurred = ratingsRDD.map(line => line.split("::")(1))
  val moviesPaired = moviesOccurred.map(movie => (movie, 1))
  val moviesCount = moviesPaired.reduceByKey(_ + _).sortBy(_._2, ascending = false)

  val movieId = moviesCount.take(10)
val topTenMovies =  spark.sparkContext.parallelize(movieId)

  val moviesRDD = spark.sparkContext.textFile("/home/knoldus/Desktop/MovieAnalytics/Movielens/movies.dat")
    .map(line => (line.split("::")(0), line.split("::")(1)))

  moviesRDD.join(topTenMovies).foreach(println)
}
