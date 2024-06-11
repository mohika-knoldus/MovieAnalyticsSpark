package RddQues

import DataframeQues.prepareMoviesData._
import org.apache.spark.sql.functions._
import DataframeQues.LoadingUsersData.spark

object DistinctGenre extends App {

  val movies_rdd = spark.sparkContext.textFile("/home/knoldus/Desktop/MovieAnalytics/Movielens/movies.dat")
  val genres = movies_rdd.map(lines => lines.split("::")(2))
  val testing = genres.flatMap(line => line.split('|'))
  val typesOfGenre = testing.distinct()
  typesOfGenre.foreach(println)

  val distinctGenre = finalMoviesDf.select(col("Genre")).dropDuplicates()
  distinctGenre.show(false)

}
