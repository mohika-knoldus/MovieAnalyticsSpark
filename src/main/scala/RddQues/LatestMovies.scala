package RddQues

import DataframeQues.prepareMoviesData._
import org.apache.spark.sql.functions._
import DataframeQues.LoadingUsersData.spark

object LatestMovies extends App {
 val movies_rdd = spark.sparkContext.textFile("/home/knoldus/Desktop/MovieAnalytics/Movielens/movies.dat")
 val movie_names = movies_rdd.map(lines => lines.split("::")(1))
 val year = movie_names.map(lines => lines.substring(lines.lastIndexOf("(") + 1, lines.lastIndexOf(")")))
 val latest = year.max
 val latest_movies = movie_names.filter(lines => lines.contains("(" + latest + ")"))
 latest_movies.foreach(println)

 //----------------DF---DF------DF-----DF----------------------------------
 val maxYear = finalMoviesDf.select(max(col("year"))).first().getString(0)
// maxYear.show()
 println(maxYear)
 val latestMovies = moviesDf.filter(col("Title").contains("("+maxYear+")"))
   .select(col("Title"))
 latestMovies.show()
}
