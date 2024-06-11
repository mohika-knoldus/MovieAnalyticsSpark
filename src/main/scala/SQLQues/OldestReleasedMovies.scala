package SQLQues

import DataframeQues.prepareMoviesData.{finalMoviesDf, moviesDf}
import DataframeQues.LoadingUsersData.spark
object OldestReleasedMovies extends App {
  finalMoviesDf.createOrReplaceTempView("movies_View")

  spark.sql(
    """SELECT Title FROM movies_View
      WHERE Year = (SELECT min(Year) FROM movies_View) """).show(false)
}
