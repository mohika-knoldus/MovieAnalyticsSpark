package SQLQues

import DataframeQues.prepareMoviesData.finalMoviesDf
import DataframeQues.LoadingUsersData.spark

object MoviesPerYear extends App {
finalMoviesDf.createOrReplaceTempView("Movies")

  spark.sql("SELECT Year,count(Title) AS Number_Of_Movies FROM Movies GROUP BY Year").show(false)

}
