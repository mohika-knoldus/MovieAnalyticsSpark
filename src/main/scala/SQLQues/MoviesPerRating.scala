package SQLQues

import DataframeQues.PrepareRatingsData.ratingDf
import DataframeQues.LoadingUsersData.spark

object MoviesPerRating extends App {
  ratingDf.createOrReplaceTempView("RatingsView")

  spark.sql("SELECT Rating,count(MovieID) AS No_Of_Movies FROM RatingsView GROUP BY Rating").show(false)

}
