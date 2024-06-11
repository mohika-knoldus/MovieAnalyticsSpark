package SQLQues

import DataframeQues.LoadingUsersData.spark
import DataframeQues.PrepareRatingsData.ratingDf

object AvgRatingPerMovie extends App {

  ratingDf.createOrReplaceTempView("RatingsView")

  spark.sql("SELECT MovieID, cast((avg(Rating)) as decimal(16, 2)) " +
    "As Avg_Rating FROM RatingsView GROUP BY MovieID ORDER BY cast(MovieID as int) ASC").show(false)
}
