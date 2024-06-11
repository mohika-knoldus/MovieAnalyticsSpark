package SQLQues

import DataframeQues.LoadingUsersData.spark
import DataframeQues.PrepareRatingsData.ratingDf

object TotalRatingPerMovie extends App {
ratingDf.createOrReplaceTempView("RatingsView")

  spark.sql("""SELECT MovieID,sum(Rating) AS Total_Rating From RatingsView GROUP BY MovieID ORDER BY cast(MovieID as Int) ASC""").show(false)

}
