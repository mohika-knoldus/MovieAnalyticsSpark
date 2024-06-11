package SQLQues

import DataframeQues.LoadingUsersData.spark
import DataframeQues.PrepareRatingsData.ratingDf

object UserRatedEachMovie extends App {
 ratingDf.createOrReplaceTempView("RatingView")

  spark.sql("""SELECT MovieID, count(UserID) FROM RatingView GROUP BY MovieID ORDER BY cast(MovieID as int) ASC""").show(false)

}
