package DataframeQues

import DataframeQues.LoadingUsersData.spark

object PrepareRatingsData  {

    val ratingData = spark.read.option("delimiter", "::")
    .csv("/home/knoldus/Desktop/MovieAnalytics/Movielens/ratings.dat")

//  ratingData.printSchema()

  val ratingDf = ratingData.toDF("UserID", "MovieID", "Rating", "Timestamp")
//  ratingDf.show()
}
