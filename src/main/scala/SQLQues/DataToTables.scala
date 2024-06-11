package SQLQues

import DataframeQues.PrepareRatingsData._
import DataframeQues.LoadingUsersData.spark
import DataframeQues.LoadingUsersData._
import DataframeQues.prepareMoviesData._

object DataToTables extends App {
  finalMoviesDf.createOrReplaceTempView("movies_staging")
  ratingDf.createOrReplaceTempView("ratings_staging")
  userDf.createOrReplaceTempView("users_staging")

    spark.sql("drop database if exists sparkdatalake cascade")
    spark.sql("create database sparkdatalake")

  spark.sql("""SELECT * FROM movies_staging""").write.mode("overwrite").saveAsTable("Movies")
  spark.sql("""SELECT * FROM ratings_staging""").write.mode("overwrite").saveAsTable("Ratings")
  spark.sql("""SELECT * FROM users_staging""").write.mode("overwrite").saveAsTable("Users")

}
