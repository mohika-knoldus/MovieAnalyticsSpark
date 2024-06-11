package RddQues

import DataframeQues.prepareMoviesData._
import DataframeQues.LoadingUsersData.spark
import org.apache.spark.sql.functions._

object MovieStartingWithNumberOrLetter extends App {
  val movies_rdd = spark.sparkContext.textFile("/home/knoldus/Desktop/MovieAnalytics/Movielens/movies.dat")
  val movies = movies_rdd.map(lines => lines.split("::")(1))

  val firstWord = movies.map(value => value.split(" ")(0))
  val startingWithLetter = firstWord.filter(word => Character.isLetter(word.head)).map(word => (word.head.toUpper, 1))
  val letterCount = startingWithLetter.reduceByKey(_ + _)

  val startingWithDigit = firstWord.filter(word => Character.isDigit(word.head)).map(word => (word.head, 1))
  val digitCount = startingWithDigit.reduceByKey(_ + _).sortByKey()
  val result = digitCount.union(letterCount)
  result.foreach(println)

// SECOND WAY ---> DATAFRAMES

val word = getTitle.select(split(col("Title"), " ").getItem(0).as("word"))
  val firstCharacter = word.select(upper(substring(col("word"), 1, 1)).as("firstCharacter"))
    .withColumn("count", lit(1))

  firstCharacter.show(false)
  val count = firstCharacter.groupBy(col("firstCharacter")).agg(sum("count").as("numberOfMovies")).sort(col("firstCharacter"))
    .filter(col("firstCharacter").rlike("^[0-9A-Z]"))
  count.show(40, false)
}
