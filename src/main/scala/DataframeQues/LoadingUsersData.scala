package DataframeQues

import org.apache.spark.sql.SparkSession

object LoadingUsersData {
 val spark = SparkSession.builder()
   .master("local[1]")
   .appName("Load Users Data")
   .getOrCreate()

  val usersData = spark.read.options(Map("delimiter" -> "::"))
    .csv("/home/knoldus/Desktop/MovieAnalytics/Movielens/users.dat")

//  usersData.show(false)
//  usersData.printSchema()

 val userDf = usersData.toDF("UserId", "Gender", "Age", "Occupation" ,"Zip-code")
//  userDf.show()
}
