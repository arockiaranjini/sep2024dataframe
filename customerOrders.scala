import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object customerOrders extends App {
  // def main(args:Array[String])
  // {}

  Logger.getLogger("org").setLevel(Level.ERROR)
  // if any error show me otherwise no need to show
  val sc = new SparkContext("local[*]", "wordCount")
  val input = sc.textFile("C:/Users/mctc/Desktop/Raja_Spark_Ex/Customerorders.txt")

  val mappedInput = input.map(x => (x.split(",")(0), x.split(",")(2).toFloat))
 // mappedInput.collect
  //or
 // mappedInput.take(10)

  val totalAmount = mappedInput.reduceByKey((x, y) => x + y)

  //val reversedTuple = totalAmount.map(x=>(x._2,x._1))

  // val sortResults=reversedTuple.sortByKey(false)

  val sortResults = totalAmount.sortBy(x => x._2,false)

  //collect means results should be in local
  //so result is not RDD
  sortResults.collect.foreach(println)
}