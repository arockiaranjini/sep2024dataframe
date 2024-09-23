import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object new1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "wordCount")
  val input =sc.textFile("C:/Users/mctcl/Desktop/sparkdataset/Raja/Book1.csv")
  input.take(10).foreach(println)
  val mappedInput = input.map(x => x.split(",")(6)) //map transformation
  mappedInput.take(10).foreach(println)

  val statesNo = mappedInput.map(x => (x, 1))
  statesNo.take(10).foreach(println)
  val reducedstates = statesNo.reduceByKey((x, y) => x + y) //reduceByKey  //same keys are gropued

  reducedstates.take(10).foreach(println)
  val sorted1 = reducedstates.sortBy(x => x._2)//sort by number
  sorted1.collect.foreach(println)
 // val sc = new SparkContext("local[*]", "wordCount")
  val input1 = sc.textFile("C:/Users/mctcl/Desktop/sparkdataset/Raja/Raj.csv")
  input1.take(10).foreach(println)

  val mappedInput1 = input1.map(x => (x.split(",")(0), x.split(",")(2).toFloat))
  mappedInput1.take(10).foreach(println)
  //or
  // mappedInput.take(10)

  val totalAmount = mappedInput1.reduceByKey((x, y) => x + y)
   totalAmount.collect.foreach(println)


   val sortResults = totalAmount.sortBy(x => x._2)

   sortResults.collect.foreach(println)
}

