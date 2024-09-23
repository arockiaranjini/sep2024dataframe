import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Ex1_test extends App {
  // def main(args:Array[String])
  // {}

  Logger.getLogger("org").setLevel(Level.ERROR)
  // if any error show me otherwise no need to show
  val sc = new SparkContext("local[*]", "wordCount")
  val input =sc.textFile("C:/Users/mctcl/Desktop/sparkdataset/Raja/Book1.csv")
//  input.collect.foreach(println)
  input.take(10).foreach(println)
  //or //take and collect are actions
  //input.take(10)

  val mappedInput = input.map(x => x.split(",")(6)) //map transformation
  mappedInput.take(10).foreach(println)
  //mappedInput.collect

  val statesNo = mappedInput.map(x => (x, 1))
  statesNo.take(10).foreach(println)
  val reducedstates = statesNo.reduceByKey((x, y) => x + y) //reduceByKey  //same keys are gropued
  //reducedstates.collect
  reducedstates.take(10).foreach(println)
  //val sorted1 = reducedstates.sortBy(x => x._1) =======> sort by ascending //x._1 sort by state

  //val filterdata = sorteimport org.apache.spark.SparkContext
  //import org.apache.log4j.Level
  //import org.apache.log4j.Logger
  //
  //object ex1 extends App {
  //  // def main(args:Array[String])
  //  // {}
  //
  //  Logger.getLogger("org").setLevel(Level.ERROR)
  //  // if any error show me otherwise no need to show
  //  val sc = new SparkContext("local[*]", "wordCount")
  //  val input =sc.textFile("hdfs://192.168.0.100:8020/user/cloudera/sparkcustomer")
  //
  //  //or //take and collect are actions
  //  //input.take(10)
  //
  //  val mappedInput = input.map(x => x.split(",")(7)) //map transformation
  //  //mappedInput.collect
  //
  //  val statesNo = mappedInput.map(x => (x, 1))
  //
  //  val reducedstates = statesNo.reduceByKey((x, y) => x + y) //reduceByKey  //same keys are gropued
  //  //reducedstates.collect
  //
  //  //val sorted1 = reducedstates.sortBy(x => x._1) =======> sort by ascending //x._1 sort by state
  //
  //  //val filterdata = sorted1.filter(x => x._1 == "CA") //filter
  //
  //  val sorted1 = reducedstates.sortBy(x => x._2)//sort by number
  //  sorted1.collect.foreach(println)
  //
  //  //val sorted2 = reducedstates.sortBy(x => x._1, false).collect //===========> sort by descending
  //
  //}d1.filter(x => x._1 == "CA") //filter

  val sorted1 = reducedstates.sortBy(x => x._2)//sort by number
  sorted1.collect.foreach(println)

  //val sorted2 = reducedstates.sortBy(x => x._1, false).collect //===========> sort by descending

}
