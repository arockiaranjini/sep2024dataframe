import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object wc2 extends App {

  //object wordCount extends App {
  // def main(args:Array[String])
  // {}

  Logger.getLogger("org").setLevel(Level.ERROR)
  // if any error show me otherwise no need to show


  val sc =new SparkContext("local[*]","wordCount")

  val input = sc.textFile("C:/Users/mctcl/Desktop/sparkdataset/data.txt")
  input.collect.foreach(println)
  val words =input.flatMap(_.split(" "))
  words.collect.foreach(println)
  val wordMap = words.map((_,1))
  val finalCount= wordMap.reduceByKey(_+_)
  finalCount.collect.foreach(println)
  val sortKeyResult =finalCount.sortByKey()//sort by key only
  sortKeyResult.collect.foreach(println)
  val sortbyResult1 = finalCount.sortBy((_._1)) //sort by first value
  sortbyResult1.collect.foreach(println)
  val sortbyResult2 = finalCount.sortBy((_._2))//sort by second value
  sortbyResult2.collect.foreach(println)
  val filterResult1 = finalCount.filter((_._2>2))//filter the result
  val filterResult2=finalCount.filter((_._1 == "you"))
  filterResult2.collect.foreach(println)
  filterResult1.saveAsTextFile("C:/Users/mctcl/Desktop/sparkdataset/wcresult") //save result into the folder
  scala.io.StdIn.readLine()
  // this means program is still running not terminated
  //it will show DAG
}



