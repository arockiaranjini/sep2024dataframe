import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source

object twoVariables1 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  //val set1 = Set("are","is")  //step1

  val sc = new SparkContext("local[*]", "remove boaring words")
  //var nameSet = sc.broadcast(set1)   // step1
  var boaringwordSet = sc.broadcast(Set("are","is"))  //instead of this we can use step1
  println("print boaringwords=" + boaringwordSet.value)
  //val myAccumulator= sc.longAccumulator("Count number of words in lines")
  val input = sc.textFile("C:/Users/mctcl/Desktop/sparkdataset/data.txt")
  val words = input.flatMap(x => x.split(" ")) // (seperate each word by space)   //flatmap
  //words.collect.foreach(println)
  val wordMap = words.map(x => (x, 1))
  //wordMap.collect.foreach(println)
  //println("================\n")
  val filteredRdd = wordMap.filter( x => !boaringwordSet.value(x._1))

  // filteredRdd.collect.foreach(println)
  // println("================\n")
  //val total = wordMap.reduceByKey(_+_)
  val total = filteredRdd.reduceByKey(_ + _)

  total.foreach(println)
  //input.foreach(x=>if(x.contains("hi")) myAccumulator.add(1))  //count word in lines

  //println("Number of lines containing 'hi': " + myAccumulator.value)
}

