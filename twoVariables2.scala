import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import twoVariables1.{boaringwordSet, sc}

import scala.io.Source

object twoVariables2 extends App {

  def loadBoaringWords(): Set[String] = {

    var boringWords: Set[String] = Set()

    val lines = Source.fromFile("C:/Users/mctcl/Desktop/sparkdataset/boaring.txt").getLines()

    for (line <- lines) {
      //put into the set
      boringWords += line
    }
    boringWords
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "wordCount")

  var boaringwordSet = sc.broadcast(loadBoaringWords)
  println("print boaringwords=" + boaringwordSet.value)
  val input = sc.textFile("C:/Users/mctcl/Desktop/sparkdataset/data.txt")
  val words = input.flatMap(x => x.split(" ")) // (seperate each word by space)   //flatmap
  //words.collect.foreach(println)
  val wordMap = words.map(x => (x, 1))
  //wordMap.collect.foreach(println)
  //println("================\n")
  val filteredRdd = wordMap.filter(x => !boaringwordSet.value(x._1))

  // filteredRdd.collect.foreach(println)
  // println("================\n")
  //val total = wordMap.reduceByKey(_+_)
  val total = filteredRdd.reduceByKey(_ + _)

  total.foreach(println)
}
