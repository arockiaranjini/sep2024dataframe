import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source

object EX15_Broadcasting_withFile2 extends App {

  def loadBoaringWords(): Set[String] = {

    var boringWords: Set[String] = Set()

    val lines = Source.fromFile("C:/Users/mctcl/Desktop/sparkdataset/Raja/boaringwords.txt").getLines()

    for (line <- lines) {
      //put into the set
      boringWords += line
    }
    boringWords
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "wordCount")

  var nameSet = sc.broadcast(loadBoaringWords)

  val input = sc.textFile("C:/Users/mctcl/Desktop/sparkdataset/Raja/bigdatacamp.csv")

  val mappedInput = input.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))

  val words = mappedInput.flatMapValues(x => x.split(" "))

  val finalMapped = words.map(x => (x._2.toLowerCase(), x._1))
  //(big,24)
  //(is,15)


  val filteredRdd = finalMapped.filter(x => !nameSet.value(x._1))


  val total = filteredRdd.reduceByKey((x, y) => x + y)

  val sorted = total.sortBy(x => x._2, false)

  sorted.take(20).foreach(println)
}


