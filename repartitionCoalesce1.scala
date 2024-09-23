import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
//learn increasing partition and decreasing partition
//using repartition and coalesce
object repartitionCoalesce1 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "repartition & coalesce")

  val input = sc.textFile("C:/Users/mctcl/Desktop/sparkdataset/moviedata.data").repartition(10)
  println(input.getNumPartitions)//filesize =348MB// so 11 partitions // because default blocksize=32MB
  //val mappedInput = input.map(x => x.split(",")(1))

  //val mappedValue =mappedInput.map(x=>(x,1))
  val mappedInput = input.map(x => {
    val columns = x.split("\t")
    (columns(2), 1)
  })

 val results= mappedInput.reduceByKey(_+_).coalesce(5) //less shuffling
 // val results= mappedInput.reduceByKey(_+_).repartition(5) // shuffling occurs

  results.collect.foreach(println)

  results.saveAsTextFile("C:/Ranjini/Ranjini/Dataset/repartfolder3")
  scala.io.StdIn.readLine()

}

/*Repartition has an intention to have
final partitions of exactly equal size and
for this it has to go through complete shuffling.

Coalesce has a intention to minimize the shuffling
  and combines existing partitions on each machine to avoid a full shuffle.
(aggregating partition on the same machine)
*/
