import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object testablePairs {

  def main(args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("rdd")

    val sc = new SparkContext(conf)

    val test = sc.textFile("test.csv").map(_.split(","))

    val train = sc.textFile("trainPairs.txt").map(_.split(","))

    val batters = train.filter(x => x(0) == "B").map(b =>b(1)).collect.toList
    val pitchers = train.filter(x => x(0) == "P").map(p=>p(1)).collect().toList




    val output = test.groupBy(x => (x(1),x(2))).mapValues(_.size).filter{case (k,v) => v >= 5}
    val result = output.filter{case (k,v) => batters.contains(k._1) && pitchers.contains(k._2)}.map{case (k,v) => (k._1,k._2)}
    println(result.collect.toList)


  }

}
