import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.io._


object driver {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    val conf = new SparkConf().setAppName("OhhYeah").setMaster("local[10]")
    val sc = new SparkContext(conf)

    //make the file writer
    //removed old code to write starting files
    val file = new File("flyoutControl.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val file2 = new File("groundControl.txt")
    val bw2 = new BufferedWriter(new FileWriter(file2))
    val file3 = new File("hitControl.txt")
    val bw3 = new BufferedWriter(new FileWriter(file3))


    val file4 = new File("flyoutResid.txt")
    val bw4 = new BufferedWriter(new FileWriter(file))
    val file5 = new File("groundResid.txt")
    val bw5 = new BufferedWriter(new FileWriter(file2))
    val file6 = new File("hitResid.txt")
    val bw6 = new BufferedWriter(new FileWriter(file3))

    val file7 = new File("flyouts.txt")
    val bw7 = new BufferedWriter(new FileWriter(file))
    val file8 = new File("groundouts.txt")
    val bw8 = new BufferedWriter(new FileWriter(file2))
    val file9 = new File("hits.txt")
    val bw9 = new BufferedWriter(new FileWriter(file3))


    //group the test data
    val testData = sc.textFile("test.txt").map(x => x.split(',')).map(x => ((x(1),x(2)),(x(3).toDouble.toInt,x(4).toDouble.toInt,x(5).toDouble.toInt,1))).persist()
    val groupedTest = testData.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4)).mapValues(x => (x._1 * 1.0 / x._4, x._2 * 1.0 / x._4, x._3 * 1.0 / x._4)).persist()


    //get groupByPitchers and groupByBatters
    //also find legue average
    val training = sc.textFile("train.txt")
    val batters = training.map(x => x.split(',')).map(x => (x(4), (x(8).toDouble.toInt, x(9).toDouble.toInt, x(10).toDouble.toInt, 1))).persist()
    val groupedBatters = batters.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4)).mapValues(x => (x._1 * 1.0 / x._4, x._2 * 1.0 / x._4, x._3 * 1.0 / x._4)).persist()
    val pitchers = training.map(x => x.split(',')).map(x => (x(5), (x(8).toDouble.toInt, x(9).toDouble.toInt, x(10).toDouble.toInt, 1))).persist()
    val groupedPitchers = pitchers.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4)).mapValues(x => (x._1 * 1.0 / x._4, x._2 * 1.0 / x._4, x._3 * 1.0 / x._4)).persist()
    val league = training.map(x => x.split(',')).map(x => (x(8).toDouble, x(9).toDouble, x(10).toDouble)).reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
    val finalLeague = (league._1 / 456024.0, league._2 / 456024.0, league._3 / 456024.0)

    //add grouped data to testable pairs
    //then perform log5 on each statistic
    //changed the final map of each statistic from string to real data types for later use
    val stuff = sc.textFile("testablePairs.txt")
    val keyed = stuff.map(x => (x.split(',')(0), x.split(',')(1)))
    val batterJoined = groupedBatters.join(keyed).map({ case (k, v) => (v._2, (k, v._1)) }).persist()
    val totJoin = groupedPitchers.join(batterJoined).map({ case (a, b) => ((b._2._1, a), (b._2._2, b._1)) }).persist()
    val flyouts = totJoin.map({ case (k, v) => (k, ((v._1._1 * v._2._1 / finalLeague._1) / ((v._1._1 * v._2._1 / finalLeague._1) + ((1 - v._1._1) * (1 - v._2._1) / (1-finalLeague._1)))))})
      .map({case (a,b) => ((a._1,a._2),(b))}).persist()
    val groundouts = totJoin.map({ case (k, v) => (k, ((v._1._2 * v._2._2 / finalLeague._2) / ((v._1._2 * v._2._2 / finalLeague._2) + ((1 - v._1._2) * (1 - v._2._2) / (1-finalLeague._2)))))})
      .map({case (a,b) => ((a._1,a._2),(b))}).persist()
    val hits = totJoin.map({ case (k, v) => (k, ((v._1._3 * v._2._3 / finalLeague._3) / ((v._1._3 * v._2._3 / finalLeague._3) + ((1 - v._1._3) * (1 - v._2._3) / (1-finalLeague._3)))))})
      .map({case (a,b) => ((a._1,a._2),(b))}).persist()
    //writes the statistic files
    flyouts.map({case(a,b) => a._1+","+a._2+","+b+"\b"}).collect().foreach(bw7.write(_))
    groundouts.map({case(a,b) => a._1+","+a._2+","+b+"\b"}).collect().foreach(bw8.write(_))
    hits.map({case(a,b) => a._1+","+a._2+","+b+"\b"}).collect().foreach(bw9.write(_))


    //finds residuals
    val testJoined = totJoin.join(groupedTest).map({case (k,v) => (k, v._2)})
    val flyoutResid = testJoined.join(flyouts).map({case (k,v) => k._1+","+k._2+","+(v._1._1 - v._2)+"\n"})
    val groundResid = testJoined.join(groundouts).map({case (k,v) => k._1+","+k._2+","+(v._1._2 - v._2)+"\n"})
    val hitsResid = testJoined.join(hits).map({case (k,v) => k._1+","+k._2+","+(v._1._3 - v._2)+"\n"})


    //finds control residuals
    val flyControl = testJoined.map({case (k,v) => k._1+","+k._2+","+(v._1 - finalLeague._1)+"\n"}).persist()
    val groundControl = testJoined.map({case (k,v) => k._1+","+k._2+","+(v._2 - finalLeague._2+"\n")}).persist()
    val hitControl = testJoined.map({case (k,v) => k._1+","+k._2+","+(v._3 - finalLeague._3)+"\n"}).persist()
    flyControl.collect().foreach(bw.write(_))
    groundControl.collect().foreach(bw2.write(_))
    hitControl.collect().foreach(bw3.write(_))


    bw.close()
    bw2.close()
    bw3.close()
    bw4.close()
    bw5.close()
    bw6.close()
    bw7.close()
    bw8.close()
    bw9.close()



  }
}
