package rddtry

import java.io.PrintWriter
import java.io._

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by saumya on 17/1/17.
  */
object RDDTryOne {
  def main(args: Array[String]): Unit =  {


    //conf creates configuration. set master is source. * takes all source
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDTryOne")
    //context needs conf
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //Reading text file
    //source---origin
    //sink-- writing RDD to external source
    //action--
    //transformation--- creates new RDD

    val ipPath = "/home/saumya/InnovAccer/warandpeace.txt"
    //source
    val readRDD = sc.textFile(ipPath)
      .cache()    //prevents reading from disk again for sample
    //not needed if read is needed only once

    //will take 10% data from original RDD


    //val sampleRDD = readRDD.sample(withReplacement = false, 0.01,123)


    //prints no of lines
    //count is action

    //println("Sample Count " + sampleRDD.count())


    //display 1st 10 lines
    //val xTake10 = readRDD.take(10)
    //xTake10.foreach(println)

    //ctrl + q --it will tell documentary..ie type of val/var

    //val y = readRDD.sample(withReplacement = false,0.1,123) //sample will take 0.1=10%, craetes RDD, trans, lazy
    //123 is seed value. used for unit testing
    //withReplacement = false dn allow same string taken again in bucket
    //y.foreach(println)
    //val z = readRDD.takeSample(withReplacement = false,10,123) //takes 10 lines, creates array, action

    //split text into words
    //meth1--better way
    //flatMap converts RDD of list of string to RDD of string

    val splitRDD = readRDD.flatMap(x=>x.toLowerCase.split(" "))
      .cache()
    //splitRDD.take(10).foreach(println)


    //meth2
//    val splitRDD1 = sampleRDD.map(x=>x.split(" ").toList)
//    val opRDD = splitRDD1.flatMap(x=>x)
//    opRDD.take(10).foreach(println)


    //Filter Stop Words
    //for removing a,an,the, is, are
    val stopWordList = List("a","an","the","is","are")
    //println("Total Output Wrd Count: " + splitRDD.count())

    val filteredRDD = splitRDD.filter(x=> !stopWordList.contains(x))
    //println("Filtered Word Count: " + filteredRDD.count())
    //filteredRDD.take(10).foreach(println)
    //filteredRDD does not have filtered words





    //Word Count
    //to tell frequency of each word

    val wordUnitRDD = filteredRDD.map(x=>(x,1))   //creates tuple for each element (a(1),b(1),a(1)
    val wordCountRDD = wordUnitRDD
      .groupBy(x=>x._1)   //RDD[word, List(int)] (a,list(1,1)) b,list(1)...
      .map(x=>{
        val key = x._1    //takes first element of tuple
        val totalCount = x._2.size    //x._2 would have list of integers. .size will sum them
        (key, totalCount)
        })
//
//    wordCountRDD.foreach(println)




    //frequency count

    val freqUnitRDD = wordCountRDD.map(x=>(x._2,1))
    val freqCountRDD = freqUnitRDD
      .groupBy(x=>x._1)
      .map(x=>{
        val k = x._1
        val c = x._2.size
        (k, c)
      })

    //write RDD to Text file

    freqCountRDD.coalesce(1).repartition(1).saveAsTextFile("/home/saumya/InnovAccer/Assignments/SparkTestOneSix/frequencyRDD1")


    //freqCountRDD.foreach(println)


    //most freq upto 50 percentile and write to CSV


    //maxPercentileRDD.foreach(println)

    val length = wordCountRDD.count()
    val m = (length / 2).toInt

    val maxPercentileRDD = wordCountRDD.coalesce(1).sortBy(_._2, false).take(m)
    //val writer = new PrintWriter(new File("test.txt" ))

    //new PrintWriter("/home/saumya/InnovAccer/Assignments/SparkTestOneSix/macPercentileRDD") { write(maxPercentileRDD.foreach()); close }

    //writer.close()

    val file = "/home/saumya/InnovAccer/Assignments/SparkTestOneSix/maxPercentileRDD"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- maxPercentileRDD) {
      writer.write(x + "\n")  // however you want to format it
    }
    writer.close()

    println("Total Sentence Count " + readRDD.count())
    println("Total Word Count after Split  " + splitRDD.count())
    println("Total Word Count after Filtering " + filteredRDD.count())
    println("Total Unique Word Count after Split " + splitRDD.count())




  }

}
