package dftry

import java.sql.Date
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * Created by saumya on 17/1/17.
  */
object DataFrameTry {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("DataFrameTry")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val personDemographicCSVPath = "/home/saumya/InnovAccer/Assignments/SparkTestOneSix/src/main/resources/dtaframetry/person-demo.csv"
    val personDemographicReadDF = sqlContext.read  //converts CSV to dataframe
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(personDemographicCSVPath)

    val personHealthCSVPath = "/home/saumya/InnovAccer/Assignments/SparkTestOneSix/src/main/resources/dtaframetry/person-health.csv"
    val personHealthDF = sqlContext.read  //converts CSV to dataframe
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(personHealthCSVPath)



//    personDemographicReadDF.show()
//    personHealthDF.show()



    //Merging data frames
    //Meth 1
//    val personDF = personDemographicReadDF
//      .join(personHealthDF,
//        personDemographicReadDF("id") === personDemographicReadDF("id"),
//      "left_outer"
//      ).drop(personHealthDF("id"))


    //Meth 2---freq used
    //large data on left (dont move)
    //less data on right
    //health>>>>demo

    //import org.apache.spark.sql.functions._
    val personDF = personHealthDF
      .join(broadcast(personDemographicReadDF),
        personHealthDF("id") === personDemographicReadDF("id"),
        "right_outer"
      ).drop(personHealthDF("id"))   //since in scala it gives id column of next table as well

    //personDF2.show()


    //filter age<50

    val ageLessThan50DF = personDF.filter(personDF("age")<50)
      .cache()
    ageLessThan50DF.show()


    //writing to CSV

//    ageLessThan50DF
//      .coalesce(1)
//      .write
//      .format("com.databricks.spark.csv")
//      .option("header","true")
//      .save("/home/saumya/InnovAccer/Assignments/SparkTestOneSix/opCSV")

    ageLessThan50DF
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header","true")
      .save("/home/saumya/InnovAccer/Assignments/SparkTestOneSix/opCSV")


    //Read Person Insurance Info from CSV
    val personInsuranceCSVPath = "/home/saumya/InnovAccer/Assignments/SparkTestOneSix/src/main/resources/dtaframetry/person-insurance.csv"
    val personInsuranceDF = sqlContext.read  //converts CSV to dataframe
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(personInsuranceCSVPath)


    //Join Person Insurance with the Demographic and Health Info
    val personDF1 = personDF
      .join(broadcast(personInsuranceDF),
        personDF("id") === personInsuranceDF("id"),
        "right_outer"
      ).drop(personDF("id"))

    //personDF1.show()


    //Get Current Payer wise information of total valid amount insured

    val format = new SimpleDateFormat("d-M-y")
    val today = format.format(Calendar.getInstance().getTime())

    val LICDF = personDF1.filter((personDF1("payer")=== "lic") && (personDF1("datevalidation")>today))
    val AIGDF = personDF1.filter((personDF1("payer")=== "aig") && (personDF1("datevalidation")>today))
    LICDF.show()
    AIGDF.show()

    val LICAmt = LICDF.groupBy().sum("amount")
    val AIGAmt = AIGDF.groupBy().sum("amount")

    println("total valid amount insured by LIC: " + LICAmt)
    println("total valid amount insured by AIG: " + AIGAmt)


    //writing to CSV

    LICAmt
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header","true")
      .save("/home/saumya/InnovAccer/Assignments/SparkTestOneSix/licCSV")

    AIGAmt
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header","true")
      .save("/home/saumya/InnovAccer/Assignments/SparkTestOneSix/aigCSV")







  }

}
