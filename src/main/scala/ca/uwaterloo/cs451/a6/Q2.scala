package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.Map
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.rdd.PairRDDFunctions
import org.rogach.scallop._
import java.io._  
import math._

class Q2Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String]("input", descr = "Input path", required = true)
    val date = opt[String]("date", descr = "Date in YYYY-MM-DD format", required = true)
    val text = opt[Boolean]("text", descr = "text command processes input as text file", required = false)
    val parquet = opt[Boolean]("parquet", descr = "parquet command processes input as parquet file", required = false)
    verify()
}

object Q2{
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]){
        val args = new Q2Conf(argv)

        log.info("Input: " + args.input())
        log.info("Date: " + args.date())
        log.info("Is input processed as text file: " + args.text())
        log.info("Is input processed as parquet file: " + args.parquet())

        val sparkSession = SparkSession.builder.appName("A6Q2").getOrCreate
        val date:String = args.date()

        val isParquet:Boolean = args.parquet()
        val limit = 20
        val o_clerkPos = 6
        val o_orderkeyPos = 0
        val l_orderkeyPos = 0
        val l_shipdatePos = 10

        val queryResult  = if(!isParquet){
            //Process as TXT file
            val lineitemRDD = sparkSession.sparkContext.textFile(args.input()+"/lineitem.tbl")
            val ordersRDD = sparkSession.sparkContext.textFile(args.input()+"/orders.tbl")
            
            val lineItemProjection = lineitemRDD.map(line => line.split('|'))
                .filter(_.apply(l_shipdatePos).equals(date))
                .map(line => (line.apply(l_orderkeyPos).toInt, line.apply(l_shipdatePos)))

            val ordersProjection = ordersRDD.map(line => {
                val row = line.split('|')
                (row.apply(o_orderkeyPos).toInt, row.apply(o_clerkPos))
            })

            lineItemProjection.cogroup(ordersProjection)
                .filter(_._2._1.size > 0)
                .filter(_._2._2.size > 0)
                .map{case (o_orderkey,value) => {
                    val o_clerk = value._2.toList.apply(0)
                    (o_clerk, (value._1.toList.apply(0), o_orderkey))
                }}
                .sortBy(_._2._2)
                .take(limit)
            
        }else{
            val lineitemRDD = sparkSession.read.parquet(args.input()+"/lineitem").rdd
            val ordersRDD = sparkSession.read.parquet(args.input()+"/orders").rdd
            
            val lineItemProjection:RDD[(Int, String)] = lineitemRDD.filter(_.getString(l_shipdatePos).equals(date))
                                                                    .map(row => (row.getInt(l_orderkeyPos), row.getString(l_shipdatePos)))

            val ordersProjection:RDD[(Int, String)] = ordersRDD.map(row => (row.getInt(o_orderkeyPos), row.getString(o_clerkPos)))

            lineItemProjection.cogroup(ordersProjection)
                .filter(_._2._1.size > 0)
                .filter(_._2._2.size > 0)
                .map{case (o_orderkey,value) => {
                    val o_clerk = value._2.toList.apply(0)
                    (o_clerk, (value._1.toList.apply(0), o_orderkey))
                }}
                .sortBy(_._2._2)
                .take(limit)
        }

        queryResult.foreach(row => println("("+row._1+","+row._2+")"))
        
    }
} 