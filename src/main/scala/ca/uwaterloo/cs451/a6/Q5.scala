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

class Q5Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String]("input", descr = "Input path", required = true)
    val text = opt[Boolean]("text", descr = "text command processes input as text file", required = false)
    val parquet = opt[Boolean]("parquet", descr = "parquet command processes input as parquet file", required = false)
    verify()
}

object Q5{
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]){
        val args = new Q5Conf(argv)

        log.info("Input: " + args.input())
        log.info("Is input processed as text file: " + args.text())
        log.info("Is input processed as parquet file: " + args.parquet())

        val sparkSession = SparkSession.builder.appName("A6Q2").getOrCreate

        val isParquet:Boolean = args.parquet()

        val o_custkeyPos = 1
        val o_orderkeyPos = 0
        
        val l_orderkeyPos = 0
        val l_shipdatePos = 10

        val c_custkeyPos = 0
        val c_nationkeyPos = 3

        val n_nationkeyPos = 0
        val n_namePos = 1

        val canadaCountryCode = 3
        val usaCountryCode = 24

        val queryResult  = if(!isParquet){
            //Process as TXT file
            val lineitemRDD = sparkSession.sparkContext.textFile(args.input()+"/lineitem.tbl")
            val ordersRDD = sparkSession.sparkContext.textFile(args.input()+"/orders.tbl")
            val customerRDD = sparkSession.sparkContext.textFile(args.input()+"/customer.tbl")
            val nationRDD = sparkSession.sparkContext.textFile(args.input()+"/nation.tbl")

            val lineItemProjection = lineitemRDD.map(line => line.split('|'))
                .map(line => (line.apply(l_orderkeyPos).toInt, line.apply(l_shipdatePos).substring(0,7)))

            val ordersProjection = ordersRDD.map(line => {
                val row = line.split('|')
                (row.apply(o_orderkeyPos).toInt, row.apply(o_custkeyPos).toInt)
            })

            val customerProjection = customerRDD.map(line => {
                val row = line.split('|')
                (row.apply(c_custkeyPos).toInt, row.apply(c_nationkeyPos).toInt)
            })

            val nationProjection = nationRDD.map(line => {
                val row = line.split('|')
                (row.apply(n_nationkeyPos).toInt, row.apply(n_namePos))
            }).filter{case(n_nationkey, n_name) => (n_nationkey == canadaCountryCode) }

            val customerMap = customerProjection.collectAsMap()
            val nationMap = nationProjection.collectAsMap()

            lineItemProjection.cogroup(ordersProjection)
                .filter(_._2._1.size > 0)
                .filter(_._2._2.size > 0)
                .filter{case (key,value) => {
                    val o_custkey = value._2.toList.apply(0)
                    customerMap.contains(o_custkey)        
                }}
                .map{case (key,value) => {
                    val o_custkey = value._2.toList.apply(0)
                    val c_nationkey = customerMap.getOrElse(o_custkey, -1)
                    val month = value._1.toList.apply(0)
                    (c_nationkey, month)
                }}
                .filter{case (c_nationkey, month) => nationMap.contains(c_nationkey)}
                .map{case (c_nationkey, month) => ((c_nationkey, month), 1))}
                .reduceByKey(_ + _)
                .sortBy(_.1._2)
                .map{case ((n_nationkey, month), count) => ((n_nationkey, nationMap.getOrElse(n_nationkey,"")), (month, count)))}
                .collect          
            
        }else{            
            val lineitemRDD = sparkSession.read.parquet(args.input()+"/lineitem").rdd
            val ordersRDD = sparkSession.read.parquet(args.input()+"/orders").rdd
            val customerRDD = sparkSession.read.parquet(args.input()+"/customer").rdd
            val nationRDD = sparkSession.read.parquet(args.input()+"/nation").rdd

            val lineItemProjection = lineitemRDD.map(row => (row.getInt(l_orderkeyPos), row.getString(l_shipdatePos).substring(0,7)))

            val ordersProjection = ordersRDD.map(row => (row.getInt(o_orderkeyPos), row.getInt(o_custkeyPos)))
            val customerProjection = customerRDD.map(row => (row.getInt(c_custkeyPos), row.getInt(c_nationkeyPos)))
            val nationProjection = nationRDD.map(row => (row.getInt(n_nationkeyPos), row.getString(n_namePos)))
                                    .filter{case(n_nationkey, n_name) => (n_nationkey == canadaCountryCode) }

            val customerMap = customerProjection.collectAsMap()
            val nationMap = nationProjection.collectAsMap()

            lineItemProjection.cogroup(ordersProjection)
                .filter(_._2._1.size > 0)
                .filter(_._2._2.size > 0)
                .filter{case (key,value) => {
                    val o_custkey = value._2.toList.apply(0)
                    customerMap.contains(o_custkey)        
                }}
                .map{case (key,value) => {
                    val o_custkey = value._2.toList.apply(0)
                    val c_nationkey = customerMap.getOrElse(o_custkey, -1)
                    val month = value._1.toList.apply(0)
                    (c_nationkey, month)
                }}
                .filter{case (c_nationkey, month) => nationMap.contains(c_nationkey)}
                .map{case (c_nationkey, month) => ((c_nationkey, month), 1))}
                .reduceByKey(_ + _)
                .sortBy(_.1._2)
                .map{case ((n_nationkey, month), count) => ((n_nationkey, nationMap.getOrElse(n_nationkey,"")), (month, count)))}
                .collect          

        }

        queryResult.foreach{case ((n_nationkey, n_name), (month, count)) => {
            println("("+n_nationkey+","+n_name+","+month+","+count+")")
        }}
        
    }
} 