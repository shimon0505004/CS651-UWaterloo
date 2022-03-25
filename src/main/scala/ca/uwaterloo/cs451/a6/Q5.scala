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
import org.apache.spark.sql.functions._
import org.rogach.scallop._
import java.io._  
import math._

class Q5Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, text, parquet)
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

        val sparkSession = SparkSession.builder.appName("A6Q5").getOrCreate

        val isParquet:Boolean = args.parquet()

        val o_custkeyPos = 1
        val o_orderkeyPos = 0
        
        val l_orderkeyPos = 0
        val l_quantityPos = 4
        val l_shipdatePos = 10
        val l_monthstartPos = 0
        val l_monthendPost = 7

        val c_custkeyPos = 0
        val c_nationkeyPos = 3

        val c_nationKeyCanada = 3
        val c_nationKeyUSA = 24

        val n_nationkeyPos = 0
        val n_namePos = 1


        val lineItemProjection = if(!isParquet){
            sparkSession.sparkContext.textFile(args.input()+"/lineitem.tbl")
                        .map(line => {
                            val arr = line.split('|')
                            val l_orderKey = arr.apply(l_orderkeyPos).toInt
                            val l_shipmonth = arr.apply(l_shipdatePos).substring(l_monthstartPos,l_monthendPost)
                            (l_orderKey, l_shipmonth)
                        })
        }else{
            sparkSession.read.parquet(args.input()+"/lineitem").rdd
                        .map(row => {
                            val l_orderKey = row.getInt(l_orderkeyPos)
                            val l_shipmonth = row.getString(l_shipdatePos).substring(l_monthstartPos,l_monthendPost)
                            (l_orderKey, l_shipmonth)
                        })
        }

        val ordersProjection = if(!isParquet){
            sparkSession.sparkContext.textFile(args.input()+"/orders.tbl")
                        .map(line => {
                            val row = line.split('|')
                            val o_orderkey = row.apply(o_orderkeyPos).toInt
                            val o_custkey = row.apply(o_custkeyPos).toInt
                            (o_orderkey, o_custkey)
                        })
        }else{
            sparkSession.read.parquet(args.input()+"/orders").rdd
                        .map(row =>{
                            val o_orderkey = row.getInt(o_orderkeyPos)
                            val o_custkey = row.getInt(o_custkeyPos)
                            (o_orderkey, o_custkey)
                        })
        }

        val customerProjection = if(!isParquet){
            sparkSession.sparkContext.textFile(args.input()+"/customer.tbl")
                        .map(line => {
                            val row = line.split('|')
                            val c_custkey = row.apply(c_custkeyPos).toInt
                            val c_nationkey = row.apply(c_nationkeyPos).toInt
                            (c_custkey, c_nationkey)
                        })
        }else{
            sparkSession.read.parquet(args.input()+"/customer").rdd
                        .map(row => {
                            val c_custkey = row.getInt(c_custkeyPos)
                            val c_nationkey = row.getInt(c_nationkeyPos)
                            (c_custkey, c_nationkey)
                        })
        }

        //Projecting only Canadian and US for our queries
        val nationProjection = if(!isParquet){
            sparkSession.sparkContext.textFile(args.input()+"/nation.tbl")
                        .map(line => {
                            val row = line.split('|')
                            val n_nationkey = row.apply(n_nationkeyPos).toInt
                            val n_name = row.apply(n_namePos)
                            (n_nationkey, n_name)
                        })
                        .filter{case(n_nationkey, n_name) => ((n_nationkey == c_nationKeyCanada) || (n_nationkey == c_nationKeyUSA))}
        }else{
            sparkSession.read.parquet(args.input()+"/nation").rdd
                        .map(row => {
                            val n_nationkey = row.getInt(n_nationkeyPos)
                            val n_name = row.getString(n_namePos)
                            (n_nationkey, n_name)
                        })
                        .filter{case(n_nationkey, n_name) => ((n_nationkey == c_nationKeyCanada) || (n_nationkey == c_nationKeyUSA))}
        }

        val customerMap = customerProjection.collectAsMap()
        val nationMap = nationProjection.collectAsMap()

        val broadcastcustomerMap = sparkSession.sparkContext.broadcast(customerMap)
        val broadcastnationMap = sparkSession.sparkContext.broadcast(nationMap)


        /*
        val queryResult = lineItemProjection.cogroup(ordersProjection)
                                            .filter{case (orderKey, (shipmonths, custkeys)) => ((shipmonths.size > 0) 
                                                                                                && (custkeys.size > 0) 
                                                                                                && broadcastcustomerMap.value.contains(custkeys.toList.apply(0)))
                                            }
                                            .map{case (orderKey, (shipmonths, custkeys))  => {
                                                val o_custkey = custkeys.toList.apply(0)
                                                val o_shipmonth = shipmonths.toList.apply(0)
                                                val n_nationkey = broadcastcustomerMap.value.getOrElse(o_custkey, -1)                                                
                                                (n_nationkey, o_shipmonth)
                                            }}
                                            .filter{case(n_nationkey, o_shipmonth) => broadcastnationMap.value.contains(n_nationkey)}
                                            .map{case(n_nationkey, o_shipmonth) => (((n_nationkey, broadcastnationMap.value.getOrElse(n_nationkey,"")), o_shipmonth), 1)}
                                            .reduceByKey(_ + _)
                                            .sortBy{case(((n_nationkey, n_name), o_shipmonth), o_shipmentCount) => (n_nationkey, n_name, o_shipmonth)}
                                            .map{case(((n_nationkey, n_name), o_shipmonth), o_shipmentCount) => ((n_nationkey, n_name), (o_shipmonth, o_shipmentCount))}                                            
                                            .collect
        */

        //No cogroup restriction, fixing issues with cogroup merging.
        val queryResult = lineItemProjection.join(ordersProjection)
                                            .filter{case (orderKey, (shipmonth, custkey)) => broadcastcustomerMap.value.contains(custkeys))}
                                            .map{case (orderKey, (o_shipmonth, o_custkey))  => {
                                                val n_nationkey = broadcastcustomerMap.value.getOrElse(o_custkey, -1)                                                
                                                (n_nationkey, o_shipmonth)
                                            }}
                                            .filter{case(n_nationkey, o_shipmonth) => broadcastnationMap.value.contains(n_nationkey)}
                                            .map{case(n_nationkey, o_shipmonth) => (((n_nationkey, broadcastnationMap.value.getOrElse(n_nationkey,"")), o_shipmonth), 1)}
                                            .reduceByKey(_ + _)
                                            .sortBy{case(((n_nationkey, n_name), o_shipmonth), o_shipmentCount) => (n_nationkey, n_name, o_shipmonth)}
                                            .map{case(((n_nationkey, n_name), o_shipmonth), o_shipmentCount) => ((n_nationkey, n_name), (o_shipmonth, o_shipmentCount))}                                            
                                            .collect

        queryResult.foreach{case ((n_nationkey, n_name), (o_shipmonth, o_shipmentCount)) => {
            println("("+n_nationkey+","+n_name+","+o_shipmonth+","+o_shipmentCount+")")
        }}
        
        /*
        if(isParquet){
            val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
            val ordersDF = sparkSession.read.parquet(args.input()+"/orders")
            val customreDF = sparkSession.read.parquet(args.input()+"/customer")
            val nationDF = sparkSession.read.parquet(args.input()+"/nation")

            val updatedlineitemDF = lineitemDF.select("l_orderkey","l_shipdate")
                                                .withColumn("l_shipdate", substring(col("l_shipdate"), 0, 7))
                                                .withColumnRenamed("l_shipdate","l_shipmonth")

            updatedlineitemDF.createOrReplaceTempView("lineitem")
            ordersDF.createOrReplaceTempView("orders")
            customreDF.createOrReplaceTempView("customer")
            nationDF.createOrReplaceTempView("nation")
            val result = sparkSession.sql("""select n_nationkey, n_name, l_shipmonth, count(*) 
                                                from lineitem, orders, customer, nation 
                                                where l_orderkey = o_orderkey 
                                                    and o_custkey = c_custkey 
                                                    and c_nationkey = n_nationkey 
                                                    and n_nationkey in (3,24) 
                                                group by n_nationkey, n_name, l_shipmonth 
                                                order by n_nationkey, l_shipmonth asc""")
                                     .show(200, false)

        }
        */
        
    }
} 