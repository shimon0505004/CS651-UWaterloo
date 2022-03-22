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
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import org.rogach.scallop._
import java.io._  
import math._

class Q3Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String]("input", descr = "Input path", required = true)
    val date = opt[String]("date", descr = "Date in YYYY-MM-DD format", required = true)
    val text = opt[Boolean]("text", descr = "text command processes input as text file", required = false)
    val parquet = opt[Boolean]("parquet", descr = "parquet command processes input as parquet file", required = false)
    verify()
}

object Q3{
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]){
        val args = new Q3Conf(argv)

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
        val l_partkeyPos = 1
        val l_suppkeyPos = 2
        val p_partkeyPos = 0
        val s_suppkeyPos = 0
        val s_namePos = 1
        val p_namePos = 1
        

        val queryResult  = if(!isParquet){
            //Process as TXT file
            val lineitemRDD = sparkSession.sparkContext.textFile(args.input()+"/lineitem.tbl")
            val partRDD = sparkSession.sparkContext.textFile(args.input()+"/part.tbl")
            val supplierRDD = sparkSession.sparkContext.textFile(args.input()+"/supplier.tbl")
            
            val lineItemProjection = lineitemRDD.map(line => line.split('|'))
                .filter(_.apply(l_shipdatePos).equals(date))
                .map(line => ((line.apply(l_partkeyPos).toInt, line.apply(l_suppkeyPos).toInt),  line.apply(l_orderkeyPos).toInt))

            val partProjection = partRDD.map(line => line.split('|')).map(line => (line.apply(p_partkeyPos).toInt , line.apply(p_namePos)))
            val supplierProjection = supplierRDD.map(line => line.split('|')).map(line => (line.apply(s_suppkeyPos).toInt , line.apply(s_namePos)))

            val partkeysMap = partProjection.collectAsMap()
            val supplierMap = supplierProjection.collectAsMap()

            lineItemProjection.filter{case (key,value) => partkeysMap.contains(key._1)}
                .filter{case (key,value) => supplierMap.contains(key._2)}
                .map{case (key,value) => {
                    val p_name = partkeysMap.getOrElse(key._1,"Error")
                    val s_name = supplierMap.getOrElse(key._2,"Error")
                    (value, (p_name, s_name))
                }}.sortBy(_._1)
                .take(limit)
            
        }else{
            val lineitemRDD = sparkSession.read.parquet(args.input()+"/lineitem").rdd
            val partRDD = sparkSession.read.parquet(args.input()+"/part").rdd
            val supplierRDD = sparkSession.read.parquet(args.input()+"/supplier").rdd

            val lineItemProjection:RDD[((Int,Int), Int)] = lineitemRDD.filter(_.getString(l_shipdatePos).equals(date))
                                                                    .map(row => ( (row.getInt(l_partkeyPos), row.getInt(l_suppkeyPos)), row.getInt(l_orderkeyPos) ))
                                        
            val partProjection = partRDD.map(row => (row.getInt(p_partkeyPos), row.getString(p_namePos)))
            val supplierProjection = supplierRDD.map(row => (row.getInt(s_suppkeyPos), row.getString(s_namePos)))

            val partkeysMap = partProjection.collectAsMap()
            val supplierMap = supplierProjection.collectAsMap()

            lineItemProjection.filter{case (key,value) => partkeysMap.contains(key._1)}
                .filter{case (key,value) => supplierMap.contains(key._2)}
                .map{case (key,value) => {
                    val p_name = partkeysMap.getOrElse(key._1,"Error")
                    val s_name = supplierMap.getOrElse(key._2,"Error")
                    (value, (p_name, s_name))
                }}.sortBy(_._1)
                .take(limit)

        }

        queryResult.foreach(row => println("("+row._1+","+row._2._1+","+row._2._2+")"))
        
    }
} 