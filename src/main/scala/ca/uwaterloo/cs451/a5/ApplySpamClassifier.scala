


package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.Map
import java.io.PrintWriter

import math._


class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, model)
    val input = opt[String]("input", descr = "input path", required = true)
    val output = opt[String]("output", descr = "output path", required = true)
    val model = opt[String]("model", descr = "saved model path", required = true)
    verify()
}

object ApplySpamClassifier {

    val log = Logger.getLogger(getClass().getName())
    var w: Map[Int, Double] = Map[Int, Double]()

    def spamminess(features: Array[Int]) : Double = {
        var score = 0d
        features.foreach(f => if (w.contains(f)) score += w(f))
        score
    }

    def main(argv: Array[String]) {
        val args = new ApplySpamClassifierConf(argv)

        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Previously Saved Model Path: " + args.model())

        val conf = new SparkConf().setAppName("Apply Spam Classifier")
        val sc = new SparkContext(conf)

        val outputPath = new Path(args.output())
        FileSystem.get(sc.hadoopConfiguration).delete(outputPath, true)

        val modelFile = sc.textFile(args.model())
        
        /*
        modelFile.foreach(line => {
            val words = line.substring(1, line.length()-1).split(",")
            val key:Int = words(0).toInt
            val value:Double = words(1).toDouble
            w += key -> value
        })
        */

        val modelMap = modelFile.map(line => {
            val words = line.substring(1, line.length()-1).split(",")
            val key:Int = words(0).toInt
            val value:Double = words(1).toDouble
            (key, value)
        }).collectAsMap()
        w = w.++(modelMap)
        
        val textFile = sc.textFile(args.input())

        val tested = textFile.map(line =>{
            // Parse input
            // ..
            val words = line.split(" +")    
            val docid = words(0)
            val actualLabel = words(1)
            val features:Array[Int] = words.slice(2, words.size).map(_.toInt)
            val score = spamminess(features)
             val predictedLabel = if(score > 0d) "spam" else "ham"

            (0, (docid, actualLabel, score, predictedLabel))
        }).groupByKey(1)
        .flatMap{case (key,values) => values}
     
        tested.saveAsTextFile(args.output())
    }
}