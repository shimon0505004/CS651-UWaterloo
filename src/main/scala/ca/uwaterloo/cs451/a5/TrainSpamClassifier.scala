


package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.Map
import java.io.PrintWriter

import math._


class TrainSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, model)
    val input = opt[String]("input", descr = "input path", required = true)
    val model = opt[String]("model", descr = "model", required = true)
    verify()
}

object TrainSpamClassifier extends Tokenizer {

    val log = Logger.getLogger(getClass().getName())

    // w is the weight vector (make sure the variable is within scope)
    val w: Map[Int, Double] = Map[Int, Double]()
    val delta = 0.002

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
        var score = 0d
        features.foreach(f => if (w.contains(f)) score += w(f))
        score
    }

    def main(argv: Array[String]) {
        val args = new TrainSpamClassifierConf(argv)

        log.info("Input: " + args.input())
        log.info("Model Path: " + args.model())

        val conf = new SparkConf().setAppName("Train Spam Classifier")
        val sc = new SparkContext(conf)

        val modelPath = new Path(args.model())
        FileSystem.get(sc.hadoopConfiguration).delete(modelPath, true)

        val textFile = sc.textFile(args.input())

        val trained = textFile.map(line =>{
            // Parse input
            // ..
            val words = line.split(" +")    
            val docid = words(0)
            val isSpam:Double = if(words(1).matches("spam"))  1d else 0d
            val features:Array[Int] = words.slice(2, words.size).map(_.toInt)

            (0, (docid, isSpam, features))
            //(docid, w.toList)
        }).groupByKey(1)
        .map{case (key,values) => {
            values.foreach{ 
                case (docid, isSpam, features) =>{
                    // Update the weights as follows:
                    val score = spamminess(features)
                    val prob = 1.0 / (1 + exp(-score))
                    features.foreach(f => {
                        val base = (isSpam - prob) * delta
                        val offset = w getOrElse(f, 0d)
                        val updatedVal = base + offset
                        w(f) = updatedVal
                    })
                }            
            }

            w.toList
        }}  

        trained.saveAsTextFile(args.model())

        


        /*
        new PrintWriter(args.model()+"-weights") {
            w.foreach {
                case (k, v) =>{
                    write(k + ":" + v)
                    write("\n")
                }
            }
            close()
        }
        */


    }
}
