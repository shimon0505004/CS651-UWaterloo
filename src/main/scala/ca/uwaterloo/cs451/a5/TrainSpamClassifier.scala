


package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
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
    val w = Map[Int, Double]()
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

            // Update the weights as follows:
            val score = spamminess(features)
            val prob = 1.0 / (1 + exp(-score))
            features.foreach(f => {
                if (w.contains(f)) {
                    w(f) += (isSpam - prob) * delta
                } else {
                    w(f) = (isSpam - prob) * delta
                }
            })

            (0, (docid, isSpam, features))
        }).groupByKey(1)
            
        w.saveAsTextFile(args.model())
    }
}
