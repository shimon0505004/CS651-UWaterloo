package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.Map
import org.rogach.scallop._
import java.io._  
import math._

class ApplyEnsembleSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, model, method)
    val input = opt[String]("input", descr = "input path", required = true)
    val output = opt[String]("output", descr = "output path", required = true)
    val model = opt[String]("model", descr = "model", required = true)
    val method = opt[String]("method", descr = "method : average/vote", required = true)
    verify()
}

object ApplyEnsembleSpamClassifier {

    val log = Logger.getLogger(getClass().getName())

    def spamminess(w: Map[Int, Double], features: Array[Int]) : Double = {
        var score = 0d
        features.foreach(f => if (w.contains(f)) score += w(f))
        score
    }

    def getListOfFiles(dir: String): List[String] = {
        //Reference: https://stackoverflow.com/questions/48162153/get-list-of-files-from-directory-in-scala
        val file = new File(dir)
        file.listFiles.filter(_.isFile)
            .filter(_.getName.startsWith("part-"))
            .map(_.getPath).toList
    }

    def main(argv: Array[String]) {
        val args = new ApplyEnsembleSpamClassifierConf(argv)

        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Previously Saved Model Path: " + args.model())
        log.info("method : average/vote: " + args.method())

        val conf = new SparkConf().setAppName("Apply Ensamble Spam Classifier")
        val sc = new SparkContext(conf)

        val outputPath = new Path(args.output())
        FileSystem.get(sc.hadoopConfiguration).delete(outputPath, true)

        val textFile = sc.textFile(args.input(), 1)
        val outputType:String = args.method()

        val modelFiles = getListOfFiles(args.model())
        val models = modelFiles.map(filepath =>{
            sc.broadcast(sc.textFile(filepath).map(line => {
                val words = line.substring(1, line.length()-1).split(",")
                val key:Int = words(0).toInt
                val value:Double = words(1).toDouble
                (key, value)
            }).collectAsMap())
        })

        val tested = textFile.map(line =>{
            // Parse input
            // ..
            val words = line.split(" +")    
            val docid = words(0)
            val actualLabel = words(1)
            val features:Array[Int] = words.slice(2, words.size).map(_.toInt)
            val scores = models.map(model => spamminess(model.value, features))

            var score1 = scores.sum / scores.length
            val predictedLabel1 = if(score1 > 0d) "spam" else "ham"
            val retval1 = (0, (docid, actualLabel, score1, predictedLabel1))        

            val numberOfSpams = scores.filter(_ > 0).length
            val numberOfHams = scores.length - numberOfSpams
            val score2 = numberOfSpams - numberOfHams
            val predictedLabel2 = if(score2 > 0d) "spam" else "ham"
            val retval2 = (0, (docid, actualLabel, score2, predictedLabel2))

            if(outputType.matches("average")) retval1 else retval2
        }).groupByKey(1)
        .flatMap{case (key,values) => values}
     
        tested.saveAsTextFile(args.output())

    }

}