/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import math._

class StripesPMIConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String]("input", descr = "input path", required = true)
  val output = opt[String]("output", descr = "output path", required = true)
  val reducers = opt[Int]("reducers", descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int]("threshold", descr = "threshold", required = false, default = Some(1))
  verify()
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new StripesPMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Compute Stripes PMI")
    val sc = new SparkContext(conf)
    val threshold = args.threshold()

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    
    val lineCounterAccumulator = sc.longAccumulator("LineCounterAccumulator")

    val uniqueWordCounts = textFile
      .flatMap(line => {
        lineCounterAccumulator.add(1)  

        var uniquetokens: Set[String] = Set()
        tokenize(line).take(40).foreach(uniquetokens += _)

        if (uniquetokens.size > 1) uniquetokens.toList else List()
      })
      .map(token => (token, 1))
      .reduceByKey(_ + _, args.reducers())
      .collectAsMap()

    val broadcastVar = sc.broadcast(uniqueWordCounts)
    val numberOfLines = lineCounterAccumulator.value

    val uniquePairCounts = textFile
      .flatMap(line =>{
        var uniquetokens: Set[String] = Set()
        tokenize(line).take(40).foreach(uniquetokens += _)

        if (uniquetokens.size > 1) uniquetokens.toList.combinations(2).toList.flatMap(l => Seq((l.head, scala.collection.mutable.Map(l.last -> (1.0, 1))), (l.last, scala.collection.mutable.Map(l.head -> (1.0, 1))))).toList else List()
      })
      .reduceByKey((map1, map2) => {
        var map2Key = 0
        for(map2Key <- map2.keys){
          var updatedValue = map1.getOrElse(map2Key, (0.0, 0))
          val map2val = map2.getOrElse(map2Key, (0.0, 0))
          updatedValue = ((updatedValue._1 + map2val._1), (updatedValue._2 + map2val._2))
          map1 += (map2Key -> updatedValue )
        }
        map1
      }, args.reducers())
      .map(p =>{
        val key = p._1
        var filteredMap = p._2.retain((key, value) => value._2 >= threshold)
                
        val c_x = broadcastVar.value.get(key).get
        val updatedMap = filteredMap.map( p => {
          val c_y = broadcastVar.value.get(p._1).get
          val pmi = log10(((p._2._1) * (1.0) * numberOfLines) / (c_x * c_y))
          (p._1, (pmi, p._2._2))       
        })
        
        (key, updatedMap)
      })
      .filter(p => p._2.size > 0)

    uniquePairCounts.saveAsTextFile(args.output())

  }
}
