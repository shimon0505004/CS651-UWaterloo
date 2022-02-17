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

package ca.uwaterloo.cs451.a3;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringInt;
import tl.lin.data.pair.PairOfWritables;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;


public class BooleanRetrievalCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BooleanRetrievalCompressed.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {

    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        IntWritable tfCount = new IntWritable(e.getRightElement());
        PairOfStringInt wordDocidPair = new PairOfStringInt(e.getLeftElement(), ((int) docno.get()));
        context.write(wordDocidPair, tfCount);  
      }
    }
  }

  private static final class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }


  private static final class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, Text, BytesWritable> {
    
    /**
     * In BytesWritable value, Variable length integers are stored. The first integer is the size n, which indicates n pairs
     * of vints stored in this BytesWritable object after the size.
     * BytesWritable have a <VInt, ByteArray> pair. To read, call readVInt followedby readCompressedByteArray
     */
    
    private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    private DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    private String previousTerm = null;
    private int df = 0;
    private int previousDocID = 0;
    private Text previousTermText = new Text();
    
    ArrayListWritable<PairOfInts> postings = new ArrayListWritable<>();

    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
           
      String currentTerm = key.getLeftElement();
      int currentDocID = key.getRightElement();

      if(!currentTerm.equals(previousTerm) && previousTerm != null){
        WritableUtils.writeVInt(dataOutputStream, df);                                //Putting document frequency in front of the bytestream
        for(PairOfInts posting: postings){
          WritableUtils.writeVInt(dataOutputStream, posting.getLeftElement());
          WritableUtils.writeVInt(dataOutputStream, posting.getRightElement());
        }
        postings.clear();

        dataOutputStream.flush();
        previousTermText.set(previousTerm);
        context.write(previousTermText, new BytesWritable(byteArrayOutputStream.toByteArray()));
        byteArrayOutputStream.reset(); 
        
        df = 0;                   //Reset the document frequency for setting up df for next term.
        previousDocID = 0;        //For next term, previous document ID is set to zero so that we can reset the gap compression.
      }

      df++;
      
      int delta = (currentDocID - previousDocID);
      int tf = 0 ;
      while(iter.hasNext()){
        tf += iter.next().get();
      }

      postings.add(new PairOfInts(delta, tf));

      previousTerm = currentTerm;
      previousDocID = currentDocID;
    }

    @Override
    public void cleanup(Context context) 
        throws IOException, InterruptedException {

      WritableUtils.writeVInt(dataOutputStream, df);                                //Putting document frequency in front of the bytestream
      for(PairOfInts posting: postings){
        WritableUtils.writeVInt(dataOutputStream, posting.getLeftElement());
        WritableUtils.writeVInt(dataOutputStream, posting.getRightElement());
      }
      postings.clear();

      dataOutputStream.flush();
      previousTermText.set(previousTerm);
      context.write(previousTermText, new BytesWritable(byteArrayOutputStream.toByteArray()));
      byteArrayOutputStream.reset(); 

      dataOutputStream.close();
      byteArrayOutputStream.close();
    }


  }

  private BooleanRetrievalCompressed() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + BooleanRetrievalCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BooleanRetrievalCompressed.class.getSimpleName());
    job.setJarByClass(BooleanRetrievalCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setPartitionerClass(MyPartitioner.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}
