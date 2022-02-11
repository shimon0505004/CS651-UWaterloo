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

package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.map.HMapKI;

import java.io.IOException;
import java.util.Iterator;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

/**
 * <p>
 * Implementation of the "pairs" algorithm for computing co-occurrence matrices from a large text
 * collection. This algorithm is described in Chapter 3 of "Data-Intensive Text Processing with 
 * MapReduce" by Lin &amp; Dyer, as well as the following paper:
 * </p>
 *
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the Masses: A Case Study in
 * Computing Word Co-occurrence Matrices with MapReduce.</b> <i>Proceedings of the 2008 Conference
 * on Empirical Methods in Natural Language Processing (EMNLP 2008)</i>, pages 419-428.</blockquote>
 *
 * @author Jimmy Lin
 */
public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  private static enum Job1LineCounter{
    LINE_COUNTER
  };

  // Mapper: emits (token, 1) for every unique word occurrence in every word.
  public static final class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
			
      Set<String> wordSet = new HashSet();

      List<String> words = Tokenizer.tokenize(value.toString());

      for (int i=0; i< Math.min(words.size(), 40); i++) {
        String word = words.get(i);
        if(!wordSet.contains(word)){
          wordSet.add(word);
          WORD.set(word);
          context.write(WORD, ONE);
        }
      }

      context.getCounter(Job1LineCounter.LINE_COUNTER).increment(1L);
    }
  }
  
  
  // Reducer: sums up all the counts.
  public static final class FirstCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects.
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

    
  // Reducer: Calculating C(y).
  public static final class FirstReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects.
    private static final IntWritable WORD_COUNT = new IntWritable();
    private int threshold = 1;

    @Override
    public void setup(Context context) {
      threshold = context.getConfiguration().getInt("threshold", 1);
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      WORD_COUNT.set(sum);
      context.write(key, WORD_COUNT);
    }
  }


  private static final class SecondMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private static final PairOfStrings KEYPAIR = new PairOfStrings();
    private static final IntWritable ONE = new IntWritable(1);

    private static final Set<String> uniquePairs = new HashSet();

    @Override
    public void setup(Context context) {

    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            
      List<String> words = Tokenizer.tokenize(value.toString());

      
      uniquePairs.clear();
      for(int i=0; i < Math.min(words.size(), 40); i++){        
        for(int j=0; j < Math.min(words.size(), 40); j++){
          
          if((i==j) || ((words.get(i).compareTo(words.get(j))) == 0)){
            continue;
          }

          KEYPAIR.set(words.get(i), words.get(j));

          if(!uniquePairs.contains(KEYPAIR.toString())){
            uniquePairs.add(KEYPAIR.toString());                //Put keypair (A,B) in the set. (B,A) will also be put in the set
            context.write(KEYPAIR, ONE);                        //Emit ((A,B), ONE) and sum them up
          }          
        }

      }

    }
  }

  private static final class SecondCombiner extends
      Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private static final IntWritable SUM = new IntWritable();
    
 
    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);

    }
  }  

  private static final class SecondReducer extends
    Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfFloatInt> {
    private static final PairOfFloatInt RESULT = new PairOfFloatInt();
    //private static final Text TEMPOUTPUT = new Text();

    private int threshold = 1;
    private long number_of_lines = 1L;
    private Map<String, Integer> countMapper = new HashMap<>();

    private float p_y = 0.0f;

    @Override
    public void setup(Context context)  throws IOException{

      threshold = context.getConfiguration().getInt("threshold", 1);
      number_of_lines = context.getConfiguration().getLong("numberOfLines", 1L);

      String sidedata_dir_path = context.getConfiguration().get("sidedata_dir", "_temp_PairsPMI");
      Path sidedata_dir = new Path(sidedata_dir_path);

      FileSystem fs = FileSystem.get(context.getConfiguration());
      PathFilter filter = new PathFilter() {

        @Override
        public boolean accept(Path path) {
            return path.getName().startsWith("part-r-");
        }
      };

      FileStatus[] fileStatuses = fs.listStatus(sidedata_dir, filter);
      for(FileStatus file: fileStatuses){
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath()) , "UTF-8"));
        String line = null;
        while((line = reader.readLine()) != null) {
          String[] words = line.split("\\s+");
          if(words.length == 2){
            String yKey = words[0];
            int c_y = Integer.parseInt(words[1]);
            countMapper.put(yKey, c_y);
          }
        }
        reader.close();
      }

    }

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int c_X_Y = 0;
      while (iter.hasNext()) {
        c_X_Y += iter.next().get();
      }
      
      int c_X = countMapper.get(key.getLeftElement());
      int c_Y = countMapper.get(key.getRightElement());

      if(c_X_Y >= threshold){
        float pmi_x_y = (float)(java.lang.Math.log10((1.0f * c_X_Y * number_of_lines) / (c_X * c_Y)));
        RESULT.set(pmi_x_y, c_X_Y);
        context.write(key, RESULT);
      }

    }
  }

  private static final class SecondPartitioner extends Partitioner<PairOfStrings, IntWritable> {
    @Override
    public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurrence")
    int threshold = 1;
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

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - threshold : " + args.threshold);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job job1 = Job.getInstance(getConf());
    job1.setJobName("Job1 - Count Lines and Words");
    job1.setJarByClass(PairsPMI.class);

    
    // Delete the Intermediate output directory if it exists already.
    String tempOutput = args.output + "_temp";
    Path tempOutputDir = new Path(tempOutput);
    FileSystem.get(getConf()).delete(tempOutputDir, true);

    job1.getConfiguration().setInt("threshold", args.threshold);
    job1.getConfiguration().set("sidedata_dir", tempOutput);

    job1.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, tempOutputDir);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setOutputFormatClass(TextOutputFormat.class);


    job1.setMapperClass(FirstMapper.class);
    job1.setCombinerClass(FirstCombiner.class);
    job1.setReducerClass(FirstReducer.class);


    job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    long startTime = System.currentTimeMillis();
    boolean successAtJob1 = job1.waitForCompletion(true);
    System.out.println("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    long lineCounter = job1.getCounters().findCounter(Job1LineCounter.LINE_COUNTER).getValue();

    if(successAtJob1){
      Job job2 = Job.getInstance(getConf());
      job2.setJobName(PairsPMI.class.getSimpleName() + " Job2 : Calculate PMI(x,y)");
      job2.setJarByClass(PairsPMI.class);

      // Delete the output directory if it exists already.
      Path outputDir = new Path(args.output);
      FileSystem.get(getConf()).delete(outputDir, true);

      job2.getConfiguration().setInt("threshold", args.threshold);
      job2.getConfiguration().set("sidedata_dir", tempOutput);
      job2.getConfiguration().setLong("numberOfLines", lineCounter);

      job2.setNumReduceTasks(args.numReducers);

      FileInputFormat.setInputPaths(job2, new Path(args.input));
      FileOutputFormat.setOutputPath(job2, new Path(args.output));

      job2.setMapOutputKeyClass(PairOfStrings.class);
      job2.setMapOutputValueClass(IntWritable.class);
      job2.setOutputKeyClass(PairOfStrings.class);
      job2.setOutputValueClass(PairOfFloatInt.class);
      job2.setOutputFormatClass(TextOutputFormat.class);

      job2.setMapperClass(SecondMapper.class);
      job2.setCombinerClass(SecondCombiner.class);
      job2.setReducerClass(SecondReducer.class);
      job2.setPartitionerClass(SecondPartitioner.class);

      job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
      job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
      job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
      job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
      job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

      startTime = System.currentTimeMillis();
      boolean successAtJob2 = job2.waitForCompletion(true);
      System.out.println("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

      if(successAtJob2){
        //Delete temporary directory once job 2 is successfully completed.
        FileSystem.get(getConf()).delete(tempOutputDir, true);
      }
    }  

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}