import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexJob {

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    
    if (args.length != 2) {
      System.err.println("Usage: Word Count <input path> <output path>");
      System.exit(-1);
    }
    
    //Creating a Hadoop job and assigning a job name for identification.
    Job job = new Job();
    job.setJarByClass(InvertedIndexJob.class);
    job.setJobName("InvertedIndexJob");

    //The HDFS input and output directories to be fetched from the Dataproc job submission console.
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    //Providing the mapper and reducer class names.
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);

    //Setting the job object with the data types of output key(Text) and value(IntWritable).
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.waitForCompletion(true);

  }

  /*
  * This is the Mapper class. It extends the Hadoop's Mapper class.
  * This maps input key/value pairs to a set of intermediate(output) key/value pairs.
  * Here our input key is a LongWritable and input value is a Text.
  * And the output key is a Text and value is an Text.
  */
  public static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>{

    // Hadoop supported data types. This is a Hadoop specific datatype that is used to handle numbers and strings in a hadoop environment.
    // Intwritable and Text are used instead of Java's Integer and String datatypes.
    // Here 'one' is the number of occurances of the 'word' and is set to the value 1 during the Map process.
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      // Reading input one line at a time and tokenizing
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);

      //in order to fetch the file names
      InputSplit input_split = context.getInputSplit();
      
      //get file name without file name extension(.txt)
      String file_name = ((FileSplit) input_split).getPath().getName().split("\\.")[0];

      // Iterating through all the words available in that line and forming the key value pair.
      while (tokenizer.hasMoreTokens()) {

        word.set(tokenizer.nextToken());

        // Sending to output collector(Context) which in-turn passes the output to Reducer.
        // The output is as follows:
        //  'word1' 948234
        //  'word1' 948234
        //  'word2' 948234
        context.write(new Text(word), new Text(file_name));
      }
    }
  }

  /*
   * This is the Reducer class. It extends the Hadoop's Reducer class.
   * This maps the intermediate key/value pairs we get from the mapper to a set 
   * of output key/value pairs, where the key is the word and the value is file name:count.
   * Here our input key is a Text and input value is a Text.
   * And the output key is a Text and value is an Text.
   */ 
  public static class WordCountReducer extends Reducer<Text, Text, Text, Text> {
    
    // Reduce method collects the output of the Mapper and adds all data with the same DocId to get the DocId's count.
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
      int sum = 0;
      
      String file_name="";

      String docID_count="";

      boolean is_file_name = true;

      // Iterates through all the values available with a key and add them together and give the final result as the key and sum of its values.
      // in the one word with different values
      for (Text value : values) {

        if(is_file_name) {

          file_name = value.toString();

          is_file_name = false;

        }
        
        boolean is_the_same_file = file_name.equals(value.toString());
        
        // if it is a new  file name, set the current docID_count info, and set new file name is newe current file name
        if (!is_the_same_file) {
          
          docID_count += file_name + ":" + sum + "  ";

          file_name = value.toString();

          sum = 1;
          

        } else { //else value = the current file name, sum++
        
          sum += 1;

        }

      }

      // after completing one word job, making output pattern
      docID_count += file_name + ":" + sum + "\n"; 

      context.write(key, new Text(docID_count));

    }
  }
}