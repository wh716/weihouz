import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;    
public class WordCount {
 public static int rank=0;    
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
	    if(word.getLength()==7){
            context.write(word, one);
	}
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }
 public static class Reduce2 extends Reducer<IntWritable,Text,IntWritable,Text> {
	private Text value = new Text();
    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
	for (Text val : values) {
		value=val;
		
	}
        if(rank<100){
	rank=rank+1;
        context.write(key, value);
}
    }
 }
 private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
     public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
          }
          
          public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
              return -super.compare(b1, s1, l1, b2, s2, l2);
          }
      }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Path Temdir=new Path("/home/weihou/tem/");
    Path Temdir2=new Path("/home/weihou/tem2/");
    Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(WordCount.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, Temdir);
        
    if(job.waitForCompletion(true)){
	Job SORT = new Job(conf, "sort");
        SORT.setJarByClass(WordCount.class);
        FileInputFormat.addInputPath(SORT, Temdir);
        SORT.setInputFormatClass(SequenceFileInputFormat.class);
	SORT.setOutputFormatClass(TextOutputFormat.class);
        SORT.setMapperClass(InverseMapper.class);
        SORT.setNumReduceTasks(1);
	SORT.setReducerClass(Reduce2.class);
        FileOutputFormat.setOutputPath(SORT, new Path(args[1]));
        SORT.setOutputKeyClass(IntWritable.class);
        SORT.setOutputValueClass(Text.class);
        SORT.setSortComparatorClass(IntWritableDecreasingComparator.class);
	SORT.waitForCompletion(true);

			
}
 }
        
}
