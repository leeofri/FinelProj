package solution;

import java.io.BufferedReader;
import solution.LogWritable;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import java.util.Hashtable;
import java.util.List;

import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FinalProj {

	public enum MyCounters {
		Counter
		}
	
	
  public static class LogMapper
       extends Mapper<LongWritable, Text, LogWritable, IntWritable>{

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	String[] line = value.toString().split("  +");
    	
    	context.write(new LogWritable(new Text(line[3]), new Text(line[1]), new Text(line[2]), new IntWritable(Integer.parseInt(line[0]))),new IntWritable(1));   	  
    }   
 }
  
  public static class LogCombainer
  extends Reducer<LogWritable,IntWritable,LogWritable,IntWritable> {

	public void reduce(LogWritable key, Iterable<IntWritable> values,
	                  Context context
	                  ) throws IOException, InterruptedException {
	 int counter = 0;
	 
	 for (IntWritable val : values) {
		  counter += val.get();
	 }

	 context.write(key, new IntWritable(counter));
	}
}
  
    
  public static class LogReducer
       extends Reducer<LogWritable,IntWritable,Text,IntWritable> {
 
    public void reduce(LogWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {;
                    
      // count the amount of time you get into the reduser
      context.getCounter(MyCounters.Counter).increment(1);
       
      // count the number of visits
      int counter = 0;
      System.out.println(key.GetKey());
      for (IntWritable val : values) {
    	  counter += val.get();
      }

      context.write(new Text(key.GetKey()), new IntWritable(counter));
    }
   
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
  
    Job job = Job.getInstance(conf, "FinelProj");
    job.setJarByClass(FinalProj.class);
    job.setMapperClass(LogMapper.class);
    job.setCombinerClass(LogCombainer.class);
    job.setReducerClass(LogReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapOutputKeyClass(LogWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    FileInputFormat.addInputPath(job, new Path(args[0]));

//    debug localhost
//    FileOutputFormat.setOutputPath(job, new Path("/home/training/Documents/output/32"));
//    FileInputFormat.addInputPath(job, new Path("/home/training/Documents/Web_logs/Log0.txt"));
    
    
    // print the counter
    //System.out.println("The amount of time we stept into the Reducer: " + job.getCounters().findCounter(MyCounters.Counter).getValue());
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
