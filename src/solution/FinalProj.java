package solution;

import java.io.BufferedReader;
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

import solution.Canopy.canopyMapper;
import solution.Canopy.canopyReducer;


public class FinalProj {
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
  
    Job job = Job.getInstance(conf, "FinelProj");
    job.setJarByClass(FinalProj.class);
    job.setMapperClass(canopyMapper.class);
//    job.setCombinerClass(LogCombainer.class);
    job.setReducerClass(canopyReducer.class);
    job.setOutputKeyClass(canopyCenter.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(canopyCenter.class);


//    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//    FileInputFormat.addInputPath(job, new Path(args[0]));

//    debug localhost
    FileOutputFormat.setOutputPath(job, new Path("/home/training/workspace/FinalProj/output"));
    FileInputFormat.addInputPath(job, new Path("/home/training/workspace/FinalProj/input"));
    
    
    // print the counter
    //System.out.println("The amount of time we stept into the Reducer: " + job.getCounters().findCounter(MyCounters.Counter).getValue());
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
