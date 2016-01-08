package solution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;

import java.util.Hashtable;
import java.util.List;

import org.apache.commons.lang.math.LongRange;
import org.apache.commons.math.optimization.fitting.ParametricRealFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import solution.FinalProj.MyCounters;

public class Canopy {
	
public static class CanopyMapper
    extends Mapper<LongWritable, Text, IntWritable, StockWritable>{

 @Override
 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 	
	 // split the the line to name|days..
 	String[] data = value.toString().split("|");
 	
 	// create the 2D array
 	DoubleWritable[][] tmp2DArray = new DoubleWritable[data.length -1][];
 	
 	// Run on all days the fist cell is the stack name
 	for (int day = 1; day < data.length; day++) {
 		String[] singleDaypParametrs = data[day].split(" ");
 		tmp2DArray[day-1] = new DoubleWritable[singleDaypParametrs.length];
 		for (int paramter = 0; paramter < singleDaypParametrs.length; paramter++) {
 			tmp2DArray[day-1][paramter] = new DoubleWritable(Integer.parseInt(singleDaypParametrs[paramter]));
		}
 	}
 	
 	// create the stock vector
 	StockWritable tmpStock = new StockWritable(tmp2DArray, new Text(data[0]));
 	
 	// check if in on of the T1 	
 	
 	context.write(new LogWritable(new Text(line[3]), new Text(line[1]), new Text(line[2]), new IntWritable(Integer.parseInt(line[0]))),new IntWritable(1));   	  
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
