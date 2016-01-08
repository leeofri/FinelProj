package solution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;

import java.util.ArrayList;
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


public class Canopy {
	
private double T1 = 1;
private double T2 = 0;
	
public static class CanopyMapper
    extends Mapper<LongWritable, Text, IntWritable, StockWritable>{
}

 private List<StockWritable> mapperCanopyCenters;
 
 public void setup(Context context) throws IOException,InterruptedException {
	 this.mapperCanopyCenters = new ArrayList<StockWritable>();
 } 

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
 	int centersIndex = -1;
 	
 	// Run on all the centers
 	do
 	{
 		centersIndex++;
 	}
 	while((centersIndex <= this.mapperCanopyCenters.size()) && 
 			(tmpStock.distance(this.mapperCanopyCenters.get(centersIndex)) > this.T1));
 	
 	// Check if found a match
 	if (centersIndex > this.mapperCanopyCenters.size()) {
 		// dont found a relevent center for the current stock, add the stoce to the center point list
 		this.mapperCanopyCenters.add(tmpStock);
 		
	}   	  
}   


}
