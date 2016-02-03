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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Canopy {
	

	
public static class canopyMapper
    extends Mapper<LongWritable, Text, IntWritable, canopyCenter>{

 // TODO: put T1,T2 to global cash
	private double T1 = 5;
	private double T2 = 0;
 private List<canopyCenter> mapperCanopyCenters;
 
 public void setup(Context context) throws IOException,InterruptedException {
	 this.mapperCanopyCenters = new ArrayList<canopyCenter>();
 } 

 @Override
 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 	
	 // split the the line to name|days..
 	String[] data = value.toString().split("\\|");
 	
 	// create the 2D array
 	DoubleWritable[][] tmp2DArray = new DoubleWritable[data.length -1][];
 	
 	// Run on all days the fist cell is the stack name
 	for (int day = 1; day < data.length; day++) {
 		String[] singleDaypParametrs = data[day].split(" ");
 		tmp2DArray[day-1] = new DoubleWritable[singleDaypParametrs.length];
 		for (int paramter = 0; paramter < singleDaypParametrs.length; paramter++) {
 			tmp2DArray[day-1][paramter] = new DoubleWritable(Double.parseDouble(singleDaypParametrs[paramter]));
		}
 	}
 	
 	// create the stock vector
 	StockWritable currStock = new StockWritable(tmp2DArray, new Text(data[0]));
 	
 	// run on lape of canopy on the current stack
 	int result = canopyPointCheck(currStock,this.mapperCanopyCenters,T1,T2);
 	
 	// increase the counter
 	if (result >= 0)
 	{
 		this.mapperCanopyCenters.get(result).increasCounter(1);
 	}
}
 
 @Override
 protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
         throws IOException,InterruptedException
         {
	 		// write the center point in the canopy to the hdfs 
	 		// run on all the centers
	 		for (int center = 0; center < this.mapperCanopyCenters.size(); center++) {
	 			context.write(new IntWritable(1), this.mapperCanopyCenters.get(center));
			}
         }
	}


public static class canopyReducer
    extends Reducer<IntWritable,canopyCenter,canopyCenter,Text> {

 public void reduce(IntWritable key, Iterable<canopyCenter> values,
                    Context context
                    ) throws IOException, InterruptedException {
	 // init the centers list
	 ArrayList<canopyCenter> mapperCanopyCenters = new ArrayList<canopyCenter>();
	 
     // Run on all the "local" mappers centers and preform canopy algoritem for custring
	 for (canopyCenter localCenter : values) {
		 // TODO: change the T1 & T2 
		 int result = canopyPointCheck(localCenter.get(),mapperCanopyCenters,5,0);
		 if (result >= 0 )
		 {
			 mapperCanopyCenters.get(result).increasCounter(localCenter.getClusterSize());
		 }
     }
	
	// Create the connection
	 Configuration conf = context.getConfiguration(); 
	 Writer writer = null; 
	 
	 try {
		 writer = SequenceFile.createWriter(conf,Writer.file(new Path("/home/training/workspace/FinalProj/data/SequenceFile.canopyCenters")),
	                             Writer.keyClass(Text.class),
	                             Writer.valueClass(canopyCenter.class));
	 }
	 catch (Exception e) {
	      throw new IOException(e);
	    }
	 
	//write the centers to HDFS
	 for (canopyCenter globalCenter : mapperCanopyCenters) {
		 writer.append(globalCenter.get().getName(), globalCenter);

     }
 }
}


// chech if the stock is center
// if in T1 return center index in list
// if in T2 return -2
// if stock is center return -1
 private static int canopyPointCheck(StockWritable currStock, List<canopyCenter> centers, double T1,double T2)
	{
		// check if in on of the T1
		int centersIndex = 0;
		double distance = 0;
		
		if (centers.size() != 0)
		{ 	
			 	// Run on all the centers
			 	do
			 	{		 		
			 		distance = currStock.distance(centers.get(centersIndex).get());
			 		centersIndex++;
			 	}
			 	while((centersIndex < centers.size()) && 
			 			(distance > T1));
			 	
		}
		 
	 	// Check if found a match
	 	if (centersIndex == centers.size()) {
	 		// dont found a relevent center for the current stock, add the stoce to the center point list
	 		centers.add(new canopyCenter(currStock));
	 		return -1;
	 	// Check if the point isnt in T2 for increase the cluster counter
	 	}else if (distance > T2)
	 	{
	 		// inform the the point is in T2
	 		return -2;
	 	}
	 	
	 	return centersIndex;
		}
}

