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

	public static class canopyMapper extends
			Mapper<LongWritable, Text, IntWritable, canopyCenter> {

		private List<canopyCenter> mapperCanopyCenters;

		public void setup(Context context) throws IOException,
				InterruptedException {
			this.mapperCanopyCenters = new ArrayList<canopyCenter>();
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// split the the line to name|days..
			String[] data = value.toString().split("\\|");

			// create the 2D array
			DoubleWritable[][] tmp2DArray = new DoubleWritable[data.length - 1][];

			// Run on all days the fist cell is the stack name
			for (int day = 1; day < data.length; day++) {
				String[] singleDaypParametrs = data[day].split(" ");
				tmp2DArray[day - 1] = new DoubleWritable[singleDaypParametrs.length];
				for (int paramter = 0; paramter < singleDaypParametrs.length; paramter++) {
					tmp2DArray[day - 1][paramter] = new DoubleWritable(
							Double.parseDouble(singleDaypParametrs[paramter]));
				}
			}

			// create the stock vector
			StockWritable currStock = new StockWritable(tmp2DArray, new Text(
					data[0]));

			// run on lape of canopy on the current stack
			int result = canopyPointCheck(currStock, this.mapperCanopyCenters,
					Globals.T1(), Globals.T2());

			// increase the counter
			if (result >= 0) {
				this.mapperCanopyCenters.get(result).increasCounter(1);
			}
		}

		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// write the center point in the canopy to the hdfs
			// run on all the centers
			for (int center = 0; center < this.mapperCanopyCenters.size(); center++) {
				context.write(new IntWritable(1),
						this.mapperCanopyCenters.get(center));
			}
		}
	}

	public static class canopyReducer extends
			Reducer<IntWritable, canopyCenter, canopyCenter, Text> {

		public void reduce(IntWritable key, Iterable<canopyCenter> values,
				Context context) throws IOException, InterruptedException {
			// init the centers list
			ArrayList<canopyCenter> mapperCanopyCenters = new ArrayList<canopyCenter>();
			
			// Run on all the "local" mappers centers and preform canopy
			// algoritem for custring
			for (canopyCenter localCenter : values) {
				int result = canopyPointCheck(localCenter.get(),
						mapperCanopyCenters, 5, 0);
				if (result >= 0) {
					mapperCanopyCenters.get(result).increasCounter(
							localCenter.getClusterSize());
				}else if (result == -1)
				{
					mapperCanopyCenters.get(mapperCanopyCenters.size()-1).increasCounter(localCenter.getClusterSize() -1);
				}
			}

			// Create the connection
			Configuration conf = context.getConfiguration();
			Writer writer = null;

			try {
				writer = SequenceFile.createWriter(conf,
						Writer.file(Globals.CanopyCenterPath()),
						Writer.keyClass(Text.class),
						Writer.valueClass(canopyCenter.class));
			} catch (Exception e) {
				throw new IOException(e);
			}

			// write the centers to HDFS
			for (canopyCenter globalCenter : mapperCanopyCenters) {
				writer.append(globalCenter.get().getName(), globalCenter);

			}
			
			writer.close();
		}
	}

	// chech if the stock is center
	// if in Globals.T1() return center index in list
	// if in Globals.T2() return -2
	// if stock is center return -1
	private static int canopyPointCheck(StockWritable currStock,
			List<canopyCenter> centers, double T1, double T2) {
		// check if in on of the Globals.T1()
		int centersIndex = 0;
		double distance = 0;

		if (centers.size() != 0) {
			// Run on all the centers
			do {
				distance = currStock.distance(centers.get(centersIndex).get());	
				centersIndex++;
			} while ((centersIndex < centers.size()) && (distance > T1));
			
			//return the index to the distance comper state
			centersIndex--;
			
			// debug
			System.out.println("canopyPointCheck - currstack :" + currStock.getName() + " distance: " + distance );
			
			// Check if didnt found a match
			if (distance > T1 && centersIndex == centers.size()-1) {
				// dont found a relevent center for the current stock, add the stoce
				// to the center point list
				centers.add(new canopyCenter(currStock));
				return -1;
				// Check if the point isnt in Globals.T2() for increase the cluster counter
			} else if (distance < T2) {
				// inform the the point is in Globals.T2()
				return -2;
			}

			return centersIndex;
		}else
		{
			// add the first center
			centers.add(new canopyCenter(currStock));
			return -1;
		}
		
	}
}
