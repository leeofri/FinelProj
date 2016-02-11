package solution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;

//first iteration, k-random centers, in every follow-up iteration we have new calculated centers
public class KMeansMapper extends
		Mapper<LongWritable, Text, KMeansCenter, StockWritable> {

	private Hashtable<String, List<KMeansCenter>> kmeansCenters; 
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		// Reading the canopy centers and the kmeans centers
		// Getting all the paths
		Path[] paths = context.getLocalCacheFiles();
		
		//System.out.println("KmeansMapper DistributedCache - num of file in cache:" + paths.length);
		// Reading the canopy centers and the kmeans centers from diserbuted
	//	if (paths.length > 0) {
			kmeansCenters = Util.ReadingKmeans(context.getConfiguration(), Globals.KmeansCenterPath());
	//	}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		StockWritable currStock = Util.GetStockFromLine(value);
		// Temp value distance that in the end will contain the lowest value

		canopyCenter nearestCanopy = null;
		double nearestKmeansDistance = Double.MAX_VALUE;
		KMeansCenter nearestKmeans = null;
		Boolean isFound = false;

		for (String canopyName : kmeansCenters.keySet()) {
			canopyCenter currCanopy = kmeansCenters.get(canopyName).get(0)
					.getRealatedCanopyCenter();

			if (Globals.T2() <= currCanopy.get().distance(currStock)
					&& Globals.T1() > currCanopy.get().distance(currStock)) {

				for (KMeansCenter currKmean : kmeansCenters.get(canopyName)) {

					double dist = currKmean.getCenter().distance(currStock);

					if (nearestKmeansDistance > dist) {
						nearestKmeans = currKmean;
						nearestKmeansDistance = dist;
					}
				}
				
				// write to output
				context.write(nearestKmeans, currStock);
				isFound = true;

			}

			if (isFound) {
				break;
			}

		}
	}

	private Text CreateKey(Text text, Text text2) {

		Text result = new Text(String.valueOf(text) + String.valueOf(text2));

		return result;
	}

}
