package solution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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
		Mapper<LongWritable, Text, Text, StockWritable> {

	private Hashtable<canopyCenter, List<KMeansCenter>> kmeansCenters = new Hashtable<canopyCenter, List<KMeansCenter>>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		// Reading the canopy centers and the kmeans centers
		// Getting all the paths
		Path[] paths = context.getLocalCacheFiles();

		// Reading the canopy centers and the kmeans centers from diserbuted
		if (paths.length > 0) {
			ReadingKmeans(context.getConfiguration(), paths[0]);
		}
	}

	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StockWritable currStock = Util.GetStockFromLine(value);
		
		// Temp value distance that in the end will contain the lowest value
		canopyCenter nearestCanopy = kmeansCenters.keys().nextElement();
		
		// If the stock inside the t2 one of the canopies
		// moving to next stock
		if (Globals.T2() < getCanopyByIndex(0).get().distance(currStock) &&
			Globals.T2() < getCanopyByIndex(1).get().distance(currStock))  {
			
		}
		double nearestCanopyDistance = Double.MAX_VALUE;

		

		// Running throw all the centers
		for (canopyCenter currCanopy : kmeansCenters.keySet()) {

			// Checking what is the distance between the centroid and the curr
			// vector point
			double dist = currCanopy.get().distance(currStock);

			if (nearestCanopyDistance > dist) {

				nearestCanopy = currCanopy;
				nearestCanopyDistance = dist;
			}
		}
		
		KMeansCenter nearestKmeans = kmeansCenters.get(nearestCanopy).get(0);

		double nearestKmeansDistance = Double.MAX_VALUE;
		
		// Running throw all the centers
		for (KMeansCenter currKmean : kmeansCenters.get(nearestCanopy)) {

			// Checking what is the distance between the centroid and the curr
			// vector point
			double dist = currKmean.getCenter().distance(currStock);

			if (nearestKmeansDistance > dist) {

				nearestKmeans = currKmean;
				nearestKmeansDistance = dist;
			}
		}

		context.write(CreateKey(nearestCanopy.get().getName(), 
				nearestKmeans.getCenter().getName()), currStock);
	}

	private canopyCenter getCanopyByIndex(int i) {
		return (canopyCenter)kmeansCenters.keySet().toArray()[i];
	}


	private Text CreateKey(Text text, Text text2) {

		Text result = new Text(String.valueOf(text)
				+ String.valueOf(text2));

		return result;
	}
	
	private void ReadingKmeans(Configuration conf,Path KmeansCentersPath) throws IOException {

		// Reading from the sequence file
		SequenceFile.Reader reader = null;

		try {
			reader = new SequenceFile.Reader(conf, Reader.file(KmeansCentersPath));
		} catch (Exception e) {
			throw new IOException(e);
		}

		canopyCenter key = new canopyCenter();
		KMeansCenter val = new KMeansCenter();

		while (reader.next(key, val)) {
			if (kmeansCenters.containsKey(key)) {
				kmeansCenters.get(key).add(val);
			} else {
				List<KMeansCenter> kmeans = new ArrayList<KMeansCenter>();
				kmeans.add(val);
				kmeansCenters.put(key, kmeans);
			}
		}
		
		// close the reader
		reader.close();

	}


}
