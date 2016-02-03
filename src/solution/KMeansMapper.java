package solution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;

//first iteration, k-random centers, in every follow-up iteration we have new calculated centers
public class KMeansMapper extends
		Mapper<KMeansCenter, StockWritable, Text, StockWritable> {

	Hashtable<canopyCenter, List<KMeansCenter>> kmeansCenters = new Hashtable<canopyCenter, List<KMeansCenter>>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		// Reading the canopy centers and the kmeans centers

		// Getting all the paths
		Path[] paths = context.getLocalCacheFiles();

		// Reading the canopy centers and the kmeans centers from diserbuted
		if (paths.length > 0) {

			ReadingKmeans(context.getConfiguration());
		}
	}

	private void ReadingKmeans(Configuration conf) throws IOException {

		// Reading from the sequence file
		SequenceFile.Reader reader = null;

		try {
			reader = new SequenceFile.Reader(conf, Reader.file(Globals
					.KmeansCenterPath()));
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

	}

	@Override
	protected void map(KMeansCenter key, StockWritable value, Context context)
			throws IOException, InterruptedException {

		// Temp value distance that in the end will contain the lowest value
		canopyCenter nearestCanopy = kmeansCenters.keys().nextElement();

		double nearestCanopyDistance = Double.MAX_VALUE;

		// TODO : LEE t2

		// Running throw all the centers
		for (canopyCenter currCanopy : kmeansCenters.keySet()) {

			// Checking what is the distance between the centroid and the curr
			// vector point
			double dist = currCanopy.get().distance(value);

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
			double dist = currKmean.getCenter().distance(value);

			if (nearestKmeansDistance > dist) {

				nearestKmeans = currKmean;
				nearestKmeansDistance = dist;
			}
		}

		context.write(CreateKey(nearestCanopy.get().getName(), 
				nearestKmeans.getCenter().getName()), value);
	}

	private Text CreateKey(Text text, Text text2) {

		Text result = new Text(String.valueOf(text)
				+ String.valueOf(text2));

		return result;
	}

}
