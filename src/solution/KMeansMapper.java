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

	private Hashtable<String, List<KMeansCenter>> kmeansCenters = new Hashtable<String, List<KMeansCenter>>();

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

		canopyCenter nearestCanopy = null;

		KMeansCenter nearestKmeans = null;

		for (String canopyName : kmeansCenters.keySet()) {
			canopyCenter currCanopy = kmeansCenters.get(canopyName).get(0)
					.getRealatedCanopyCenter();

			if (Globals.T2() <= currCanopy.get().distance(currStock)
					&& Globals.T1() > currCanopy.get().distance(currStock)) {
				for (KMeansCenter currKmean : kmeansCenters.get(canopyName)) {

					double nearestKmeansDistance = Double.MAX_VALUE;

					double dist = currKmean.getCenter().distance(currStock);

					if (nearestKmeansDistance > dist) {
						nearestKmeans = currKmean;
						nearestKmeansDistance = dist;
					}
				}
				
				System.out.println("Kmeans mapper - Kcenter:" +  nearestKmeans.getCenter().getName() + "   Canopy:" + nearestKmeans.getRealatedCanopyCenter().get().getName());
				context.write(nearestKmeans, currStock);
			}
		}
	}

	private Text CreateKey(Text text, Text text2) {

		Text result = new Text(String.valueOf(text) + String.valueOf(text2));

		return result;
	}

	private void ReadingKmeans(Configuration conf, Path KmeansCentersPath)
			throws IOException {

		// Reading from the sequence file
		SequenceFile.Reader reader = null;

		try {
			reader = new SequenceFile.Reader(conf,
					Reader.file(KmeansCentersPath));
		} catch (Exception e) {
			throw new IOException(e);
		}

		Text key = new Text();
		KMeansCenter val = new KMeansCenter();

		while (reader.next(key, val)) {
			if (kmeansCenters.containsKey(key.toString())) {
				kmeansCenters.get(key.toString()).add(val);
			} else {
				List<KMeansCenter> kmeans = new ArrayList<KMeansCenter>();
				kmeans.add(val);
				kmeansCenters.put(key.toString(), kmeans);
			}

			val = new KMeansCenter();

		}

		// close the reader
		reader.close();

	}

}
