package solution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//first iteration, k-random centers, in every follow-up iteration we have new calculated centers
public class KMeansMapper extends
		Mapper<KMeansCenter, StockWritable, Text, StockWritable> {

	List<canopyCenter> canopyCenters = new LinkedList<canopyCenter>();
	List<KMeansCenter> kmeansCenters = new LinkedList<KMeansCenter>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		// Getting all the paths
		Path[] paths = context.getLocalCacheFiles();

		// Reading the canopy centers and the kmeans centers from diserbuted
		if (paths.length > 0) {

			ReadingCanopyCenters(paths[0].toString());

			ReadingKmeansCenters(paths[1].toString());
		}
	}

	void ReadingCanopyCenters(String canopyath) {

		// The reader
		BufferedReader br;

		try {
			br = new BufferedReader(new FileReader(canopyath));

			String canopyRow;

			// Reading all the words
			while ((canopyRow = br.readLine()) != null) {

				StockWritable stock = new StockWritable();

				// TODO : Fill the canopy

				canopyCenter canopyCenter = new canopyCenter(stock);
				canopyCenters.add(canopyCenter);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	void ReadingKmeansCenters(String kmeansPath) {

		// The reader
		BufferedReader br;

		try {
			br = new BufferedReader(new FileReader(kmeansPath));

			String kmeansLine;

			// Reading all the words
			while ((kmeansLine = br.readLine()) != null) {

				StockWritable stock = new StockWritable();

				// TODO : Fill the canopy

				KMeansCenter kmeansCenter = new KMeansCenter();
				kmeansCenters.add(kmeansCenter);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	protected void map(KMeansCenter key, StockWritable value, Context context)
			throws IOException, InterruptedException {

		// Temp value distance that in the end will contain the lowest value
		int canopyNumber = 0;

		canopyCenter nearestCanopy = null;

		double nearestCanopyDistance = Double.MAX_VALUE;

		// Running throw all the centers
		for (canopyCenter currCanopy : canopyCenters) {

			// Checking what is the distance between the centroid and the curr
			// vector point
			double dist = currCanopy.get().distance(value);

			// Finding the nearest center to the point
			if (nearestCanopy == null) {

				nearestCanopy = currCanopy;
				nearestCanopyDistance = dist;

			} else {
				if (nearestCanopyDistance > dist) {

					nearestCanopy = currCanopy;
					nearestCanopyDistance = dist;
				}
			}
		}

		KMeansCenter nearestKmeans = null;

		int kmeansNumber = 0;

		double nearestKmeansDistance = Double.MAX_VALUE;

		// Running throw all the centers
		for (KMeansCenter currKmeanCenter : kmeansCenters) {

			// Checking what is the distance between the centroid and the curr
			// vector point
			double dist = currKmeanCenter.getCenter().distance(value);

			// Finding the nearest center to the point
			if (nearestCanopy == null) {

				nearestKmeans = currKmeanCenter;
				nearestKmeansDistance = dist;

			} else {
				if (nearestKmeansDistance > dist) {

					nearestKmeans = currKmeanCenter;
					nearestKmeansDistance = dist;
				}
			}
		}

		context.write(CreateKey(canopyNumber, kmeansNumber), value);
	}

	private Text CreateKey(int canopyNumber, int kmeansNumber) {
		
		Text result = new Text(String.valueOf(canopyNumber) + 
							   String.valueOf(kmeansNumber));

		return result;
	}

}
