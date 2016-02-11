package solution;

import java.io.IOException;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Reducer;

//calculate a new clustercenter for these vertices
public class KMeansReducer extends
		Reducer<KMeansCenter, StockWritable, Text, Text> {

	public static enum Counter {
		CONVERGED
	}

	List<KMeansCenter> centers = new LinkedList<KMeansCenter>();

	@Override
	protected void reduce(KMeansCenter key, Iterable<StockWritable> values,
			Context context) throws IOException, InterruptedException {

		// check last run trigger
		String isLastRun = context.getConfiguration().get("num.lastrun");
		isLastRun = Util.readFileToHDFS(context.getConfiguration(), "./finalrun/data/isLastRunTrigger");
		
		Util.writeFileToHDFS(context.getConfiguration(), "./finalrun/data/Leelog", isLastRun.toString(),true);
		if (Integer.parseInt(isLastRun.trim()) == 1) {
			Util.writeFileToHDFS(context.getConfiguration(), "./finalrun/data/LeelogInIf","in",true);
			for (StockWritable stock : values) {
				context.write(new Text(key.getRealatedCanopyCenter().get()
						.getName()
						+ key.getCenter().getName().toString()),
						stock.getName());
			}

		} else {
			// copy the old center
			KMeansCenter newCenter = new KMeansCenter(key);

			// calc the new vector
			// create the 2D array
			DoubleWritable[][] tmp2DArray = new DoubleWritable[Globals.daysNumber][Globals.featuresNumber];

			// init with 0
			for (int currDay = 0; currDay < Globals.daysNumber; currDay++) {
				for (int currFeature = 0; currFeature < Globals.featuresNumber; currFeature++) {
					tmp2DArray[currDay][currFeature] = new DoubleWritable(0);
				}
			}

			// stock counter
			int stockCouter = 0;

			// sum all the elements
			for (StockWritable stock : values) {
				stockCouter++;
				for (int currDay = 0; currDay < Globals.daysNumber; currDay++) {
					for (int currFeature = 0; currFeature < Globals.featuresNumber; currFeature++) {
						tmp2DArray[currDay][currFeature]
								.set(tmp2DArray[currDay][currFeature].get()
										+ getFeatureInDay(stock, currDay,
												currFeature));
					}
				}
			}

			// divaiding
			for (int currDay = 0; currDay < Globals.daysNumber; currDay++) {
				for (int currFeature = 0; currFeature < Globals.featuresNumber; currFeature++) {
					tmp2DArray[currDay][currFeature]
							.set(tmp2DArray[currDay][currFeature].get()
									/ stockCouter);
				}
			}

			// add the new stock vector to th new center
			newCenter.getCenter().set(tmp2DArray);

			centers.add(newCenter);
		}
	}

	private double getFeatureInDay(StockWritable newCenter, int currDay,
			int currFeature) {
		return ((DoubleWritable) newCenter.get().get()[currDay][currFeature])
				.get();
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);

		Configuration conf = context.getConfiguration();
		Path outPath = Globals.KmeansCenterPath();

		// get the old point
//		Hashtable<String, List<KMeansCenter>> oldCenters = Util.ReadingKmeans(
//				conf, context.getLocalCacheFiles()[0]);
		Hashtable<String, List<KMeansCenter>> oldCenters = Util.ReadingKmeans(
				conf, Globals.KmeansCenterPath());


		// delete the old centers seq file

		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);

		try {

			// write center to the file
			final Writer writer = SequenceFile.createWriter(conf,
					Writer.file(outPath),
					Writer.keyClass(Text.class),
					Writer.valueClass(KMeansCenter.class));

			// write the new centers
			for (KMeansCenter center : centers) {
				// write the new center
				writer.append(center.getRealatedCanopyCenter().get().getName(),
						center);

				// delete the beckup from old list
				List<KMeansCenter> currKCenterCluster = oldCenters.get(center
						.getRealatedCanopyCenter().get().getName().toString());

				// delete the center from list
				for (KMeansCenter currKCenter : currKCenterCluster) {
					if (currKCenter.getCenter().getName().toString()
							.equals(center.getCenter().getName().toString())) {
						currKCenterCluster.remove(currKCenter);
						break;
					}
				}
			}

			// write old centers with k-cluster size 0, aka didnt sent from
			// mapper to reducer
			for (String currClustr : oldCenters.keySet()) {
				// write the old remaining centers
				for (KMeansCenter center : oldCenters.get(currClustr)) {
					// write the old center
					writer.append(center.getRealatedCanopyCenter().get()
							.getName(), center);
				}
			}

			// close the writer
			writer.close();
		} catch (Exception e) {
			System.out.println("Kmeans reducer - final Writer :"
					+ e.getMessage());
		}
	}
}
