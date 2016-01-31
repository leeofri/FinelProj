package solution;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.Reducer;

//calculate a new clustercenter for these vertices
public class KMeansReducer extends
		Reducer<Text, StockWritable, KMeansCenter, StockWritable> {

	public static enum Counter {
		CONVERGED
	}

	List<KMeansCenter> centers = new LinkedList<KMeansCenter>();

	@Override
	protected void reduce(Text key, Iterable<StockWritable> values,
			Context context) throws IOException, InterruptedException {

		if (isLastReduce()) {

			// KMeansCenter center = new KMeansCenter(newCenter);
			//
			// centers.add(center);
			//
			// for (StockWritable vector : vectorList) {
			// context.write(center, vector);
			// }
			//
			// if (center.converged(key))
			// context.getCounter(Counter.CONVERGED).increment(1);
		} else {
			StockWritable newCenter;
			List<StockWritable> stocks = new LinkedList<StockWritable>();

			for (StockWritable value : values) {
				stocks.add(new StockWritable(value));
			}

			// first stock
			newCenter = stocks.get(0);

			int days = newCenter.get().get().length;
			int features = newCenter.get().get()[1].length;

			for (int currStock = 1; currStock < stocks.size(); currStock++) {

				for (int currDay = 1; currDay < days; currDay++) {

					for (int currFeature = 1; currFeature < features; currFeature++) {

						double value = getFeatureInDay(newCenter, currDay,currFeature)
								+ getFeatureInDay(stocks.get(currStock), currDay, currFeature);

						DoubleWritable temp = new DoubleWritable(value);

						newCenter.get().get()[currDay][currFeature] = temp;

					}
				}
			}
		}
	}

	private double getFeatureInDay(StockWritable newCenter, int currDay,
			int currFeature) {
		return ((DoubleWritable) newCenter.get().get()[currDay][currFeature])
				.get();
	}

	private boolean isLastReduce() {
		// TODO : need to read from cahce
		return false;
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		
		// super.cleanup(context);
		// Configuration conf = context.getConfiguration();
		// Path outPath = new Path(conf.get("centroid.path"));
		// FileSystem fs = FileSystem.get(conf);
		// fs.delete(outPath, true);
		// final SequenceFile.Writer out = SequenceFile.createWriter(fs,
		// context.getConfiguration(), outPath, KMeansCenter.class,
		// IntWritable.class);
		// final IntWritable value = new IntWritable(0);
		// for (KMeansCenter center : centers) {
		// out.append(center, value);
		// }
		// out.close();
	}
}
