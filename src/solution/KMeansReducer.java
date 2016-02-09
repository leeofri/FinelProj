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

		if (Globals.isLastReduce()) {

			for (StockWritable stock : values) {
				context.write(new Text(key.getRealatedCanopyCenter().get().getName() + key.getCenter().getName().toString()), stock.getName());
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
					tmp2DArray[currDay][currFeature].set(tmp2DArray[currDay][currFeature].get()/stockCouter);
				}
			}
			
			// add the new stock vector to th new center
			newCenter.getCenter().set(tmp2DArray);

			//debug
			System.out.println("Kmeans reduce - distance (new-old):" + newCenter.getCenter().distance(key.getCenter()));
			
			centers.add(newCenter);
		}
	}

	private double getFeatureInDay(StockWritable newCenter, int currDay,
			int currFeature) {
		//System.out.println("getFeatureInDay - Name:"+ newCenter.getName() +" Day:" + currDay +" Feature: " + currFeature);
		return ((DoubleWritable) newCenter.get().get()[currDay][currFeature])
				.get();
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		 super.cleanup(context);
		 
		 // delete the old centers seq file
		 Configuration conf = context.getConfiguration();
		 Path outPath = Globals.KmeansCenterPath();
		 FileSystem fs = FileSystem.get(conf);
		 fs.delete(outPath, true);
		 
		// write center to the file
		Writer writer = null;

		try {
			writer = SequenceFile.createWriter(context.getConfiguration(),
					Writer.file(new Path("data/lee.sq")),
					Writer.keyClass(Text.class),
					Writer.valueClass(KMeansCenter.class));
		} catch (Exception e) {
			throw new IOException(e);
		}

		// write the new centes
		 for (KMeansCenter center : centers) {
			// write the new center
			writer.append(center.getRealatedCanopyCenter().get().getName(),
						center);
		 }
		 
		// close the writer
		writer.close();
	}
}
