package solution;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import solution.ThirdParty.StdRandom;

public class Util {

	public static StockWritable GetStockFromLine(Text value) {
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
		return new StockWritable(tmp2DArray, new Text(data[0]));
	}

	public static KMeansCenter GetRendomKmeanCenterByCanapoy(double vectorRows,
			double vectorsCols, double Radios, canopyCenter sphereCenter) {

		// create the vector
		// DoubleWritable[][] tmp2DArray = new
		// DoubleWritable[N][C];
		StockWritable randomVector = new StockWritable(sphereCenter.get());

		for (int currFeature = 0; currFeature < vectorsCols; currFeature++) {

			double U = StdRandom.uniform(-1.0, 1.0) * Radios
					/ (vectorRows * vectorsCols);

			// print scaled vector
			for (int day = 0; day < vectorRows; day++)
				((DoubleWritable) randomVector.get().get()[day][currFeature])
						.set(((DoubleWritable) randomVector.get().get()[day][currFeature])
								.get() + U);

		}

		// check
		System.out.println("GetRendomKmeanCenterByCanapoy (check)-> canopy :"
				+ sphereCenter.get().getName().toString() + " T1:"
				+ Globals.T1() + " - dis: "
				+ sphereCenter.get().distance(randomVector));

		return (new KMeansCenter(randomVector));
	}

}
