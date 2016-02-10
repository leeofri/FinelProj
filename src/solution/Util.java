package solution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;

import solution.StdRandom;

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


		return (new KMeansCenter(randomVector));
	}

	public static int numberOfRowsInSeqFile(Path path, Configuration conf) throws IOException, InstantiationException, IllegalAccessException {

		Reader reader = null;

		try {
			reader = new SequenceFile.Reader(conf, Reader.file(path));
		} catch (Exception e) {
			throw new IOException(e);
		}

		Writable key = (Writable) reader.getKeyClass().newInstance();
		Writable val = (Writable) reader.getValueClass().newInstance();

		int count = 0;
		while (reader.next(key, val)) {
			count++;
		}
		reader.close();
		return count;
	}

	public static Hashtable<String,KMeansCenter> getKmeansCenterFromFile(Path path, Configuration conf) throws IOException, InstantiationException, IllegalAccessException {

		Hashtable<String,KMeansCenter> fileKmeansCenters = new Hashtable<String, KMeansCenter>();
		
		Reader reader = null;

		try {
			reader = new SequenceFile.Reader( (FileSystem)FileSystem.getLocal(conf), path, conf);
		} catch (Exception e) {
			throw new IOException(e);
		}

		Writable key = (Writable) reader.getKeyClass().newInstance();
		KMeansCenter val =  new KMeansCenter();

		int count = 0;
		while (reader.next(key, val)) {
			fileKmeansCenters.put(val.getRealatedCanopyCenter().get().getName() + val.getCenter().getName().toString(), val);
			val =  new KMeansCenter();
		}
		
		reader.close();
		return fileKmeansCenters;
	}
	
	public static boolean comperKMeansCenter(Hashtable<String,KMeansCenter> first, Hashtable<String,KMeansCenter> second){
		
		// run on the hashtable and comper distances
		for (String kmeansCenterName : second.keySet()) {
			
			// debug
			System.out.println("comperKMeansCenter - center:" + kmeansCenterName + " diffrence(distance):" + first.get(kmeansCenterName).getCenter().distance(second.get(kmeansCenterName).getCenter()));			
			
			// check the distance
			if (first.get(kmeansCenterName).getCenter().distance(second.get(kmeansCenterName).getCenter()) > Globals.getKmeansZeroDistance())
			{
				return true;
			}	
		}
		
		return false;
	}
	
	public static Hashtable<String, List<KMeansCenter>> ReadingKmeans(Configuration conf, Path KmeansCentersPath)
			throws IOException {

		// Creat the result objecy
		Hashtable<String, List<KMeansCenter>> kmeansFromFile = new Hashtable<String, List<KMeansCenter>>(); 
		
		// Reading from the sequence file
		SequenceFile.Reader reader = null;

		try {
			reader = new SequenceFile.Reader((FileSystem)FileSystem.getLocal(conf),KmeansCentersPath,conf);
		} catch (Exception e) {
			throw new IOException(e);
		}

		Text key = new Text();
		KMeansCenter val = new KMeansCenter();

		while (reader.next(key, val)) {
			if (kmeansFromFile.containsKey(key.toString())) {
				kmeansFromFile.get(key.toString()).add(val);
			} else {
				List<KMeansCenter> kmeans = new ArrayList<KMeansCenter>();
				kmeans.add(val);
				kmeansFromFile.put(key.toString(), kmeans);
			}

			val = new KMeansCenter();

		}

		// close the reader
		reader.close();
		
		return kmeansFromFile;

	}
}
