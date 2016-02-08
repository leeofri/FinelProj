package solution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import solution.ThirdParty.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import solution.Canopy.canopyMapper;
import solution.Canopy.canopyReducer;

public class FinalProj {
	
	private static int usedKmeans = 0;
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// Reading from config file from the user
		ReadingUserConfigFile();

		// Canopy
		Job job = Job.getInstance(conf, "FinelProj.Canopy");
		job.setJarByClass(FinalProj.class);
		job.setMapperClass(canopyMapper.class);
		job.setReducerClass(canopyReducer.class);
		job.setOutputKeyClass(canopyCenter.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(canopyCenter.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Global cache
		// DistributedCache.addCacheFile((new
		// Path("/home/training/workspace/FinalProj/data/job.config")).toUri(),
		// job.getConfiguration());

		// FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// FileInputFormat.addInputPath(job, new Path(args[0]));

		// debug localhost
		FileOutputFormat.setOutputPath(job, Globals.OutputFolderCanopy());
		FileInputFormat.addInputPath(job, Globals.InputFolder());
		
		// TODO : lee delete debug 
	    IsDeleteUtputFolder(true);
		
	    // run canopy
		job.waitForCompletion(true);
		
		// Kmeans
		Configuration Kmeansconf = new Configuration();
		
		// Adding the canopy centers and the kmeans centres to the cache
		// kmeans get the canopy centers from SequenceFile
		InitKmeansJobSequenceFile(Kmeansconf);
		
		Job kmeansJob = Job.getInstance(Kmeansconf, "FinelProj.KMeans");
		kmeansJob.setJarByClass(FinalProj.class);
		kmeansJob.setMapperClass(KMeansMapper.class);
		kmeansJob.setReducerClass(KMeansReducer.class);
		kmeansJob.setOutputKeyClass(canopyCenter.class);
		kmeansJob.setOutputValueClass(Text.class);
		kmeansJob.setMapOutputKeyClass(IntWritable.class);
		kmeansJob.setMapOutputValueClass(canopyCenter.class);
		kmeansJob.setOutputFormatClass(SequenceFileOutputFormat.class);

	

		// FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// FileInputFormat.addInputPath(job, new Path(args[0]));

		// debug localhost
		FileOutputFormat.setOutputPath(kmeansJob, Globals.OutputFolderKmeans());
		FileInputFormat.addInputPath(kmeansJob, Globals.InputFolder());

		// print the counter
		// System.out.println("The amount of time we stept into the Reducer: " +
		// job.getCounters().findCounter(MyCounters.Counter).getValue());
		kmeansJob.waitForCompletion(true);
	}

	private static void IsDeleteUtputFolder(Boolean indicate) {
		if (indicate) {
			// Deleting the output folder
			try {
				File outputFolder = new File("output");
				FileUtils.deleteDirectory(outputFolder);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// Deleting the canopy seq file 
			try {
				File outputFolder = new File("data/SequenceFile.canopyCenters");
				outputFolder.delete();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void ReadingUserConfigFile() throws IOException {
		try {
			Path pt = Globals.UserConfigFilePath();
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));
			String line;
			line = br.readLine();
			

			String[] values = line.split(" ");

			// TODO : LEE reflecation 
			Globals.setKmeansCount(Integer.parseInt(values[1]));
			Globals.setDaysNumber(Integer.parseInt(values[3]));
			Globals.setFeaturesNumber(Integer.parseInt(values[5]));
			
			// spilits parameter
			String filedType;
			String val = "";
			
			while (line != null) {
				System.out.println(line);
				line = br.readLine();
			}


		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new IOException(e);
		
		}
	}

	private static void InitKmeansJobSequenceFile(Configuration conf)
			throws Exception {

		SequenceFile.Reader reader = null;

		try {
			reader = new SequenceFile.Reader(conf, Reader.file(Globals
					.CanopyCenterPath()));
		} catch (Exception e) {
			throw new IOException(e);
		}

		List<canopyCenter> canopyCentres = new ArrayList<canopyCenter>();

		Text key = new Text();
		canopyCenter val = new canopyCenter();

		int stockCount = 0;

		while (reader.next(key, val)) {
			stockCount += val.getClusterSize();
			canopyCentres.add(val);
		}

		// Create the connection
		Writer writer = null;

		try {
			writer = SequenceFile.createWriter(conf,
					Writer.file(Globals.KmeansCenterPath()),
					Writer.keyClass(canopyCenter.class),
					Writer.valueClass(KMeansCenter.class));
		} catch (Exception e) {
			throw new IOException(e);
		}

		for (canopyCenter canopyCenter : canopyCentres) {

			int kmeansNumber = calcKmeansForCanopy(
					canopyCenter.getClusterSize(), stockCount);

			for (int i = 0; i < kmeansNumber; i++) {

				KMeansCenter randomKmeans = GetRendomKmeanCenterByCanapoy(
						Globals.daysNumber, Globals.T1(), canopyCenter);

				// Giving name for the kmeans - this name used by the kmeans
				// mapper
				randomKmeans.getCenter().setName(String.valueOf(i));

				writer.append(canopyCenter, randomKmeans);
			}
		}
		
		// close the writer
		writer.close();

		// add the SequenceFile to the global
		try {
			DistributedCache.addCacheFile(Globals.KmeansCenterPath().toUri(),conf);
			//DistributedCache.addLocalFiles(conf,Globals.KmeansCenterPath().toString());
		} catch (Exception e) {
			System.out
					.println("ERROR - InitKmeansJobSequenceFile - problam with adding the kmeans seq file: "
							+ Globals.KmeansCenterPath().toUri());
			throw e;
		}

	}

	private static int calcKmeansForCanopy(double stocksPerCanopy,
			double stockCount) {

		double propotion = stocksPerCanopy / stockCount;
		
		// Is first canopy
		if (usedKmeans == 0) {
			// In case that is 0
			if ((int) Math.floor(Globals.kmeansCount * propotion) == 0) {
				usedKmeans = 1;
				return 1;
			}
			else {
				usedKmeans = (int) Math.round(Globals.kmeansCount * propotion);
				
				return usedKmeans;
			}	
		}
		else {
			int temp = usedKmeans;
			usedKmeans = 0;
			return Globals.kmeansCount - temp;
		}
	}

	private static KMeansCenter GetRendomKmeanCenterByCanapoy(double N, double R,
			canopyCenter sphereCenter) {

		// create the vector
		// DoubleWritable[][] tmp2DArray = new
		// DoubleWritable[N][Globals.featuresNumber];
		StockWritable randomVector = new StockWritable(sphereCenter.get());

		for (int currFeature = 0; currFeature < Globals.featuresNumber; currFeature++) {
			
			double U = StdRandom.uniform(-1.0, 1.0) * R / (N*Globals.featuresNumber);
			
			// print scaled vector
			for (int day = 0; day < N; day++)
				((DoubleWritable) randomVector.get().get()[day][currFeature])
						.set(((DoubleWritable) randomVector.get().get()[day][currFeature])
								.get() + U);

		}

		// check
		System.out.println("GetRendomKmeanCenterByCanapoy (check)-> T1:"
				+ Globals.T1() + " - dis: "
				+ sphereCenter.get().distance(randomVector));

		return (new KMeansCenter(randomVector));
	}
}
