package solution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import solution.ThirdParty.*;
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
		FileOutputFormat.setOutputPath(job, new Path(
				"/home/training/workspace/FinalProj/output/Canopy"));
		FileInputFormat.addInputPath(job, new Path(
				"/home/training/workspace/FinalProj/input"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		// Kmeans
		Job kmeansJob = Job.getInstance(conf, "FinelProj.KMeans");
		job.setJarByClass(FinalProj.class);
		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		job.setOutputKeyClass(canopyCenter.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(canopyCenter.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Adding the canopy centers and the kmeans centres to the cache
		// kmeans get the canopy centers from SequenceFile

		InitKmeansJobSequenceFile(conf);

		// FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// FileInputFormat.addInputPath(job, new Path(args[0]));

		// debug localhost
		FileOutputFormat.setOutputPath(job, new Path(
				"/home/training/workspace/FinalProj/output/Canopy"));
		FileInputFormat.addInputPath(job, new Path(
				"/home/training/workspace/FinalProj/input"));

		// print the counter
		// System.out.println("The amount of time we stept into the Reducer: " +
		// job.getCounters().findCounter(MyCounters.Counter).getValue());
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private static void ReadingUserConfigFile() {
		try {
			Path pt = Globals.UserConfigFilePath();
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));
			String line;
			line = br.readLine();

			// spilits parameter
			String filedType;
			String val = "";

			while (line != null) {
				System.out.println(line);
				line = br.readLine();

			}

			String[] values = line.split(" ");

			// TODO : LEE reflecation
			Globals.setKmeansCount(Integer.parseInt(values[1]));
			Globals.setDaysNumber(Integer.parseInt(values[3]));
			Globals.setFeaturesNumber(Integer.parseInt(values[3]));

		} catch (Exception e) {
		}
	}

	private static void InitKmeansJobSequenceFile(Configuration conf)
			throws IOException {

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

		writer = SequenceFile.createWriter(conf,
				Writer.file(Globals.KmeansCenterPath()),
				Writer.keyClass(canopyCenter.class),
				Writer.valueClass(KMeansCenter.class));

		for (canopyCenter canopyCenter : canopyCentres) {

			int kmeansNumber = calcKmeansForCanopy();

			for (int i = 0; i < kmeansNumber; i++) {

				KMeansCenter randomKmeans = GetRendomKmeanCenterByCanapoy();

				writer.append(canopyCenter, randomKmeans);
			}
		}
	}

	private static int calcKmeansForCanopy() {
		// TODO Auto-generated method stub
		return 0;
	}

	private static KMeansCenter GetRendomKmeanCenterByCanapoy(int N, double R ) {
		

		KMeansCenter randomKmeans = new KMeansCenter();
		

	 	// create the 2D array
	 	DoubleWritable[][] tmp2DArray = new DoubleWritable[N][Globals.featuresNumber];
		
		for (int currFeature = 0; currFeature < Globals.featuresNumber; currFeature++) {
			
			// generate N variables from Gaussian distribution
			for (int day = 0; day < N; day++)
				tmp2DArray[day][currFeature] = 
					new DoubleWritable(StdRandom.gaussian());
			
	
			// compute Euclidean norm of vector x[]
			for (int day = 0; day < N; day++)
			{
				r = r + tmp2DArray[day][currFeature] * tmp2DArray[day][currFeature];
			}
			r = Math.sqrt(r);
		}
		

		// print scaled vector
		for (int i = 0; i < N; i++)
			System.out.println(x[i] / r);
	}
}
