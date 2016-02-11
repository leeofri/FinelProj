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
import solution.StdRandom;

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
		ReadingUserConfigFile(args);

		// Canopy
		Job job = Job.getInstance(conf, "FinelProj.Canopy");
		job.setJarByClass(FinalProj.class);
		job.setMapperClass(canopyMapper.class);
		job.setReducerClass(canopyReducer.class);
		job.setOutputKeyClass(canopyCenter.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(canopyCenter.class);

		FileOutputFormat.setOutputPath(job, Globals.OutputFolderCanopy());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		// delete the privies run output
	    IsDeleteUtputFolder(false);
		
	    // run canopy
		job.waitForCompletion(true);

		// Adding the canopy centers and the kmeans centres to the cache
		// kmeans get the canopy centers from SequenceFile
		Hashtable<String,KMeansCenter> oldKmeansCenters = null;
		Hashtable<String,KMeansCenter> newKmeansCenters = InitKmeansJobSequenceFile(new Configuration());
		
	
		int counter = 1;
		do{
			// job config and run
			kmeansJobConf(args,0,counter);
			
			
			// get new centers
			oldKmeansCenters = newKmeansCenters;
			newKmeansCenters = Util.getKmeansCenterFromFile(Globals.KmeansCenterPath(), new Configuration());
			
			// debug
			System.out.println("Main - old center: " + oldKmeansCenters.toString());
			System.out.println("Main - new center: " + newKmeansCenters.toString());
			System.out.println("Main - Run No:"+ counter +" |Num of kmeans in file:" + Util.numberOfRowsInSeqFile(Globals.KmeansCenterPath(), new Configuration()) + " Same centers:" + Util.comperKMeansCenter(oldKmeansCenters, newKmeansCenters));
			
			counter++;
		} while (Util.comperKMeansCenter(oldKmeansCenters, newKmeansCenters));

		// Last run for write output
		kmeansJobConf(args,1,0);

	}

	private static Boolean kmeansJobConf (String[] args, Integer isLastRun,int iteration) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration Kmeansconf = new Configuration();
		Kmeansconf.set("num.lastrun",isLastRun.toString());
		Util.writeFileToHDFS(Kmeansconf, "./finalrun/data/isLastRunTrigger", isLastRun.toString(),true);
	
		
		Job job = new Job(Kmeansconf, "FinelProj.KMeans"+iteration);

		// the kmeans config
		job.setJarByClass(FinalProj.class);
		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(KMeansCenter.class);
		job.setMapOutputValueClass(StockWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, Globals.OutputFolderKmeans(iteration));
		
		return job.waitForCompletion(true);
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

	private static void ReadingUserConfigFile(String[] arg) throws IOException {
		try {
			Path pt = Globals.UserConfigFilePath();
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));
			String line;
			line = br.readLine();

			String[] values = line.split(" ");

			Globals.setKmeansCount(Integer.parseInt(values[1]));
			Globals.setDaysNumber(Integer.parseInt(values[3]));
			Globals.setFeaturesNumber(Integer.parseInt(values[5]));
			
			// set the basic output path
			Globals.setOutputFolder(arg[1]);
			
			// spilits parameter
			String filedType;
			String val = "";

			while (line != null) {
				line = br.readLine();
			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new IOException(e);

		}
	}

	private static Hashtable<String, KMeansCenter> InitKmeansJobSequenceFile(
			Configuration conf) throws Exception {

		Hashtable<String, KMeansCenter> randomKmeansCenters = new Hashtable<String, KMeansCenter>();
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
			val = new canopyCenter();
		}

		// Create the connection
		Writer writer = null;

		try {
			writer = SequenceFile.createWriter(conf,
					Writer.file(Globals.KmeansCenterPath()),
					Writer.keyClass(Text.class),
					Writer.valueClass(KMeansCenter.class));
		} catch (Exception e) {
			throw new IOException(e);
		}

		for (canopyCenter canopyCenter : canopyCentres) {

			int kmeansNumber = calcKmeansForCanopy(
					canopyCenter.getClusterSize(), stockCount);

			for (int i = 0; i < kmeansNumber; i++) {

				KMeansCenter randomKmeans = Util.GetRendomKmeanCenterByCanapoy(
						Globals.daysNumber, Globals.getFeaturesNumber(),
						Globals.T1(), canopyCenter);

				// Giving name for the kmeans - this name used by the kmeans
				// mapper
				randomKmeans.getCenter().setName(String.valueOf(i));

				randomKmeans.setRealatedCanopyCenter(canopyCenter);

				writer.append(canopyCenter.get().getName(), randomKmeans);

				// add to the returned list for comper in main
				randomKmeansCenters.put(randomKmeans.getRealatedCanopyCenter()
						.get().getName()
						+ randomKmeans.getCenter().getName().toString(),
						randomKmeans);
			}
		}

		// close the writer
		writer.close();

		// add the SequenceFile to the global
//		try {
//
//			DistributedCache.addCacheFile(new URI(Globals.KmeansCenterPath()
//					.toString()), conf);
//			// DistributedCache.addLocalFiles(conf,Globals.KmeansCenterPath().toString());
//		} catch (Exception e) {
//			System.out
//					.println("ERROR - InitKmeansJobSequenceFile - problam with adding the kmeans seq file: "
//							+ Globals.KmeansCenterPath().toUri());
//			throw e;
//		}

		return randomKmeansCenters;

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
			} else {
				usedKmeans = (int) Math.round(Globals.kmeansCount * propotion);

				if (usedKmeans == Globals.kmeansCount) {
					usedKmeans = Globals.kmeansCount - 1;
				}

				return usedKmeans;
			}
		} else {
			int temp = usedKmeans;
			usedKmeans = 0;
			return Globals.kmeansCount - temp;
		}
	}
}
