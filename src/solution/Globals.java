package solution;

import org.apache.hadoop.fs.Path;

public class Globals {
	static int kmeansCount = 0 ;
	static int daysNumber = 0 ;
	static int featuresNumber  =  0;
	
	public static double T1() {
		return 0.5;
	}
	
	public static double T2() {
		return 0;
	}
	
	public static Path CanopyCenterPath()
	{
		return new Path("/home/training/workspace/FinalProj/data/SequenceFile.canopyCenters");
	}
	
	public static Path KmeansCenterPath()
	{
		return new Path("/home/training/workspace/FinalProj/data/SequenceFile.kmeansCenters");
	}
	
	public static Path UserConfigFilePath()
	{
		return new Path("");
	}
	
	public static int getKmeansCount()
	{
		return kmeansCount;
	}
	
	public static void setKmeansCount(int count)
	{
		 kmeansCount = count;
	}
	
	public static int getDaysNumber()
	{
		return daysNumber;
	}
	
	public static void setDaysNumber(int count)
	{
		daysNumber = count;
	}
	
	
	public static int getFeaturesNumber()
	{
		return featuresNumber ;
	}
	
	public static void setFeaturesNumber(int count)
	{
		featuresNumber  = count;
	}
	
}
