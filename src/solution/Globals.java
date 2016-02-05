package solution;

import org.apache.hadoop.fs.Path;

public class Globals {
	static int kmeansCount = 0 ;
	static int daysNumber = 0 ;
	static int featuresNumber  =  0;
	
	public static double T1() {
		return 0.7;
	}
	
	public static double T2() {
		return 0;
	}
	
	public static Path CanopyCenterPath()
	{
		return new Path("./data/SequenceFile.canopyCenters");
	}
	
	public static Path InputFolder()
	{
		return new Path("./input/input");
	}
	
	public static Path OutputFolder()
	{
		return new Path("./output/Canopy");
	}
	
	public static Path KmeansCenterPath()
	{
		return new Path("./data/SequenceFile.kmeansCenters");
	}
	
	public static Path UserConfigFilePath()
	{
		return new Path("./data/userConfigFile.config");
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
