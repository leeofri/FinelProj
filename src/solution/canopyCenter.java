package solution;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class canopyCenter  implements Writable,Comparable<canopyCenter>{
	

  private IntWritable pointsInCluster;
  private StockWritable clusterCenter;
  
  public canopyCenter() {
	  canopyCenter(1,new StockWritable());
  }
  
  private void canopyCenter(int amuntOfPoints, StockWritable center)
  {
	  
	  this.pointsInCluster = new IntWritable(amuntOfPoints);
	  
	  // create the stack vector
	  this.clusterCenter = new StockWritable(center);
  }

public canopyCenter(int amuntOfPoints, StockWritable center)
  {
	canopyCenter(amuntOfPoints,center);
  }
  
  public canopyCenter(StockWritable center)
  {
	  this.pointsInCluster = new IntWritable(1);
	  
	  // create the stack vector
	  this.clusterCenter = new StockWritable(center);
  }
  
  public StockWritable get() {
	    return this.clusterCenter;
	  }
  
  
  public int getClusterSize ()
  {
	  return this.pointsInCluster.get();
  }
  
  public void increasCounter(int numToAdd)
  {
	  this.pointsInCluster = new IntWritable(this.pointsInCluster.get() + numToAdd);
  }


  @Override
  public void write(DataOutput out) throws IOException 
  {   
	  // write the name of the stack
	  this.pointsInCluster.write(out);
	  
	  // write all the vector 
      this.clusterCenter.write(out);  
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    try {
  	  // read the name of the stack
  	  this.pointsInCluster.readFields(in);
  	  
  	  // read all the vector 
	  this.clusterCenter.readFields(in);
	
    } catch (ClassCastException cce) {
      throw new IOException(cce);
    }
  }

@Override
public int compareTo(canopyCenter o) {
	return this.clusterCenter.compareTo(o.get());
}
}



