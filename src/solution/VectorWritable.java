package solution;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class VectorWritable extends Configured implements Writable,Comparable<VectorWritable> {

  private Text stackName;
  private TwoDArrayWritable stock;
  private int nRow;
  private int nCol;
  
  public enum type {
	  OPEN(0),
	  CLOSE(1),
	  HIGH(2),
	  LOW(3);
	  private final int value;
	  
	  private type(int value)
	  {
		  this.value = value;
	  }
  }
 
  
  public VectorWritable(String name,int n,int m)
  {
	  this.stackName = new Text(name);
	  this.nRow = n;
	  this.nCol = m;
	  
	  // create the stack vector
	  this.stock = new TwoDArrayWritable(DoubleWritable.class);
	  IntWritable[][] v = new IntWritable[n][m];
	  this.stock.set(v);
  }
  
  public int getRows()
  {
	  return (this.nRow);
  }
  
  public int getCols()
  {
	  return this.nCol;
  }
  
  private Text getName() {
	return this.stackName;

}
  
  public TwoDArrayWritable get() {
    return this.stock;
  }

  public void set(TwoDArrayWritable Stock) {
    this.stock = Stock;
  }

  public VectorWritable() {
  }

  public VectorWritable(TwoDArrayWritable v) {
    this.stock = v;
  }

  @Override
  public void write(DataOutput out) throws IOException 
  {   
	  // write the name of the stack
	  this.stackName.write(out);
	  
	  // write all the vector 
      this.stock.write(out);  
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    try {
  	  // read the name of the stack
  	  this.stackName.readFields(in);
  	  
  	  // read all the vector 
	  this.stock.readFields(in);
    } catch (ClassCastException cce) {
      throw new IOException(cce);
    }
  }

	@Override
	public int compareTo(VectorWritable o) {
		// TODO: compare by stack name
		return 0;
	}
	
	public int distance(VectorWritable o){
		// calc the distance between all the four parameter of each day
		int distance = 0;
		
		for (int day = 0; day < this.nRow; day++) {
			distance += Math.abs(((IntWritable)this.stock.get()[day][type.HIGH.value]).get() - ((IntWritable)o.stock.get()[day][type.HIGH.value]).get() );
			distance += Math.abs(((IntWritable)this.stock.get()[day][type.LOW.value]).get() - ((IntWritable)o.stock.get()[day][type.LOW.value]).get() );
			distance += Math.abs(((IntWritable)this.stock.get()[day][type.CLOSE.value]).get() - ((IntWritable)o.stock.get()[day][type.CLOSE.value]).get() );
			distance += Math.abs(((IntWritable)this.stock.get()[day][type.OPEN.value]).get() - ((IntWritable)o.stock.get()[day][type.OPEN.value]).get() );
		}
		
		return distance;
	}
}

