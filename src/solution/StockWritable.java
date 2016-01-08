package solution;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class StockWritable extends Configured implements Writable,Comparable<StockWritable> {

	// DM
  private Text stackName;
  private TwoDArrayWritable stock;
  
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
 
  
//  public StockWritable(String name,int n,int m)
//  {
//	  this.stackName = new Text(name);
//	  
//	  // create the stack vector
//	  this.stock = new TwoDArrayWritable(DoubleWritable.class);
//	  DoubleWritable[][] v = new DoubleWritable[n][m];
//	  this.stock.set(v);
//  }
  public StockWritable(DoubleWritable[][] v, Text name)
  {
	  this.stackName = new Text(name);
	  
	  // create the stack vector
	  this.set(v);
  }
  
  public int getDaysNumber()
  {
	  return (this.stock.get().length);
  }
  
  public int getDataTypsNumber()
  {
	  return this.stock.get()[0].length;
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

  public void set(DoubleWritable[][] Stock) {
	  this.stock = new TwoDArrayWritable(DoubleWritable.class);
	  this.stock.set(Stock);
  }
  public StockWritable() {
  }

  public StockWritable(TwoDArrayWritable v, Text name) {
	  this.stackName = name;
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
	public int compareTo(StockWritable o) {
		// TODO: compare by stack name
		return 0;
	}
	
	public int distance(StockWritable o){
		// calc the distance between all the four parameter of each day
		int distance = 0;
		
		for (int day = 0; day < this.getDaysNumber(); day++) {
			for (int parameter = 0; parameter < this.getDataTypsNumber(); parameter++) {
				distance += Math.abs(((DoubleWritable)this.stock.get()[day][parameter]).get() - ((DoubleWritable)o.stock.get()[day][parameter]).get() );
			}
		}
		
		return distance;
	}
}

