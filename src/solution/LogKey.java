package solution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class LogKey implements Writable {
	
	// DM
   public Text user,domin;
	
   public LogKey() {
	this.domin = new Text();
	this.user = new Text();
   }
   
   public LogKey(Text usr,Text Domia,int enters) {
	this.domin = new Text(Domia);
	this.user = new Text(usr);
   }
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	

}
