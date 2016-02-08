package solution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;
import org.mockito.internal.stubbing.answers.Returns;

public class KMeansCenter implements WritableComparable<KMeansCenter> {

	private StockWritable center;
	private canopyCenter realatedCanopyCenter;

	public KMeansCenter() {
		super();
		this.center = null;
		this.realatedCanopyCenter = null;
	}

	public KMeansCenter(KMeansCenter center) {
		super();
		this.center = new StockWritable(center.center);
		this.realatedCanopyCenter = center.getRealatedCanopyCenter();
	}

	public KMeansCenter(StockWritable center) {
		super();
		this.center = center;
		this.realatedCanopyCenter = null;
	}
	
	public canopyCenter getRealatedCanopyCenter(){
		return this.realatedCanopyCenter;
	}
	
	public void setRealatedCanopyCenter(canopyCenter c){
		this.realatedCanopyCenter = c;
	}
	
	public boolean converged(KMeansCenter c) {
		return compareTo(c) == 0 ? false : true;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		center.write(out);
		this.realatedCanopyCenter.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.center = new StockWritable();
		center.readFields(in);
		this.realatedCanopyCenter.readFields(in);
	}

	@Override
	public int compareTo(KMeansCenter o) {
		
		// comper the centers
		int result = center.compareTo(o.getCenter());
		
		//check if ther is a canopycenter to the object
		if (this.getRealatedCanopyCenter() != null && o.getRealatedCanopyCenter() != null)
		{
			result =+ this.getRealatedCanopyCenter().compareTo(o.getRealatedCanopyCenter());
		}
		
		
		return result;
	}

	/**
	 * @return the center
	 */
	public StockWritable getCenter() {
		return center;
	}

	@Override
	public String toString() {
		return "ClusterCenter [center=" + center + "]";
	}

}
