package solution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

public class KMeansCenter implements WritableComparable<KMeansCenter> {

	private StockWritable center;

	public KMeansCenter() {
		super();
		this.center = null;
	}

	public KMeansCenter(KMeansCenter center) {
		super();
		this.center = new StockWritable(center.center);
	}

	public KMeansCenter(StockWritable center) {
		super();
		this.center = center;
	}
	
	public boolean converged(KMeansCenter c) {
		return compareTo(c) == 0 ? false : true;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		center.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.center = new StockWritable();
		center.readFields(in);
	}

	@Override
	public int compareTo(KMeansCenter o) {
		return center.compareTo(o.getCenter());
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
