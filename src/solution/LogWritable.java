package solution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class LogWritable implements WritableComparable<LogWritable> {

	private IntWritable index;
	private Text user, url, date;

	public LogWritable() {
		this.index = new IntWritable();
		this.url = new Text();
		this.user = new Text();
		this.date = new Text();
	}

	public LogWritable(Text user, Text url, Text date, IntWritable index) {
		this.index = index;
		this.url = url;
		this.user = user;
		this.date = date;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		user.readFields(in);
		url.readFields(in);
		date.readFields(in);
		index.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		user.write(out);
		url.write(out);
		date.write(out);
		index.write(out);
	}

	@Override
	public int compareTo(LogWritable o) {
		 if (this.user.compareTo(o.user) == 0) {
            return this.GetDomain().compareTo(o.GetDomain());
        } else
            return this.user.compareTo(o.user);
	}
	
	public String GetDomain()
	{
		String domin = this.url.toString();
		domin = org.apache.commons.lang.StringUtils.substringAfter(domin, "//").split("/")[0];
		return (domin);
	}

	public String GetKey()
	{
		return (this.GetDomain() + this.user.toString());
	}
}


