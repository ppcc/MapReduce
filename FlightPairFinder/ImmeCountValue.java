import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * ImmediateValue
 * <p>Used by second job
 * @author Peili Cao
 *
 */
public class ImmeCountValue implements WritableComparable<ImmeCountValue> {
	
	private DoubleWritable sum;
	private IntWritable num;
	
	public DoubleWritable getSum(){
		return this.sum;
	}
	
	public IntWritable getNum(){
		return this.num;
	}
	
	public void set(DoubleWritable s, IntWritable n){
		this.sum = s;
		this.num = n;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if(sum == null)
			sum = new DoubleWritable();
		if(num == null)
			num = new IntWritable();
		sum.readFields(in);
		num.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		sum.write(out);
		num.write(out);
	}

	@Override
	public int compareTo(ImmeCountValue icv) {
		int cmp = this.sum.compareTo(icv.getSum());
		if(cmp != 0)
			return cmp;
		else
			return this.num.compareTo(icv.getNum());
	}
	
	@Override
	public String toString(){
		return sum+" "+num;
	}
	
	@Override
	public int hashCode(){
		return this.sum.hashCode()*167+this.num.hashCode();
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof ImmeCountValue){
			ImmeCountValue icv = (ImmeCountValue) o;
			return sum.equals(icv.getSum())
					&& num.equals(icv.getNum());
		}
		return false;
		
	}

}
