import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Customized ImmediateKey<br>
 * <b>(airline,month)</b>
 * @author Peili Cao
 *
 */
public class ImmeKey implements WritableComparable<ImmeKey> {
			private IntWritable airline;
			private IntWritable month;
	
			public IntWritable getAirline() {
				return airline;
			}

			public void setAirline(IntWritable airline) {
				this.airline = airline;
			}

			public IntWritable getMonth() {
				return month;
			}

			public void setMonth(IntWritable month) {
				this.month = month;
			}

			public void set(IntWritable id,IntWritable m){
				this.airline = id;
				this.month = m;
			}
	
			@Override
			public void readFields(DataInput in) throws IOException {
				if(airline == null){
					airline = new IntWritable();
				}
				
				if(month == null){
					month = new IntWritable();
				}
				
				airline.readFields(in);
				month.readFields(in);
				
			}
	
			@Override
			public void write(DataOutput out) throws IOException {
				airline.write(out);
				month.write(out);
			}
			
			@Override
			public String toString(){
				return airline+" "+month;
			}
	
			@Override
			public int compareTo(ImmeKey moo) {
				int cmp = airline.compareTo(moo.getAirline());
				if(cmp != 0)
					return cmp;
				else{
					return month.compareTo(moo.getMonth());
				}
			}
			
			@Override
			public int hashCode(){
				int result = airline.hashCode()*163+month.hashCode();
				return result;
			}
			
			
			@Override
			public boolean equals(Object o){
				if(o instanceof ImmeKey){
					ImmeKey moo = (ImmeKey) o;
					return airline.equals(moo.getAirline())
							&& month.equals(moo.getMonth());
				}
				
				return false;
			}
			
			
			
			
}
