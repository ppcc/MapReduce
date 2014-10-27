import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * ImmediateKey
 * <p>
 * Used by first job
 * @author Peili Cao
 *
 */
public class ImmeKey implements WritableComparable<ImmeKey> {
			private Text flightdate;
			private Text intercity;
			private Text time;
			private Text tag;
			
			public Text getFlightdate() {
				return flightdate;
			}
	
			public Text getIntercity() {
				return intercity;
			}
	
			public Text getTime() {
				return time;
			}
			
			public Text getTag(){
				return tag;
			}
	
			public void set(Text i,Text f,Text tm, Text tag){
				this.intercity = i;
				this.flightdate = f;
				this.time = tm;
				this.tag = tag;
			}
	
			@Override
			public void readFields(DataInput in) throws IOException {
				if(intercity == null){
					intercity = new Text();
				}
				
				if(flightdate == null){
					flightdate = new Text();
				}
				
				if(time == null){
					time = new Text();
				}
				
				if(tag == null)
					tag = new Text();
				intercity.readFields(in);
				flightdate.readFields(in);
				time.readFields(in);
				tag.readFields(in);
				
			}
	
			@Override
			public void write(DataOutput out) throws IOException {
				intercity.write(out);
				flightdate.write(out);
				time.write(out);
				tag.write(out);
				
			}
			
			@Override
			public String toString(){
				return intercity+" "+flightdate+" "+time+ " "+tag;
			}
	
			@Override
			public int compareTo(ImmeKey moo) {
				int cmp = intercity.compareTo(moo.getFlightdate());
				if(cmp != 0)
					return cmp;
				else{
					cmp = flightdate.compareTo(moo.getIntercity());
					if(cmp != 0)
						return cmp;
					else{
						cmp = time.compareTo(moo.getTime());
						if(cmp != 0)
							return cmp;
						else {
							return tag.compareTo(moo.getTag());
						}
						
					}
				}
			}
			
			@Override
			public int hashCode(){
				int result = intercity.hashCode()*163+flightdate.hashCode()*127+time.hashCode()*7+tag.hashCode();
				return result;
			}
			
			
			@Override
			public boolean equals(Object o){
				if(o instanceof ImmeKey){
					ImmeKey moo = (ImmeKey) o;
					return intercity.equals(moo.getFlightdate())
							&& flightdate.equals(moo.getIntercity())
							&& time.equals(moo.getTime())
							&& tag.equals(moo.getTag());
				}
				
				return false;
			}
			
			
			
			
}
