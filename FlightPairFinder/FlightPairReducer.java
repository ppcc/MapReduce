import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightPairReducer extends
		Reducer<ImmeKey, DoubleWritable, DoubleWritable,IntWritable> {

	private DoubleWritable result = new DoubleWritable();
	private IntWritable num = new IntWritable();

	@Override
	public void reduce(ImmeKey key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		
		double frtDelay = 0.0;
		int sndDelay = 0;
		int count = 0;
		int sumDelay = 0;
		String flightDate = "";
		String curFlight;
		for(DoubleWritable val : values){
			curFlight = key.getFlightdate().toString();
			if(!curFlight.equals(flightDate)){
				//Flightdate update, reset related variables
				flightDate = curFlight;
				sumDelay = 0;
				count = 0;
			}
			if(key.getTag().toString().equals("B")){
				//Flight is Second leg
				//update sum and count
				sumDelay += val.get();
				count ++;
			}else{
				if(count != 0){
					// There are second flight after this flight, then we can make pair
					// number of pair = count
					// sum of delay = count*firstdelay + sum of seconddelay
					frtDelay = val.get();
					sndDelay = sumDelay;
					result.set(frtDelay*count+sndDelay);
					num.set(count);
					context.write(result,num);
				}
			}
		}
		
	}

}
