import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightReducer extends
		Reducer<ImmeKey, DoubleWritable, IntWritable,Text> {

	private IntWritable result = new IntWritable();
	private Text pairs = new Text();
	StringBuilder builder = new StringBuilder("");

	@Override
	public void reduce(ImmeKey key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		
		double arrDelayMins = 0.0;
		double sumDelayMins = 0.0;
		int month = 1;
		int preMon = 0;
		int currentMon = 0;
		int count = 0;
		int avgDelayMins = 0;
		//reset builder
		builder.setLength(0);
		
		for(DoubleWritable val : values){
			
			month  = key.getMonth().get();
			arrDelayMins = val.get();
			
			if(currentMon == month){
				//Same month with previous (key,value) pair
				//update sumDelayMins and count
				sumDelayMins += arrDelayMins;
				count += 1;
			}else{	
				if(count >0){
					//Has records needed to emit
					avgDelayMins = (int)Math.ceil(sumDelayMins/count);
					buildValidOutput(currentMon,avgDelayMins);
				}
				
				//No flights in some months
				while(month > (preMon+1))
					buildNullOutput(++preMon);
				
				//Start to count a new month
				currentMon = month;
				sumDelayMins = arrDelayMins;
				preMon = month;
				count =1;
			}
				
		}
		
		if(count >0){
			//Has records needed to emit
			avgDelayMins = (int)Math.ceil(sumDelayMins/count);
			buildValidOutput(currentMon,avgDelayMins);
		}
		preMon++;
		while(preMon <= 12){
			//no flight records in rest month
			buildNullOutput(preMon);
			preMon++;
		}
		
		//emit
		result.set(key.getAirline().get());
		pairs.set(builder.toString());
		context.write(result,pairs);
	}
	
	//Helper functions
	
	private void buildValidOutput(int month, int avg){
		// (month,avgDelay)
		this.buildOutput(month, Integer.toString(avg));
	}
	
	private void buildNullOutput(int month){
		// (month, NULL)
		this.buildOutput(month, "NULL");
	}
	
	private void buildOutput(int month,String avg){
		builder.append(",");
		builder.append("(");
		builder.append(month);
		builder.append(",");
		builder.append(avg);					
		builder.append(")");
	}
	
	

}
