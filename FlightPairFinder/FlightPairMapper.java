import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.com.bytecode.opencsv.CSVParser;


public class FlightPairMapper extends Mapper<Object, Text, ImmeKey, DoubleWritable>{
	
	private ImmeKey imkey;
	private Text flightdate;
	private Text intercity;
	private Text time;
	private Text tag;
	private DoubleWritable delay;
	private CSVParser parser;
	
	
	protected void setup(Context context){
		flightdate = new Text();
		intercity = new Text();
		time = new Text();
		tag = new Text();
		delay = new DoubleWritable();
		parser = new CSVParser();
		imkey = new ImmeKey();
	}
	
	
	public void map(Object key, Text value, Context context
	            ) throws IOException, InterruptedException {
		
			String[] lines = parser.parseLine(value.toString());
			String org = "";
			String dest = "";
			if(lines.length < 56)
				return;
			int month = 0;
			boolean time1;
			boolean time2;
			boolean flag = false;
			month = Integer.parseInt(lines[2]);
			
			time1 = lines[0].equals("2007") && (month>=6) && (month<=12);
			time2 = lines[0].equals("2008") && (month>=1) && (month<=5);
			
			//Flight during 2007.6 ~ 2008.5 and No cancelled no diverted
			if((time1 || time2) && lines[41].equals("0.00") && lines[43].equals("0.00")){
				org = lines[11];
				dest = lines[17];
				if(org.equals("ORD") && !dest.equals("JFK")){
					//First Leg
					
					/*
					 * Make sure when time is equal, 
					 * A(first leg) will come first before B(second leg)
					 */
					tag.set("A"); 
					//Set arrival time as time
					time.set(lines[35]);
					intercity.set(dest);
					flag = true;
				}else if(!org.equals("ORD") && dest.equals("JFK")){
					//Second Leg
					
					/*
					 * Make sure when time is equal, 
					 * A(first leg) will come first before B(second leg)
					 */
					tag.set("B");
					intercity.set(org);
					//Set departure time as time
					time.set(lines[24]);
					flag = true;
				}
				
				if(flag){
					flag = false;
					flightdate.set(lines[5]);
					imkey.set(intercity,flightdate,time,tag);
					delay.set(Double.parseDouble(lines[37]));
					context.write(imkey, delay);
				}
			}
	}

}
