import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.com.bytecode.opencsv.CSVParser;

public class FlightMapper extends Mapper<Object, Text, ImmeKey, DoubleWritable> {

	private ImmeKey imkey;
	private IntWritable month;
	private IntWritable airline;
	private DoubleWritable arrDelayMins;
	private CSVParser parser;

	protected void setup(Context context) {
		imkey = new ImmeKey();
		month = new IntWritable();
		airline = new IntWritable();
		arrDelayMins = new DoubleWritable();
		parser = new CSVParser();
	}

	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] lines = parser.parseLine(value.toString());
		if (lines.length < 56)
			return;
		
		/**
		 * line[0]: year, line[2]: month line[7]:airline
		 * line[37]: arrDelayMins, line[41]:cancelled, 
		 * line[43]:diverted
		 */
		
		if (lines[0].equals("2008") && lines[41].equals("0.00")
				&& lines[43].equals("0.00")) {
			// Flight in 2008 and No cancelled no diverted
			month.set(Integer.parseInt(lines[2]));
			airline.set(Integer.parseInt(lines[7]));
			imkey.set(airline, month);
			arrDelayMins.set(Double.parseDouble(lines[37]));
			context.write(imkey, arrDelayMins);
		}
	}

}
