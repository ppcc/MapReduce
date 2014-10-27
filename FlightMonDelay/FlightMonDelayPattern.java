
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This Project is to compute the pattern of monthly delays for each airline.<br>
 * Using secondary sort
 * <p>
 * Map Output Key: <b>(airline, month)</b><br>
 * Records are grouped solely by 'airline'<br>
 * Map Output Value: <b>delayMinutes</b><br>
 * <p>
 * Reduce Output Key: <b>airline</b><br>
 * Reduce Output Value:<b>(month, avgDelay)</b><br>
 * E.g.: (1,11),(2,12),(3,22),...,(12,NULL)
 * @author Peili Cao
 *
 */
public class FlightMonDelayPattern {

	/**
	 * Secondary Sort<br>
	 * Order: <br>
	 * ariline: ASC, month:ASC
	 */
	public static class Comparator extends WritableComparator {

		protected Comparator() {
			super(ImmeKey.class, true);
		}

		@Override
		public int compare(WritableComparable one, WritableComparable two) {

			ImmeKey k1 = (ImmeKey) one;
			ImmeKey k2 = (ImmeKey) two;

			int result = k1.getAirline().compareTo(k2.getAirline());
			if (0 == result) {
				result = k1.getMonth().compareTo(k2.getMonth());
			}
			return result;
		}

	}

	/**
	 * Group Comparator <p>
	 * Records with same airline will go to same reduce call
	 */
	public static class CustomGroupComparator extends WritableComparator {

		protected CustomGroupComparator() {
			super(ImmeKey.class, true);
		}

		@Override
		public int compare(WritableComparable one, WritableComparable two) {

			ImmeKey k1 = (ImmeKey) one;
			ImmeKey k2 = (ImmeKey) two;
			return  k1.getAirline().compareTo(k2.getAirline());
		}

	}

	/**
	 * Job Client Setting and running
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Flight <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Flights Month Delay Pattern");
		job.setJarByClass(FlightMonDelayPattern.class);
		
		job.setMapperClass(FlightMapper.class);
		job.setReducerClass(FlightReducer.class);
		//Set comparator
		job.setGroupingComparatorClass(CustomGroupComparator.class);
		job.setSortComparatorClass(Comparator.class);
		job.setMapOutputKeyClass(ImmeKey.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(10);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
