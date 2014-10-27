import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Join in MapReduce
 * 
 * This project is to compute the average delay for all two-leg flights from
 * airport ORD(Chicago) to JFK(New York) where both legs have a flight data that
 * falls into between June 2007 and May 2008. The two flight must have same
 * flightdate, and the departure time of F2 must be later than the arriva time
 * of F1
 * <p>
 * There are two jobs:<br>
 * First job: 
 * Map Output Key: <b>(city, flightdate, time, tag)</b><br>
 * Map Output Value: <b>delayMins</b><br>
 * Reduce Output Key: <b>pairDelayMin</b><br>
 * Reduce Output Value: <b>count</b><br>
 * <p>
 * Second job: 
 * Map Output Key: <b>"dummy"</b><br>
 * Map Output Value: <b>(pairDelayMin,count)</b><br>
 * Reduce Output Key: <b>numTotalPairs</b><br>
 * Reduce Output Value: <b>avgDelayMin</b><br>
 * @author Peili Cao
 *
 */
public class FlightPairFinder {

	/**
	 * Used for second job to map all (delays,num of pair) to same reduce call
	 */
	public static class DummyMapper extends
			Mapper<Object, Text, Text, ImmeCountValue> {

		private ImmeCountValue val;
		private DoubleWritable pairDelayMin;
		private IntWritable count;
		private Text dummykey;

		protected void setup(Context context) {
			val = new ImmeCountValue();
			pairDelayMin = new DoubleWritable();
			count = new IntWritable();
			//Make sure all records will go to same reduce call
			dummykey = new Text("dummy");
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				pairDelayMin.set(Double.parseDouble(itr.nextToken()));
				count.set(Integer.parseInt(itr.nextToken()));
				val.set(pairDelayMin, count);
				context.write(dummykey, val);
			}
		}
	}

	/**
	 * Calculate average delay
	 */
	public static class AvgDelayReducer extends
			Reducer<Text, ImmeCountValue, IntWritable, DoubleWritable> {

		private IntWritable totalNum = new IntWritable();
		private DoubleWritable avgDelays = new DoubleWritable();

		public void reduce(Text key, Iterable<ImmeCountValue> values,
				Context context) throws IOException, InterruptedException {
			double pairDelayMin = 0.0;
			int count = 0;
			for (ImmeCountValue val : values) {
				pairDelayMin += val.getSum().get();
				count += val.getNum().get();
			}

			totalNum.set(count);
			avgDelays.set(pairDelayMin / (count * 1.0));
			context.write(totalNum, avgDelays);
		}
	}

	/**
	 * Secondary Sort Field Sorting <p>
	 * Order: Flightdate ASC, time DESC, Tag ASC
	 * 
	 * @author Peili Cao
	 *
	 */
	public static class Comparator extends WritableComparator {

		protected Comparator() {

			super(ImmeKey.class, true);
		}

		@Override
		public int compare(WritableComparable one, WritableComparable two) {

			ImmeKey k1 = (ImmeKey) one;
			ImmeKey k2 = (ImmeKey) two;

			int result = k1.getIntercity().compareTo(k2.getIntercity());
			if (0 == result) {
				result = k1.getFlightdate().compareTo(k2.getFlightdate());
				if (0 == result) {
					result = -1 * k1.getTime().compareTo(k2.getTime());
					if (0 == result)
						result = k1.getTag().compareTo(k2.getTag());
				}
			}
			return result;
		}

	}

	/**
	 * Group Comparator Only consider City field so that Same city go to same
	 * reduce call
	 * 
	 * @author Peili Cao
	 *
	 */
	public static class CustomGroupComparator extends WritableComparator {

		protected CustomGroupComparator() {

			super(ImmeKey.class, true);
		}

		@Override
		public int compare(WritableComparable one, WritableComparable two) {

			ImmeKey k1 = (ImmeKey) one;
			ImmeKey k2 = (ImmeKey) two;
			int result = k1.getIntercity().compareTo(k2.getIntercity());
			return result;
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: flightfinder <in> <imme> <out>");
			System.exit(2);
		}
		
		//First job
		Job job = new Job(conf, "Flight Pairs Find");
		job.setJarByClass(FlightPairFinder.class);
		job.setMapperClass(FlightPairMapper.class);
		job.setReducerClass(FlightPairReducer.class);
		job.setGroupingComparatorClass(CustomGroupComparator.class);
		job.setSortComparatorClass(Comparator.class);
		job.setMapOutputKeyClass(ImmeKey.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(10);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.waitForCompletion(true);

		//Second Job
		Job job2 = new Job(conf, "Avg Delay Count");
		job2.setJarByClass(FlightPairFinder.class);
		job2.setMapperClass(DummyMapper.class);
		job2.setReducerClass(AvgDelayReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(ImmeCountValue.class);
		job2.setNumReduceTasks(1);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}

}
