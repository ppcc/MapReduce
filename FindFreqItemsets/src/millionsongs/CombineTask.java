package millionsongs;

import millionsongs.objects.TagKey;
import millionsongs.objects.TupleKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * This is a MapReduce job aiming to emit transaction (User, song1, song2, song3 ... songn). Song ids are in asc order.
 * Created by peilicao on 11/9/14.
 */
public class CombineTask {
	/**
	 * This mapper will read the triplet file and emits (user,song) as output key, output value is null
	 * @author peilicao
	 *
	 */
    public static class MyMapper extends Mapper<Object, Text, TupleKey, NullWritable> {

        private IntWritable user;
        private IntWritable song;
        private TupleKey outKey;

        protected void setup(Context context){
            user = new IntWritable();
            song = new IntWritable();
            outKey = new TupleKey();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("\t");
            /**
             * Convert userid and songid to integer for easier ordered by reducer
             */
            user.set(Integer.parseInt(items[0]));
            song.set(Integer.parseInt(items[1]));
            outKey.set(user,song);
            context.write(outKey,NullWritable.get());
        }
    }

    /**
     * This reducer will read ((user,song),null) emitted by mapper and outputs transactions (User, song1, song2, song3 ... songn).
     * The order of input key is first ordered by userid asc, then for same userid, ordered by songid asc.
     * By doing this, we make sure that the output of reducer is in order in each line.
     * @author peilicao
     *
     */
    public static class MyReducer extends Reducer<TupleKey,NullWritable,NullWritable,Text> {
        private static final NullWritable nullKey = NullWritable.get();
        private Text out = new Text();
        private StringBuilder builder = new StringBuilder();

        @Override
        public void reduce(TupleKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            builder.setLength(0);
            builder.append(key.getFirst().get());

            for(NullWritable val : values){
                builder.append("\t");
                builder.append(key.getSecond().get());
            }

            out.set(builder.toString());
            context.write(nullKey,out);
        }
    }

    public static class CustomizeSortComparator extends  WritableComparator{
        protected CustomizeSortComparator() {
            super(TagKey.class,true);
        }

        @Override
        public int compare(WritableComparable oo1,WritableComparable oo2){
            TagKey tk1 = (TagKey) oo1;
            TagKey tk2 = (TagKey) oo2;
            return tk1.compareTo(tk2);
        }
    }

    /**
     * Partitioning records only consider userId.
     * @author peilicao
     *
     */
    public static class CustomizePartitioner extends Partitioner<TupleKey,NullWritable>{

        @Override
        public int getPartition(TupleKey tupleKey, NullWritable intWritable, int numPartitioner) {
            return tupleKey.getFirst().hashCode()*127 % numPartitioner;
        }
    }

    /**
     * Grouping records only consider userId.
     * @author peilicao
     *
     */
    public static class CustomizeGroupComparator extends  WritableComparator{
        protected CustomizeGroupComparator() {
            super(TupleKey.class,true);
        }

        @Override
        public int compare(WritableComparable oo1,WritableComparable oo2){
            TupleKey tk1 = (TupleKey) oo1;
            TupleKey tk2 = (TupleKey) oo2;
            return tk1.keyCompareTo(tk2);
        }
    }
    
    public static boolean run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: Combine <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Combine");
        job.setJarByClass(CombineTask.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setGroupingComparatorClass(CustomizeGroupComparator.class);
        job.setPartitionerClass(CustomizePartitioner.class);

        job.setMapOutputKeyClass(TupleKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(10);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        boolean code = run(args);
        System.exit(code ? 0 : 1);
    }
}
