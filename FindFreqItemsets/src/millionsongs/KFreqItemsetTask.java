package millionsongs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.jasper.runtime.ProtectedFunctionMapper;

import com.sun.corba.se.impl.encoding.OSFCodeSetRegistry.Entry;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generate k frequent itemsets from k-itemset candidates files.
 * Created by cpp on 12/01/14.
 */
public class KFreqItemsetTask {
	
    //
    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text itemset;
        private IntWritable times;
        private List<List<Integer>> kItemCandidates;
        private List<String> candidateStrings;
        private int count = 0;
        private Map<String,Integer> counter;
        
        private int k;

        protected void setup(Context context) throws IOException{
            itemset = new Text();
            times = new IntWritable();
        	kItemCandidates = loadKItemCandidates(context);
            prepareCandidates();
            k = Integer.parseInt(context.getConfiguration().get("K"));
            counter = new HashMap<String, Integer>();
        }
        
        private void prepareCandidates(){
        	candidateStrings = new ArrayList<String>();
        	StringBuilder builder = new StringBuilder();
        	String s;
        	for(List<Integer> candidate:kItemCandidates){
        		builder.setLength(0);
        		for(int id:candidate){
        			s = String.valueOf(id);
        			builder.append(String.format("%0"+(7-s.length()+"d%s"),0, s));
        			builder.append("\t");
        		}
        		builder.setLength(builder.length()-1);
        		candidateStrings.add(builder.toString());
        	}
        }
        
        private boolean doesContains(List<Integer> bigList, List<Integer> smallList){
        	if(smallList.size() > bigList.size())
        		return false;
        	
        	int bIdx = 0;
        	int sIdx = 0;
        	int bigLength = bigList.size();
        	int smallLength = smallList.size();
        	
        	//Preprocess
        	int startIndex = bigList.indexOf(smallList.get(0));
        	
        	if(startIndex == -1 || (bigLength - startIndex) < smallLength)
        		return false;
        	
        	bIdx = startIndex;
        	
        	while(bIdx < bigLength && sIdx < smallLength){
        		if(bigList.get(bIdx).equals(smallList.get(sIdx))){
        			sIdx ++;
        			if(sIdx == smallLength)
        				return true;
        			bIdx = bigList.indexOf(smallList.get(sIdx));
        			if(bIdx == -1)
        				return false;
        		}else if(bigList.get(bIdx) > smallList.get(sIdx)){
        			return false;
        		}else{
        			bIdx ++;
        		}
        	}
        	return false;
        }

        public void map(Object key, Text value, Context context){
        	count ++;
        	if(count % 1000 == 0)
        		System.out.println("Transaction:" + count + " time:"+new Date());
        	// Store items into List<Integer>
            String[] items = value.toString().split("\t");
            if(items.length < k)
            	return;
//            System.out.print("transaction:"+value.toString());
            List<Integer> transactions = new ArrayList<Integer>();
            boolean first = true;
            for(String item : items){
            	if(first){
            		first = false;
            		continue;
            	}
            	transactions.add(Integer.parseInt(item));
            }
            int size = transactions.size();
            //Go through all candidates, if transaction contains candidate, emit candidate
            List<Integer> candidate;
            int candSize  = kItemCandidates.size();
            int count;
            String str;
//            List<Integer> test = Arrays.asList(6127,316817);
            for(int i=0;i<kItemCandidates.size();i++){
//            	if(i%1000 == 0)
//            		System.out.format("Progress:%d/%d\n", i,candSize);
            	candidate = kItemCandidates.get(i);
            	if(candidate.get(0) > transactions.get(size-1))
            		break;
            	
//            	if(candidate.containsAll(test) && transactions.containsAll(test)){
//            		System.out.println("Hhaha");
//            	}
//            	if(doesContains(transactions,candidate)){
            	if(transactions.containsAll(candidate)){
            		str = candidateStrings.get(i);
            		if(counter.containsKey(str)){
            			count = counter.get(str)+1;
            		}else{
            			count = 1;
            		}
            		counter.put(str, count);
            		
            	}
            }
            
            
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException{
        	for(Map.Entry<String, Integer> entry:counter.entrySet()){
        		itemset.set(entry.getKey());
        		times.set(entry.getValue());
        		context.write(itemset, times);
        	}
        	
        	candidateStrings.clear();
        	counter.clear();
        	kItemCandidates.clear();
        }
        
        private List<List<Integer>> loadKItemCandidates(Context context) throws IOException{
    		List<List<Integer>> kItemCandidates = new ArrayList<List<Integer>>();
    		
    		Path[] uris = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    		File folder = new File(uris[0].toString());
    		File[] files = folder.listFiles();
    		for(File file : files){
    			if(file.getName().startsWith("."))
    				continue;
    			BufferedReader reader = new BufferedReader(new FileReader(file));
        		String line;
        		while((line = reader.readLine()) != null){
        			List<Integer> candidate = new ArrayList<Integer>();
        			String[] items = line.split("\t");
        			for(String item: items){
        				candidate.add(Integer.parseInt(item));
        			}
        			kItemCandidates.add(candidate);
        		}
        		reader.close();
    		}
    		
    		return kItemCandidates;
    	}
    }

    public static class MyReducer extends Reducer<Text,IntWritable,Text,NullWritable> {
        private IntWritable output;
        private int minSupport;
        
        protected void setup(Context context) throws IOException{
            output = new IntWritable();
            minSupport = Integer.parseInt(context.getConfiguration().get("minSupport"));
        }
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int count = 0;
        	for(IntWritable value:values){
        		count += value.get();
        	}
        	
        	System.out.println(key.toString()+" count:"+count);
        	if(count >= minSupport){
        		output.set(count);
            	context.write(key, NullWritable.get());
            	context.getCounter("Reduce", "Output").increment(1);
        	}
        }
    }
    
    public static class MyCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable output;
        private int minSupport;
        
        protected void setup(Context context) throws IOException{
            output = new IntWritable();
            minSupport = Integer.parseInt(context.getConfiguration().get("minSupport"));
        }
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int count = 0;
        	for(IntWritable value:values){
        		count += value.get();
        	}
        	output.set(count);
           	context.write(key, output);
        }
    }
    
    public static long run(Configuration conf, int k, String transactionsLoc, String location, String minSupport) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException{
    	String datapath = location + "Candidates" +k;
    	String out = location +"FreqItems"+k;
    	String[] args = {transactionsLoc,out};
    	return run(conf, args,datapath,minSupport);
    }
    
    public static long run(Configuration conf,String[] args, String datapath, String minSupport) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: K-Freq-Itemsets <transactions> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "K-Freq-Itemsets");
        job.setJarByClass(KFreqItemsetTask.class);
        
        job.getConfiguration().set("minSupport", minSupport);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
//        job.setCombinerClass(MyCombiner.class);
        

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        DistributedCache.addCacheFile(new URI(datapath), job.getConfiguration());

        boolean code = job.waitForCompletion(true);
        
        if(!code)
        	return -1;
        
        long number = job.getCounters().getGroup("Reduce").findCounter("Output").getValue();
        return number;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        String datapath = "/Users/peilicao/WorkSpace/MillionSong/Candidates3";
        Configuration conf = new Configuration();
        long code = run(conf,args,datapath,"3");
        System.exit(code == -1 ? 0 : 1);
    }
}
