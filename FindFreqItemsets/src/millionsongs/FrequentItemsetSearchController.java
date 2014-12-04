package millionsongs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import millionsongs.prepare.OneFreqItemTask;
import millionsongs.prepare.SongCountTask;

public class FrequentItemsetSearchController {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		if (args.length != 3) {
            System.err.println("Usage: FrequentItemSetSearch <input> <location> <minSupport>");
            System.exit(2);
        }

		CandidatesGenerator candidateGenerator = new CandidatesGenerator();
		FileReadWriteUtil fileUtils = new FileReadWriteUtil();

		String location = args[1];
		String transactionLoc = args[0]+"/transactions";
		String itemFreqLoc = args[0]+"/songfreq";
		String oneFreqItemLoc = location+"FreqItems1";
		List<List<Integer>> candidates;
		
		conf.set("minSupport", args[2]);

		//Generating Transactions
		String[] localArgs = {args[0],transactionLoc};
//		generateTransactions(localArgs);

		//Count item freq
		localArgs[0] = transactionLoc;
		localArgs[1] = itemFreqLoc;
//		countAllItemsFreq(localArgs);
		
		//Generate OneFreqItem
		generateOneFreqItems(itemFreqLoc,oneFreqItemLoc,args[2]);

		//Generate Candidates2
		//Loop

		List<String> contents;
		int k = 2;
		while(true){
			System.out.println("Finding "+k+" Itemset");
			if(k==2){
				candidates = candidateGenerator.generateCandidates2(location+"FreqItems1",conf);
			}else{
				//Generate Candidates
				candidates = candidateGenerator.generateCandidates(location+"FreqItems"+(k-1),conf);
			}

			if(candidates.isEmpty()) {
				System.out.println("Candidate is zero");
				break;
			}
			contents = generateStrings(candidates);
			System.out.println("Start write candidates");
			fileUtils.write(contents, conf, location+"Candidates"+k+"/");

			System.out.println("Finish writing candidates");

			//Generate K frequent Itemset
			conf.set("K", String.valueOf(k));
			long itemsetNumber = KFreqItemsetTask.run(conf, k, transactionLoc, location,args[2]);
			if(itemsetNumber == -1){
				System.err.println("Error happens when generating "+ k +" frequent itemsets!");
				System.exit(1);
			}
			System.out.println("Finish generating itemsets");
			
			//Check output size
			if(itemsetNumber == 0){
				break;
			}
			k++;
		}
		System.exit(0);
		
    }

	public static void generateTransactions(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
		boolean succeed = CombineTask.run(args);
		if(!succeed){
			System.err.println("Error happens when generating transactions!");
			System.exit(1);
		}
	}

	public static void countAllItemsFreq(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
		boolean succeed = SongCountTask.run(args);
		if(!succeed){
			System.err.println("Error happens when generating transactions!");
			System.exit(1);
		}
	}

	public static void generateOneFreqItems(String itemFreqLoc, String out, String minSupport) throws InterruptedException, IOException, ClassNotFoundException {
		boolean succeed = OneFreqItemTask.run(itemFreqLoc,out,minSupport);
		if(!succeed){
			System.err.println("Error happens when generating one frequent itemsets!");
			System.exit(1);
		}
	}

	private static List<String> generateStrings(List<List<Integer>> lists){
		StringBuilder builder = new StringBuilder();
		List<String> contents = new ArrayList<String>();
		for(List<Integer> list : lists){
			builder.setLength(0);
			for(int i : list){
				builder.append(i);
				builder.append("\t");
			}
			contents.add(builder.toString());
		}
		
		return contents;
		
	}

}
