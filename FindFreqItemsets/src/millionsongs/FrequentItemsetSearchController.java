package millionsongs;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import millionsongs.prepare.OneFreqItemTask;
import millionsongs.prepare.SongCountTask;

public class FrequentItemsetSearchController {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException{
		Configuration conf = new Configuration();

		if (args.length != 3) {
            System.err.println("Usage: FrequentItemSetSearch <input> <location> <minSupport>");
            System.exit(2);
        }
		
		String location = args[1];
		String transactionLoc = location+"transactions";
		String itemFreqLoc = location+"songfreq";
		String oneFreqItemLoc = location+"FreqItems1";
		Boolean succeed;
		List<List<Integer>> candidates;
		
		conf.set("minSupport", args[2]);
		
		
		CandidatesGenerator candidateGenerator = new CandidatesGenerator();
		FileReadWriteUtil fileUtils = new FileReadWriteUtil();
		
		//Generating Transactions
		String[] localArgs = {args[0],transactionLoc};
//		succeed = CombineTask.run(localArgs);
//		if(!succeed){
//			System.err.println("Error happens when generating transactions!");
//			System.exit(1);
//		}
//		
//		//Count item freq
		localArgs[0] = transactionLoc;
		localArgs[1] = itemFreqLoc;
		succeed = SongCountTask.run(localArgs);
		if(!succeed){
			System.err.println("Error happens when generating transactions!");
			System.exit(1);
		}
		
		//Generate OneFreqItem
		succeed = OneFreqItemTask.run(itemFreqLoc,oneFreqItemLoc,args[2]);
		if(!succeed){
			System.err.println("Error happens when generating one frequent itemsets!");
			System.exit(1);
		}
		
		//Generate Candidates2
		//Loop
		List<String> contents;
		int k = 2;
		while(true){
			System.out.println("Finding "+k+" Itemset");
			if(k==2){
				candidates = candidateGenerator.generateCandidates2(location+"FreqItems1");
			}else{
				//Generate Candidates
				candidates = candidateGenerator.generateCandidates(location+"FreqItems"+(k-1));
			}
			contents = generateStrings(candidates);
			fileUtils.write(contents, conf, location+"Candidates"+k+"/");
			
			System.out.println("Finish generating candidates");
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
