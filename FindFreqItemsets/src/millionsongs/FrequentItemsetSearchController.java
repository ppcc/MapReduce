package millionsongs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import millionsongs.prepare.OneFreqItemTask;
import millionsongs.prepare.SongCountTask;

/**
 * This is the controller which is to perform MapReduce Apriori Algorithm on
 * MillionSongs Database.<br>
 * There are three parameters needed when run this class:<br>
 * <p>
 * input - converted user-song-counts triplet<br>
 * baseOutputPath - All outputs created by MapReduce jobs will in this path<br>
 * minSupport - minimum support to determine frequent itemset
 * 
 * @author peilicao
 *
 */
public class FrequentItemsetSearchController {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		if (args.length != 3) {
			System.err
					.println("Usage: FrequentItemSetSearch <input> <baseOutputPath> <minSupport>");
			System.exit(2);
		}

		CandidatesGenerator candidateGenerator = new CandidatesGenerator();
		FileReadWriteUtil fileUtils = new FileReadWriteUtil();
		List<List<Integer>> candidates;

		conf.set("minSupport", args[2]);

		String baseOutputPath = args[1];
		String transactionsPath = args[0] + "/transactions";
		String songFreqPath = args[0] + "/songfreq";
		String oneFreqItemPath = baseOutputPath + "FreqItems1";

		String[] localArgs = { args[0], transactionsPath };
		generateTransactions(localArgs);

		localArgs[0] = transactionsPath;
		localArgs[1] = songFreqPath;
		countAllItemsFreq(localArgs);

		generateOneFreqItems(songFreqPath, oneFreqItemPath, args[2]);

		List<String> candidatesStrings;
		int k = 2;
		while (true) {
			System.out.println("Finding " + k + " Frequent Itemset");
			if (k == 2) {
				candidates = candidateGenerator.generateCandidates2(
						baseOutputPath + "FreqItems1", conf);
			} else {
				/* generate k-1 candidates */
				candidates = candidateGenerator.generateCandidates(
						baseOutputPath + "FreqItems" + (k - 1), conf);
			}

			if (candidates.isEmpty()) {
				System.out.println("The size of candidates is zero");
				break;
			}

			/*
			 * Write candidates to HDFS. These files will be loaded to
			 * DistributedCache
			 */
			candidatesStrings = generateStrings(candidates);
			fileUtils.write(candidatesStrings, conf, baseOutputPath
					+ "Candidates" + k + "/");

			/* Generate K frequent Itemset */
			conf.set("K", String.valueOf(k));
			long itemsetNumber = KFreqItemsetTask.run(conf, k,
					transactionsPath, baseOutputPath, args[2]);
			if (itemsetNumber == -1) {
				System.err.println("Error happens when generating " + k
						+ " frequent itemsets!");
				System.exit(1);
			}
			System.out.println("Finish generating itemsets");

			/* Check output size */
			if (itemsetNumber == 0) {
				break;
			}
			k++;
		}
		System.exit(0);

	}

	/**
	 * This method will call a MapReduce job to generate transactions which is
	 * in format: (<code>userid</code> <code>songid1</code> <code>songid2</code>
	 * ... <code>songidn</code>). And write transactions into output path. This
	 * output will be used when find k frequent itemsets.
	 * <p>
	 * System will exit with error code when this MapReduce job fails.
	 * 
	 * @param args
	 *            - {input,output}
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void generateTransactions(String[] args)
			throws InterruptedException, IOException, ClassNotFoundException {
		boolean succeed = CombineTask.run(args);
		if (!succeed) {
			System.err.println("Error happens when generating transactions!");
			System.exit(1);
		}
	}

	/**
	 * This method will call a MapReduce job to count the frequency for each
	 * song. Then write the results to output path.
	 * <p>
	 * System will exit with error code when this MapReduce job fails.
	 * 
	 * @param args
	 *            - this array of arguments should include two paths:<br>
	 *            first:the path where transactions locates<br>
	 *            second:the path where song frequency should locate.
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void countAllItemsFreq(String[] args)
			throws InterruptedException, IOException, ClassNotFoundException {
		boolean succeed = SongCountTask.run(args);
		if (!succeed) {
			System.err.println("Error happens when generating transactions!");
			System.exit(1);
		}
	}

	/**
	 * This method will call a MapReduce job to generate all qualified one
	 * frequent items, whose frequencies are all not less than
	 * <code>minSupport</code>.
	 * 
	 * <p>
	 * System will exit with error code when this MapReduce job fails.
	 * 
	 * @param songFreqPath
	 *            - a path which contains frequency of all songs
	 * @param oneFreqItemPath
	 *            - a path which one frequency item should be located
	 * @param minSupport
	 *            - minimum support which will determine if a item is frequent
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void generateOneFreqItems(String songFreqPath,
			String oneFreqItemPath, String minSupport)
			throws InterruptedException, IOException, ClassNotFoundException {
		boolean succeed = OneFreqItemTask.run(songFreqPath, oneFreqItemPath,
				minSupport);
		if (!succeed) {
			System.err
					.println("Error happens when generating one frequent itemsets!");
			System.exit(1);
		}
	}

	/**
	 * Convert a list of <code>list of Integer</code> to a list of
	 * <code>String<code>.Uses "\t" as delimiter
	 * 
	 * @param lists
	 * @return List<code><String></code>
	 */
	private static List<String> generateStrings(List<List<Integer>> lists) {
		StringBuilder builder = new StringBuilder();
		List<String> contents = new ArrayList<String>();
		for (List<Integer> list : lists) {
			builder.setLength(0);
			for (int i : list) {
				builder.append(i);
				builder.append("\t");
			}
			contents.add(builder.toString());
		}
		builder.setLength(0);
		return contents;
	}

}
