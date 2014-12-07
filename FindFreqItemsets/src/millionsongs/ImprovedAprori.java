package millionsongs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * My implementation of Improved Aprori Algorithms.<br>
 * This improved Aprori Algorithm is described in <b>Mohammed Al-Maolegi and
 * Bassam Arkok</b>'s paper.
 * <p>
 * It dramatically reduces the number of scans by using a L1 table which records
 * the transaction id for each item.
 * 
 * @author peilicao
 *
 */
public class ImprovedAprori {

	private int minSupport;

	public ImprovedAprori(int ms) {
		minSupport = ms;
	}

	/**
	 * Load transactions from file. Meanwhile, generating a map in which key is
	 * item id and value is a list of transactionId that contains this item id.
	 * One line, one transaction
	 * 
	 * @param filename
	 * @param oneFreqItems
	 *            itemId and its counts
	 * @param transMap
	 *            itemId and the list of transaction id that contains this item
	 * @return
	 * @throws IOException
	 */
	public List<List<Integer>> loadTransactions(String filename,
			Map<Integer, Integer> oneFreqItems,
			Map<Integer, List<Integer>> transMap) throws IOException {
		List<List<Integer>> transactions = new ArrayList<List<Integer>>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(new File(filename)));
			int itemId;
			int count;
			int lineNumber = 1;
			List<Integer> tranList;
			List<Integer> transaction;
			String line;
			int transId = 0; // track id of transaction
			while ((line = br.readLine()) != null) {
				transaction = new ArrayList<Integer>();
				if (line.trim().length() > 0) {
					String[] items = line.split("\t");
					if (lineNumber % 10000 == 0)
						System.out.format("Reading Line: %d \r", lineNumber);

					// Ignore the first item(assuming that first item is user
					// id)
					for (int i = 1; i < items.length; i++) {
						itemId = Integer.parseInt(items[i]);
						transaction.add(itemId);

						// Add to L1 table
						if (transMap.containsKey(itemId)) {
							tranList = transMap.get(itemId);
						} else {
							tranList = new ArrayList<Integer>();
						}
						tranList.add(transId);
						transMap.put(itemId, tranList);
						if (oneFreqItems.containsKey(itemId)) {
							count = oneFreqItems.get(itemId) + 1;
						} else {
							count = 1;
						}
						oneFreqItems.put(itemId, count);
					}
					transId++;
				}
				if (!transaction.isEmpty())
					transactions.add(transaction);
				lineNumber++;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (br != null)
				br.close();
		}

		return transactions;
	}

	/**
	 * Get a list of one frequent items (in order)
	 * 
	 * @param oneItemFreq
	 * @return
	 */
	public List<Integer> getOneFreqItems(Map<Integer, Integer> oneItemFreq) {
		List<Integer> oneItem = new ArrayList<Integer>();
		for (Map.Entry<Integer, Integer> entry : oneItemFreq.entrySet()) {
			if (entry.getValue() >= minSupport) {
				oneItem.add(entry.getKey());
			}
		}
		Collections.sort(oneItem);
		return oneItem;
	}

	/**
	 * Main part of the improved Aprori algorithm. Each loop will first generate
	 * candidates, then find qualified frequent itemset, loop will be terminated
	 * when there is no candidates.
	 * 
	 * @param transactionFile
	 * @param outputFolder
	 * @throws IOException
	 */
	public void generateAllFreqItemsets(String transactionFile,
			String outputFolder) throws IOException {
		CandidatesGenerator candidatesGenerator = new CandidatesGenerator();

		Map<Integer, Integer> oneItemFreq = new HashMap<Integer, Integer>();
		Map<Integer, List<Integer>> itemTransMap = new HashMap<Integer, List<Integer>>();
		List<List<Integer>> Lk = new ArrayList<List<Integer>>();
		List<List<Integer>> candidates;
		int k;

		// Load transactions and generate L1 table
		List<List<Integer>> transactions = loadTransactions(transactionFile,
				oneItemFreq, itemTransMap);
		System.out.format("Transaction Size:%d, oneItemFreq size: %d\n",
				transactions.size(), oneItemFreq.size());
		List<Integer> L1 = getOneFreqItems(oneItemFreq);

		System.out.format("OneFreqItem size: %d\n", L1.size());

		int minSup;
		int minIdx;
		int curSup;
		int count;
		List<Integer> transIds;
		Map<String, Integer> countMap = new HashMap<String, Integer>();
		k = 2;
		while (true) {
			System.out.format("Loop:%d, Lk Size: %d \n", k, Lk.size());
			if (k == 2) {
				candidates = candidatesGenerator.generateCandidates2(L1);
			} else {
				// generate k itemset candidates from Lk-1
				candidates = candidatesGenerator.generateCandidates(Lk);
			}

			Lk.clear();
			countMap.clear();

			List<Integer> transac;
			System.out.format("Candidate Size: %d \n", candidates.size());
			int t = 0;
			for (List<Integer> candidate : candidates) {
				System.out.format("Visiting candidates:%d/%d\r", t,
						candidates.size());
				/**
				 * Find out the minimums number of transactions that need to
				 * scan
				 */
				count = 0;
				minSup = oneItemFreq.get(candidate.get(0));
				minIdx = candidate.get(0);
				for (int item : candidate) {
					curSup = oneItemFreq.get(item);
					if (minSup > curSup) {
						minSup = curSup;
						minIdx = item;
					}

				}

				transIds = itemTransMap.get(minIdx);

				if (transIds.size() < minSupport)
					continue;
				/**
				 * Only scan transactions that find by previous step
				 */
				for (int id : transIds) {
					transac = transactions.get(id);
					if (transac.containsAll(candidate)) {
						count++;
					}
				}

				if (count >= minSupport) {
					Lk.add(candidate);
				}

				t++;
			}

			System.out.format("Outside Lk Size: %d\n", Lk.size());
			// write to files
			if (!Lk.isEmpty()) {
				writeToFile(Lk, k, outputFolder);
				k++;
			} else {
				break;
			}
		}
	}

	/** Helper functions */

	/**
	 * Write L_k into file one line, one set
	 * 
	 * @param Lk
	 *            k frequent item set
	 * @param k
	 *            k
	 * @throws IOException
	 */
	private void writeToFile(List<List<Integer>> Lk, int k, String folder)
			throws IOException {
		File f = new File(folder + "/Lk" + k);
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(f));
			for (List<Integer> itemsets : Lk) {
				bw.write(listToString(itemsets));
				bw.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			bw.close();
		}
	}

	/**
	 * List.toString()
	 * 
	 * @param itemset
	 * @return
	 */
	private String listToString(List<Integer> itemset) {
		StringBuilder builder = new StringBuilder();
		for (int item : itemset) {
			builder.append(item);
			builder.append("\t");
		}
		builder.setLength(builder.length() - 1);
		return builder.toString();
	}

	public static void main(String[] args) throws IOException {
		System.out.println(new Date() + " Program starts to run...");
		ImprovedAprori project = new ImprovedAprori(30000);
		project.generateAllFreqItemsets(
				"/Users/peilicao/WorkSpace/MillionSong/basic/transactions/part-r-00000",
				"/Users/peilicao/WorkSpace/MillionSong/total30000");
		System.out.println(new Date() + " Program ends");
	}
}
