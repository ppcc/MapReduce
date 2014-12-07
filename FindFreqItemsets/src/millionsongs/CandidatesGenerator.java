package millionsongs;

import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.*;

/**
 * Generate k frequent itemsets from k-itemset candidates' files.<br>
 * Created by peilicao on 12/01/14.
 */
public class CandidatesGenerator {

	FileReadWriteUtil utils = new FileReadWriteUtil();

	/**
	 * Check if two lists are qualified to be used to generate candidate.<br>
	 * Return return true if first[0..-2] are equals to second[0...-2],
	 * otherwise return false.
	 * <p>
	 * The two list must already be ordered ascending.
	 * 
	 * @param first
	 *            - a list of <code>songid</code>
	 * @param second
	 *            - a list of <code>songid</code>
	 * @return
	 */
	public boolean isQualified(List<Integer> first, List<Integer> second) {
		int idx1 = 0;
		int idx2 = 0;
		while (idx1 < first.size() - 1 && idx2 < second.size() - 1) {
			if (!first.get(idx1).equals(second.get(idx2)))
				return false;
			idx1++;
			idx2++;
		}
		return true;
	}

	/**
	 * Generate Candidates_2 by self join one frequent items. One frequent items
	 * are loaded from folder name provided.
	 * 
	 * @param folderName
	 *            - Folder contains one frequent items
	 * @param conf
	 *            - job configuration for loading files from HDFS
	 * @return List<<>> Candidates_2
	 * @throws IOException
	 */
	public List<List<Integer>> generateCandidates2(String folderName,
			Configuration conf) throws Exception {
		List<Integer> oneFreqItems = loadOneFreqItems(folderName, conf);
		List<List<Integer>> candidates = generateCandidates2(oneFreqItems);
		return candidates;
	}

	/**
	 * Generate Candidates_2 by self join one frequent items. One frequent items
	 * are provided through parameters
	 * 
	 * @param oneFreqItems
	 *            - List of one frequent items
	 * @return a list of candidates2
	 */
	public List<List<Integer>> generateCandidates2(List<Integer> oneFreqItems) {

		List<List<Integer>> candidates = new ArrayList<List<Integer>>();
		int size = oneFreqItems.size();
		for (int i = 0; i < size; i++) {
			for (int j = i + 1; j < size; j++) {
				List<Integer> list = new ArrayList<Integer>();
				list.add(oneFreqItems.get(i));
				list.add(oneFreqItems.get(j));
				candidates.add(list);
			}
		}
		return candidates;
	}

	/**
	 * Generate candidates_k by doing (k-1) frequent itemsets self join.<br>
	 * (k-1) frequent itemsets is loaded from file.
	 * 
	 * @param folderName
	 *            Folder L_(k-1) which contains frequent itemsets with k-1 items
	 * @param conf
	 *            job configuration for loading files from HDFS
	 * @return a list of candidates_k
	 * @throws IOException
	 */
	public List<List<Integer>> generateCandidates(String folderName,
			Configuration conf) throws Exception {
		List<List<Integer>> freqItemsets = loadFreqItems(folderName, conf);
		List<List<Integer>> candidates = generateCandidates(freqItemsets);
		return candidates;
	}

	/**
	 * Generate candidates_k by doing (k-1) frequent itemsets self join.<br>
	 * (k-1) frequent itemsets is given by parameters.
	 * 
	 * @param freqItemsets
	 *            List of frequent itemsets with k-1 items
	 * @return a list of candidates_k
	 * @throws IOException
	 */
	public List<List<Integer>> generateCandidates(
			List<List<Integer>> freqItemsets) throws IOException {
		List<List<Integer>> candidates = selfJoinFreqItemsets(freqItemsets);
		System.out.format("Candidates original size:%d\n", candidates.size());
		prune(freqItemsets, candidates);
		System.out.format("Candidates after pruned size:%d\n",
				candidates.size());
		return candidates;
	}

	/**
	 * Generate all subsets which is only 1 item less than fatherSet
	 * 
	 * @param fatherSet
	 * @return
	 */
	public List<List<Integer>> generateAllSubsets(List<Integer> fatherSet) {
		List<List<Integer>> subsets = new ArrayList<List<Integer>>();
		for (int i = 0; i < fatherSet.size(); i++) {
			List<Integer> subset = new ArrayList<Integer>();
			for (int j = 0; j < fatherSet.size(); j++) {
				if (j != i)
					subset.add(fatherSet.get(j));
			}
			subsets.add(subset);
		}
		return subsets;
	}

	/**
	 * Prune candidates. Remove the certain candidate if and only if it contains
	 * subset which is not in L_k-1
	 * 
	 * @param freqItemsets
	 *            L_k-1, frequent itemsets with k-1 items
	 * @param candidates
	 *            C_k, frequent itemset candidates with k items
	 */
	private void prune(List<List<Integer>> freqItemsets,
			List<List<Integer>> candidates) {
		Iterator<List<Integer>> itr = candidates.iterator();
		List<Integer> candidate;
		List<List<Integer>> subsets;
		while (itr.hasNext()) {
			candidate = itr.next();
			subsets = generateAllSubsets(candidate);
			// Check if all subsets of candidate is in freqItemsets
			if (!freqItemsets.containsAll(subsets)) {
				itr.remove();
			}
		}
	}

	/**
	 * Self join a list of itemsets. Only join two list if and only if their
	 * items are equals except for last one
	 * 
	 * @param freqItemsets
	 * @return
	 */
	private List<List<Integer>> selfJoinFreqItemsets(
			List<List<Integer>> freqItemsets) {
		List<List<Integer>> candidates = new ArrayList<List<Integer>>();
		int size = freqItemsets.size();
		List<Integer> first;
		List<Integer> second;
		List<Integer> cand;
		for (int i = 0; i < size; i++) {
			first = freqItemsets.get(i);
			for (int j = i + 1; j < size; j++) {
				second = freqItemsets.get(j);
				if (isQualified(first, second)) {
					cand = new ArrayList<Integer>();
					for (int tmp : first)
						cand.add(tmp);
					cand.add(second.get(second.size() - 1));
					candidates.add(cand);
				} else {
					break;
				}
			}
		}
		return candidates;
	}

	/**
	 * Load K-1 frequent itemsets from folder
	 *
	 * @param folderName
	 * @return
	 * @throws IOException
	 */
	private List<List<Integer>> loadFreqItems(String folderName,
			Configuration conf) throws Exception {
		List<List<Integer>> freqItems = new ArrayList<List<Integer>>();
		List<String> contents = utils.read(folderName, conf);
		String[] items;
		for (String line : contents) {
			List<Integer> candidate = new ArrayList<Integer>();
			items = line.split("\t");
			for (String item : items) {
				candidate.add(Integer.parseInt(item));
			}
			freqItems.add(candidate);
		}
		return freqItems;
	}

	/**
	 * Load 1-frequent itemsets from folder
	 * 
	 * @param folder
	 * @return conf
	 * @throws IOException
	 */
	private List<Integer> loadOneFreqItems(String folder, Configuration conf)
			throws Exception {
		List<Integer> oneFreqItems = new ArrayList<Integer>();
		List<String> contents = utils.read(folder, conf);
		for (String line : contents) {
			oneFreqItems.add(Integer.parseInt(line));
		}
		Collections.sort(oneFreqItems);
		return oneFreqItems;
	}
}
