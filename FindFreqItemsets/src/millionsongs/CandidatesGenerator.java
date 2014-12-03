package millionsongs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Generate k frequent itemsets from k-itemset candidates' files.
 * Created by cpp on 12/01/14.
 */
public class CandidatesGenerator {

    /**
     * Check if first[0..-2] are equals to second[0...-2]
     * @param first
     * @param second
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
     * Generate Candidates_2
     * @param folderName Folder contains one frequent items
     * @return Candidates_2
     * @throws IOException
     */
    public List<List<Integer>> generateCandidates2(String folderName) throws IOException {
        List<Integer> oneFreqItems = loadOneFreqItems(folderName);
        List<List<Integer>> candidates = generateCandidates2(oneFreqItems);

        return candidates;
    }

    /**
     * Generate Candidates_2 (in order self join)
     * @param oneFreqItems List of one frequent items
     * @return
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
     * Generate candidates_k
     * @param folderName Folder L_(k-1) which contains frequent itemsets with k-1 items
     * @return
     * @throws IOException
     */
    public List<List<Integer>> generateCandidates(String folderName) throws IOException {
        List<List<Integer>> freqItemsets = loadFreqItems(folderName);

        //self join
        List<List<Integer>> candidates = generateCandidates(freqItemsets);
        return candidates;
    }

    /**
     * Generate candidates_k
     * @param freqItemsets List of frequent itemsets with k-1 items
     * @return
     * @throws IOException
     */
    public List<List<Integer>> generateCandidates(List<List<Integer>> freqItemsets) throws IOException {
        //self join
        List<List<Integer>> candidates = selfJoinFreqItemsets(freqItemsets);
        System.out.format("Candidates original size:%d\n", candidates.size());
        //prune
        prune(freqItemsets, candidates);
        System.out.format("Candidates after pruned size:%d\n", candidates.size());
        return candidates;
    }

    /**
     * Generate all subsets which is only 1 item less than fatherSet
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
     * Prune candidates
     * Remove candidate if it contains subset which is not in L_k-1
     * @param freqItemsets L_k-1, frequent itemsets with k-1 items
     * @param candidates C_k, frequent itemset candidates with k items
     */
    private void prune(List<List<Integer>> freqItemsets, List<List<Integer>> candidates) {
        Iterator<List<Integer>> itr = candidates.iterator();
        List<Integer> candidate;
        List<List<Integer>> subsets;
        while (itr.hasNext()) {
            candidate = itr.next();
            subsets = generateAllSubsets(candidate);
            //Check if all subsets of candidate is in freqItemsets
            if (!freqItemsets.containsAll(subsets)) {
                itr.remove();
            }
        }
    }

    /**
     * Self join a list of itemsets.
     * Only join two list if and only if their items are equals except for last one
     * @param freqItemsets
     * @return
     */
    private List<List<Integer>> selfJoinFreqItemsets(List<List<Integer>> freqItemsets) {
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
    private List<List<Integer>> loadFreqItems(String folderName) throws IOException {
        List<List<Integer>> freqItems = new ArrayList<List<Integer>>();
        File folder = new File(folderName);
        File[] files = folder.listFiles();
        for (File file : files) {
            if (file.getName().startsWith("."))
                continue;
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                List<Integer> candidate = new ArrayList<Integer>();
                String[] items = line.split("\t");
                for (String item : items) {
                    candidate.add(Integer.parseInt(item));
                }
                freqItems.add(candidate);
            }
            reader.close();
        }
        return freqItems;
    }

    /**
     * Load 1-frequent itemsets from folder
     * @param folderName
     * @return
     * @throws IOException
     */
    private List<Integer> loadOneFreqItems(String folderName) throws IOException {
        List<Integer> oneFreqItems = new ArrayList<Integer>();
        File folder = new File(folderName);
        File[] files = folder.listFiles();
        for (File file : files) {
            if (file.getName().startsWith("."))
                continue;
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                oneFreqItems.add(Integer.parseInt(line));
            }
            reader.close();
        }
        return oneFreqItems;
    }
}
