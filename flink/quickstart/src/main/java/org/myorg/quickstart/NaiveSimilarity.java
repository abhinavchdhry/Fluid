package org.myorg.quickstart;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

public class NaiveSimilarity {
	
	/*
	 *	List of stopwords that should not be accounted for in the similarity match score
	 */
	private final String[] stopwords = {
		"a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren\'t", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can\'t", "cannot", "could", "couldn\'t", "did", "didn\'t", "do", "does", "doesn\'t", "doing", "don\'t", "down", "during", "each", "few", "for", "from", "further", "had", "hadn\'t", "has", "hasn\'t", "have", "haven\'t", "having", "he", "he\'d", "he\'ll", "he\'s", "her", "here", "here\'s", "hers", "herself", "him", "himself", "his", "how", "how\'s", "i", "i\'d", "i\'ll", "i\'m", "i\'ve", "if", "in", "into", "is", "isn\'t", "it", "it\'s", "its", "itself", "let\'s", "me", "more", "most", "mustn\'t", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan\'t", "see", "she", "she\'d", "she\'ll", "she\'s", "should", "shouldn\'t", "so", "some", "such", "than", "that", "that\'s", "the", "their", "theirs", "them", "themselves", "then", "there", "there\'s", "these", "they", "they\'d", "they\'ll", "they\'re", "they\'ve", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn\'t", "we", "we\'d", "we\'ll", "we\'re", "we\'ve", "were", "weren\'t", "what", "what\'s", "when", "when\'s", "where", "where\'s", "which", "while", "who", "who\'s", "whom", "why", "why\'s", "with", "won\'t", "would", "wouldn\'t", "you", "you\'d", "you\'ll", "you\'re", "you\'ve", "your", "yours", "yourself", "yourselves"
				};

	public NaiveSimilarity() { }
	
	public Double computeSimilarity(String s1, String s2) {
		
		Double matches = new Double(0);
		String[] splits2 = s2.split("\\s+");

		List<String> stop = Arrays.asList(stopwords);

		ArrayList<String> ar = new ArrayList<String>();
		for (int i = 0; i < splits2.length; i++) {
			String lower = splits2[i].toLowerCase();
			if (!stop.contains(lower) && s1.contains(lower)) {
				matches += 1;
			}
		}

		return matches;
	}
}
