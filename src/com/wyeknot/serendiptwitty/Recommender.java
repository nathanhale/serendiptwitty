package com.wyeknot.serendiptwitty;

import java.awt.EventQueue;
import java.util.HashSet;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ling.CoreLabel;


public class Recommender {

	private LuceneIndexManager indexMgr;
	private DatabaseInterface database;
	private GraphManager graph;

	public static final String DEFAULT_TWEET_DATA_PATH = "/Users/nathan/Documents/MSc Project/Twitter Data/Stanford/";
	public static final String DEFAULT_LUCENE_INDEX_PATH = DEFAULT_TWEET_DATA_PATH + "lucene_index/";
	public static final String DEFAULT_NER_CLASSIFIERS_PATH = "/Users/nathan/Documents/MSc Project/classifiers/";
	
	public static String tweetDataPath = DEFAULT_TWEET_DATA_PATH;
	public static String luceneIndexPath = DEFAULT_LUCENE_INDEX_PATH; 
	public static String nerClassifiersPath = DEFAULT_NER_CLASSIFIERS_PATH;

	AbstractSequenceClassifier<CoreLabel> classifier;
	
	public static final int NUM_TWEETS_TO_INDEX = 500000;

	public static void main(String[] args) {
		
		if (args.length > 0) {
			Recommender.tweetDataPath = args[0];
			
			if (args.length > 1) {
				Recommender.luceneIndexPath = args[1];
				
				if (args.length > 2) {
					Recommender.nerClassifiersPath = args[2];
				}
			}
		}
		
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					Recommender recommender = new Recommender();
					recommender.recommend();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	public void recommend() {
		HashSet<String> otherDistinguishedUsers = new HashSet<String>(2);
		graph = new GraphManager(database, indexMgr, "____", otherDistinguishedUsers);
		graph.createGraph();
		graph.runAlgorithm();
	}

	public Recommender() {
		try {
			database = new DatabaseInterface();

			indexMgr = new LuceneIndexManager(Recommender.luceneIndexPath);
			if (!indexMgr.indexExists()) {				
				indexMgr.indexTweets(Recommender.tweetDataPath);
				indexMgr.closeIndexForWriting();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}




