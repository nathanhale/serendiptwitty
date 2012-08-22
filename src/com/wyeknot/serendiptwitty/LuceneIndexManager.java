package com.wyeknot.serendiptwitty;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

import com.wyeknot.serendiptwitty.StanfordTweetIndexer.StanfordParser;

public class LuceneIndexManager {

	public static final String USER_NAME_FIELD_NAME = "user";
	public static final String DATE_FIELD_NAME = "date";
	public static final String TWEET_FIELD_NAME = "tweet";

	Directory index;

	IndexReader reader;
	IndexSearcher searcher;

	private IndexWriter writer = null;
	Analyzer gAnalyzer = null;

	private Map<String,Integer> terms;

	public LuceneIndexManager(String path) throws IOException { 
		index = FSDirectory.open(new File(path));
	}

	public boolean indexExists() {
		try {
			return IndexReader.indexExists(index);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public Directory getIndex() {
		return index;
	}
	
	public IndexReader getReader() {
		return reader;
	}

	public static IndexWriter getIndexWriter(Directory index, Analyzer analyzer) throws CorruptIndexException, LockObtainFailedException, IOException {
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_35, analyzer);
		return new IndexWriter(index, config);
	}

	public static IndexWriter getIndexWriter(Directory index) throws CorruptIndexException, LockObtainFailedException, IOException {
		Analyzer analyzer = getDefaultAnalyzer();
		return getIndexWriter(index,analyzer);
	}

	public static Analyzer getDefaultAnalyzer() {
		return new StandardAnalyzer(Version.LUCENE_35);
	}

	public Analyzer getOrInitializeAnalyzer() {
		if (null == gAnalyzer) {
			gAnalyzer = LuceneIndexManager.getDefaultAnalyzer();
		}

		return gAnalyzer;
	}

	private void initializeIndexForWriting() throws CorruptIndexException, LockObtainFailedException, IOException {
		if (null == writer) {
			writer = getIndexWriter(index, getOrInitializeAnalyzer());
		}
	}

	public void addDocument(Document doc) throws CorruptIndexException, LockObtainFailedException, IOException {
		initializeIndexForWriting(); //This function only initializes it if needed
		writer.addDocument(doc);
	}

	public void closeIndexForWriting() {
		try {
			if (null != writer) {
				writer.close();
			}

			writer = null; //So that it will get initialized if it's needed again
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void initializeForReading() throws CorruptIndexException, IOException {
		if (null == reader) {
			reader = IndexReader.open(index);
			searcher = new IndexSearcher(reader);
		}
	}

	public void createDocumentFrequency() throws CorruptIndexException, IOException {

		initializeForReading();

		// first find all terms in the index
		terms = new HashMap<String,Integer>();
		TermEnum termEnum = reader.terms();

		while (termEnum.next()) {
			Term term = termEnum.term();

			if (!"tweet".equals(term.field())) {
				continue;
			}

			terms.put(term.text(), reader.docFreq(term));
		}
	}

	public Map<String,Integer> getDocumentFrequency() {
		return terms;
	}

	public Document getDocumentFromDocId(int doc) throws CorruptIndexException, IOException {
		return searcher.doc(doc);
	}

	ScoreDoc[] runQuery(String queryString) throws ParseException, CorruptIndexException, IOException {

		initializeForReading();

		//We search in the tweets themselves if nothing else is specified
		Query query = new QueryParser(Version.LUCENE_35, TWEET_FIELD_NAME, getOrInitializeAnalyzer()).parse(queryString);

		TopScoreDocCollector collector = TopScoreDocCollector.create(25000, true);

		searcher.search(query, collector);

		return collector.topDocs().scoreDocs;
	}

	public void closeIndexForReading() {
		if (null != reader) {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			reader = null;
		}

		if (null != searcher) {
			try {
				searcher.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			searcher = null;
		}
	}
	
	public void indexTweets(String path) {
		parser.indexTweets(path, Recommender.NUM_TWEETS_TO_INDEX);
	}
	
	StanfordParser parser = new StanfordParser() {
		@Override
		boolean tweetHandler(Tweet tweet) {
			Document doc = new Document();
			Long curTweet = Long.valueOf(tweet.id);
			doc.add(new Field("tweetid", curTweet.toString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.add(new Field("user", tweet.user, Field.Store.YES, Field.Index.ANALYZED));
			doc.add(new Field("date", tweet.timestamp.toString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.add(new Field("tweet", tweet.tweet, Field.Store.YES, Field.Index.ANALYZED, TermVector.YES));
			try {
				addDocument(doc);
			} catch (CorruptIndexException e) {
				e.printStackTrace();
				return false;
			} catch (LockObtainFailedException e) {
				e.printStackTrace();
				return false;
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
			
			return true;
		}
	};

	

	public static class DocVector {

		private Map<String,Double> terms;
		private double magnitude = 0;

		/* At creation, the map takes a term to the term frequency
		 * in the vector. It will later be modified to be tf*idf
		 */
		public DocVector(String[] termList, int[] frequenciesList, Map<String,Integer> dfValues, int totalDocs) {
			if (termList.length != frequenciesList.length) {
				throw new RuntimeException("Term list and Frequency list had different lengths!");
			}

			terms = new HashMap<String,Double>();

			for (int ii = 0 ; ii < termList.length ; ii++) {
				if (!dfValues.containsKey(termList[ii])) {
					System.err.println("Something is wrong -- all of the terms should be in the df values -- " + termList[ii]);
					continue;
				}

				int df = dfValues.get(termList[ii]).intValue();
				double idf = Math.log10((double)totalDocs / (double)(df + 1)) * (1.0 / (Math.log10((double)(df + 1))));

				terms.put(termList[ii], Double.valueOf((double)frequenciesList[ii] * idf));
			}
		}

		public double cosineSimilarity(DocVector v) {

			double curSum = 0;

			for (String term : terms.keySet()) {
				double otherVal = v.getTfIdfValue(term);
				if (otherVal == 0) {
					continue;
				}

				curSum += otherVal * terms.get(term).doubleValue();
			}

			double magnitudes = v.getMagnitude() * this.getMagnitude();
			if (magnitudes == 0) {
				return 0;
			}

			curSum /= magnitudes;

			return curSum;
		}

		public double getMagnitude() {
			if (magnitude == 0) {
				double curSum = 0;

				for (Double tfIdf : terms.values()) {
					curSum += tfIdf.doubleValue() * tfIdf.doubleValue();
				}

				magnitude = Math.sqrt(curSum);
			}

			return magnitude;
		}

		public double getTfIdfValue(String term) {
			if (terms.containsKey(term)) {
				return terms.get(term).doubleValue();
			}
			else {
				return 0;
			}
		}
	}
}
