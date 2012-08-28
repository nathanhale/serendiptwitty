package com.wyeknot.serendiptwitty;


import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import com.wyeknot.serendiptwitty.LuceneIndexManager.DocVector;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations.AnswerAnnotation;
import edu.stanford.nlp.ling.CoreLabel;


public class GraphManager {

	public static final double DEFAULT_ORIGINAL_SCORE = 0;

	private LuceneIndexManager indexMgr;
	private DatabaseInterface database;

	private String distinguishedUser;
	private Set<String> otherDistinguishedUsers;

	//Protects us from adding a user into the tweets twice
	private HashSet<String> usersInTweets;

	private HashSet<String> userBatch;
	private HashSet<Tweet> tweetBatch;
	private HashSet<Edge> edgeBatch;

	private static final int MAX_ITERATIONS = 10;

	//Values closer to 0 put more weight on the original score
	private static final double lambdaUsers = 0.7;
	private static final double lambdaTweets = 0.9;

	private static final double MAX_USER_SCORE_MULTIPLIER = 1.5;


	GraphManager(DatabaseInterface database, LuceneIndexManager index, String distinguishedUser, Set<String> otherDistinguishedUsers) {
		this.database = database;
		this.indexMgr = index;
		this.distinguishedUser = distinguishedUser.toLowerCase();
		this.otherDistinguishedUsers = otherDistinguishedUsers;

		usersInTweets = new HashSet<String>();

		userBatch = new HashSet<String>();
		tweetBatch = new HashSet<Tweet>();
		edgeBatch = new HashSet<Edge>();
	}

	
	/*
	 * 
	 * Code for creating the graph, including edges and vertices
	 *   
	 */

	public void createGraph() {
		if (!database.tableHasRows("edges")) {

			usersInTweets.add(distinguishedUser);
			userBatch.add(distinguishedUser);

			for (String user : otherDistinguishedUsers) {
				user = user.toLowerCase();
				userBatch.add(user);
				usersInTweets.add(user);
			}

			tweetsParser.indexTweets(Recommender.tweetDataPath, Recommender.NUM_TWEETS_TO_INDEX);

			if (userBatch.size() > 0) {
				database.addUserBatch(userBatch);
				userBatch.clear();
			}

			if (tweetBatch.size() > 0) {
				database.addTweetBatch(tweetBatch);
				tweetBatch.clear();
			}

			database.clusterUsers();			
			database.clusterTweetsByAuthor();

			//Now create the remainder of the edges
			createFollowerRetweetAndMentionEdges();
			createContentEdges();

			if (edgeBatch.size() > 0) {
				database.addEdgeBatch(edgeBatch);
				edgeBatch.clear();
			}

			
			database.clusterEdges();
			database.clusterTweetsById();
			
			database.createAndClusterEdgesTweetIds();
			database.analyzeTables();
		}
	}

	private void createFollowerRetweetAndMentionEdges() {

		database.acquireCursorForTweetVertices();

		String tweet = null;
		List<Long> curUserTweetIds = new ArrayList<Long>();
		String lastUser = null;

		while (null != (tweet = database.getNextTweetFromCursor())) {

			String tweeter = database.getNameFromCurrentCursorPos();
			long tweetId = database.getTweetIdFromCurrentCursorPos();

			if (!tweeter.equals(lastUser) && (lastUser != null)) {
				createFollowerEdgesForUser(lastUser,curUserTweetIds);
				curUserTweetIds.clear();
			}

			lastUser = tweeter;
			curUserTweetIds.add(Long.valueOf(tweetId));

			Set<String> retweets = Tweet.findRetweetedUsers(tweet);
			String atReply = Tweet.findAtReply(tweet);
			Set<String> mentions = Tweet.findMentionedUsers(tweet, retweets);

			createRetweetEdgesForTweet(tweeter, tweetId, retweets);

			createMentionEdgesForTweet(tweeter, tweetId, mentions);

			if (null != atReply) {
				database.acquireInternalCursorForTweets(atReply);
				long internalTweetId = -1;
				while (-1 != (internalTweetId = database.getNextTweetIdFromInternalCursor())) {
					addEdge(tweeter, internalTweetId, Edge.Types.EDGE_TYPE_AT_REPLY_CONTENT, false);
				}
			}
		}
	}

	private void createFollowerEdgesForUser(String user, List<Long> curUserTweetIds) {		
		database.acquireInternalCursorForFollowersInUserVertices(user);

		String internalUser = null;
		while (null != (internalUser = database.getNextNameFromInternalCursor())) {
			for (Long id : curUserTweetIds) {
				addEdge(internalUser, id.longValue(), Edge.Types.EDGE_TYPE_FOLLOWER, false);
			}
		}
	}

	private void createRetweetEdgesForTweet(String tweeter, long tweetId, Set<String> retweets) {
		for (String retweetee : retweets) {
			//Connects the tweets of the followees of the person being retweeted to the retweeter
			database.acquireInternalCursorForFolloweesEdges(retweetee, tweeter,
					Edge.Types.EDGE_TYPE_AUTHORSHIP.id(), Edge.Types.EDGE_TYPE_RETWEET_FOLLOWEES.id());

			long internalTweetId = -1;
			while (-1 != (internalTweetId = database.getNextTweetIdFromInternalCursor())) {
				addEdge(tweeter, internalTweetId, Edge.Types.EDGE_TYPE_RETWEET_FOLLOWEES, false);
			}

			//Connects the tweets of the person being retweeted to the followers of the retweeter				
			database.acquireInternalCursorForFollowersEdges(retweetee, tweeter,
					Edge.Types.EDGE_TYPE_AUTHORSHIP.id(), Edge.Types.EDGE_TYPE_RETWEET_FOLLOWERS.id());
			internalTweetId = -1;

			while (-1 != (internalTweetId = database.getNextTweetIdFromInternalCursor())) {
				String internalUserName = database.getNameFromCurrentInternalCursorPos();
				addEdge(internalUserName, internalTweetId, Edge.Types.EDGE_TYPE_RETWEET_FOLLOWERS, false);	
			}
		}
	}

	private void createMentionEdgesForTweet(String tweeter, long tweetId, Set<String> mentions) {
		for (String mentionee : mentions) {
			//Connects the tweets of the followees of the person being mentioned to the mentioner
			database.acquireInternalCursorForFolloweesEdges(mentionee, tweeter,
					Edge.Types.EDGE_TYPE_AUTHORSHIP.id(), Edge.Types.EDGE_TYPE_MENTION_FOLLOWEES.id());


			long internalTweetId = -1;
			while (-1 != (internalTweetId = database.getNextTweetIdFromInternalCursor())) {
				addEdge(tweeter, internalTweetId, Edge.Types.EDGE_TYPE_MENTION_FOLLOWEES, false);
			}

			//Connects the tweets of the person being mentioned to the followers of the mentioner				
			database.acquireInternalCursorForFollowersEdges(mentionee, tweeter,
					Edge.Types.EDGE_TYPE_AUTHORSHIP.id(), Edge.Types.EDGE_TYPE_MENTION_FOLLOWERS.id());
			internalTweetId = -1;

			while (-1 != (internalTweetId = database.getNextTweetIdFromInternalCursor())) {
				String internalUserName = database.getNameFromCurrentInternalCursorPos();
				addEdge(internalUserName, internalTweetId, Edge.Types.EDGE_TYPE_MENTION_FOLLOWERS, false);	
			}
		}
	}


	private void createContentEdges() {
		String serializedClassifier = Recommender.nerClassifiersPath + "english.all.3class.distsim.crf.ser.gz";

		@SuppressWarnings("unchecked")
		AbstractSequenceClassifier<CoreLabel> classifier = CRFClassifier.getClassifierNoExceptions(serializedClassifier);

		/*
		 * These HashMaps have the entity/hashtag as the key, and then for
		 * each hashtag there is a List of tweetId/author pairs and a set
		 * of authors who have used this entity/hashtag.
		 * 
		 * We need to keep track of the tweet id AND the author of each
		 * tweet because we don't want to create a link between the author's
		 * own tweet and their vertex.
		 *  
		 * Note: If we have a set of tweetids instead of a list then we can
		 * prevent duplicate edges, but I think that the duplicate edges
		 * add extra value, so I don't plan to implement it that way.
		 */		
		HashMap<String , Pair< List<Pair<Long,String>> , Set<String> > > entities =
				new HashMap<String,Pair<List<Pair<Long,String>>,Set<String>>>();
		HashMap<String , Pair< List<Pair<Long,String>> , Set<String> > > hashtags =
				new HashMap<String,Pair<List<Pair<Long,String>>,Set<String>>>();

		database.acquireCursorForTweetVertices();

		String tweet = null;

		while (null != (tweet = database.getNextTweetFromCursor())) {
			String tweeter = database.getNameFromCurrentCursorPos();
			Long tweetId = Long.valueOf(database.getTweetIdFromCurrentCursorPos());

			List<List<CoreLabel>> out = classifier.classify(tweet);
			for (List<CoreLabel> sentence : out) {
				for (CoreLabel word : sentence) {
					String type = word.get(AnswerAnnotation.class);

					if (!type.equals("O")) {
						String key = word.word().toLowerCase();

						if (!Character.isLetter(key.charAt(0)) || key.equals("rt")) {
							continue;
						}

						if (!entities.containsKey(key)) {
							ArrayList<Pair<Long,String>> tweets = new ArrayList<Pair<Long,String>>();
							HashSet<String> users = new HashSet<String>();

							tweets.add(new Pair<Long,String>(tweetId,tweeter));
							users.add(tweeter);							

							Pair<List<Pair<Long,String>>,Set<String>> value =
									new Pair<List<Pair<Long,String>>,Set<String>>(tweets, users);

							entities.put(key, value);
						}
						else {
							Pair<List<Pair<Long,String>>,Set<String>> value = entities.get(key);
							value.getFirst().add(new Pair<Long,String>(tweetId,tweeter));
							value.getSecond().add(tweeter);
						}
					}

					/* Ignore something if it is not a proper word AND
					 * it isn't BOTH preceded and followed by another entity
					 */

					/* Can see if this words .beginPosition is one above the
					 * last word's .endPosition to ignore spaces.
					 */
				}
			}

			Set<String> hashtagsInTweet = Tweet.findHashTags(tweet);
			for (String tag : hashtagsInTweet) {
				String key = tag.toLowerCase();

				if (!hashtags.containsKey(tag)) {
					ArrayList<Pair<Long,String>> tweets = new ArrayList<Pair<Long,String>>();
					HashSet<String> users = new HashSet<String>();

					tweets.add(new Pair<Long,String>(tweetId,tweeter));
					users.add(tweeter);

					Pair<List<Pair<Long,String>>,Set<String>> value = new Pair<List<Pair<Long,String>>,Set<String>>(tweets, users);

					hashtags.put(key, value);
				}
				else {
					Pair<List<Pair<Long,String>>,Set<String>> value = hashtags.get(key);
					value.getFirst().add(new Pair<Long,String>(tweetId,tweeter));
					value.getSecond().add(tweeter);
				}
			}
		}

		//Evaluate the entities
		
		for (Pair<List<Pair<Long,String>>,Set<String>> edges : entities.values()) {
			List<Pair<Long,String>> tweetIds = edges.getFirst();
			Set<String> authors = edges.getSecond();


			for (Pair<Long,String> tweetIdAndAuthor : tweetIds) {
				for (String author : authors) {
					if (!author.equals(tweetIdAndAuthor.getSecond())) {
						addEdge(author, tweetIdAndAuthor.getFirst(), Edge.Types.EDGE_TYPE_CONTENT, false);
					}
				}
			}
		}


		//Evaluate the hashtags

		for (Pair<List<Pair<Long,String>>,Set<String>> edges : hashtags.values()) {
			List<Pair<Long,String>> tweetIds = edges.getFirst();
			Set<String> authors = edges.getSecond();

			for (Pair<Long,String> tweetIdAndAuthor : tweetIds) {
				for (String author : authors) {
					if (!author.equals(tweetIdAndAuthor.getSecond())) {
						addEdge(author, tweetIdAndAuthor.getFirst(), Edge.Types.EDGE_TYPE_HASHTAG, false);
					}
				}
			}
		}
	}

	
	Set<String> createBasicRetweetEdges(String tweet, long curTweetId) {
		Set<String> retweets = Tweet.findRetweetedUsers(tweet);
		for (String s : retweets) {
			addEdge(s, curTweetId, Edge.Types.EDGE_TYPE_RETWEET, true);
		}

		return retweets;
	}

	void createBasicAtReplyEdge(String tweet, long curTweetId) {
		String atReply = Tweet.findAtReply(tweet);
		if (null != atReply) {
			addEdge(atReply, curTweetId, Edge.Types.EDGE_TYPE_AT_REPLY, true);
		}
	}

	void createBasicMentionEdges(String tweet, long curTweetId, Set<String> retweetNames) {
		Set<String> mentions = Tweet.findMentionedUsers(tweet, retweetNames);
		for (String s : mentions) {
			addEdge(s, curTweetId, Edge.Types.EDGE_TYPE_MENTION, true);
		}
	}

	
	/*
	 * 
	 * Code for updating the scores
	 * 
	 */

	private void updateTweetsFromUser(String userName, double userScore, Set<Pair<Integer,Long>> edgeTypeAndDest,
			Map<Long,Double> updatedTweetScores, double totalEdgeWeight) {

		for (Pair<Integer,Long> t : edgeTypeAndDest) {
			double chanceOfGoingToTweet = (Edge.idToEdgeType[t.getFirst()].probability() / totalEdgeWeight);
			double scoreEffectFromThisEdge = chanceOfGoingToTweet * userScore * lambdaTweets;

			Long tweetId = t.getSecond();

			if (!updatedTweetScores.containsKey(tweetId)) {
				updatedTweetScores.put(tweetId, Double.valueOf(scoreEffectFromThisEdge));
			}
			else {
				Double currentScore = updatedTweetScores.get(tweetId);
				double newScore = scoreEffectFromThisEdge + currentScore.doubleValue();
				updatedTweetScores.put(tweetId, Double.valueOf(newScore));
			}
		}
	}

	private void updateUsersFromTweet(long tweetId, double tweetScore, Set<Pair<Integer,String>> edgeTypeAndDest,
			Map<String,Double> updatedUserScores, double totalEdgeWeight) {

		for (Pair<Integer,String> t : edgeTypeAndDest) {
			double chanceOfGoingToUser = Edge.idToEdgeType[t.getFirst()].probability() / totalEdgeWeight;
			double scoreEffectFromThisEdge = chanceOfGoingToUser * tweetScore * lambdaUsers;

			String userName = t.getSecond();

			if (!updatedUserScores.containsKey(userName)) {
				updatedUserScores.put(userName, Double.valueOf(scoreEffectFromThisEdge));
			}
			else {
				Double currentScore = updatedUserScores.get(userName);
				double newScore = scoreEffectFromThisEdge + currentScore.doubleValue();
				updatedUserScores.put(userName, Double.valueOf(newScore));
			}
		}
	}

	private void updateTweetScores() {
		database.acquireCursorForUpdatingTweetScores();

		long tweetId = -1;

		String userName = null;
		String lastUserName = null;
		double lastUserScore = -1;
		double lastUserTotalEdgeWeight = 0;

		double scoreTotalFromVerticesWithNoExit = 0;

		//Holds a record of the scores of all the tweets that we've updated the score for
		Map<Long,Double> updatedTweetScores = new HashMap<Long,Double>();

		Set<Pair<Integer,Long>> currentUserTypesAndDestinations = new HashSet<Pair<Integer,Long>>();
		
		while (null != (userName = database.getNextNameFromCursor())) {			
			if (!userName.equals(lastUserName) && (lastUserName != null)) {
				if (currentUserTypesAndDestinations.isEmpty()) {
					scoreTotalFromVerticesWithNoExit += lastUserScore;
				}
				else {
					updateTweetsFromUser(lastUserName, lastUserScore, currentUserTypesAndDestinations,
							updatedTweetScores,lastUserTotalEdgeWeight);
					currentUserTypesAndDestinations.clear();
					lastUserTotalEdgeWeight = 0;
				}
			}

			tweetId = database.getTweetIdFromCurrentCursorPos();
			if (-1 == tweetId) {
				throw new RuntimeException("Error retrieving tweetId while calculating tweet scores!");
			}

			Edge.Types type = Edge.idToEdgeType[database.getEdgeTypeFromCurrentCursorPos()];
			if (type.userToTweetDir()) {
				if (currentUserTypesAndDestinations.add(new Pair<Integer,Long>(
						Integer.valueOf(type.id()),
						Long.valueOf(tweetId)))) {
					lastUserTotalEdgeWeight += type.probability();
				}
			}

			lastUserName = userName;
			lastUserScore = database.getUserScoreFromCurrentCursorPos();
		}

		//And update the last tweet
		if (currentUserTypesAndDestinations.isEmpty()) {
			scoreTotalFromVerticesWithNoExit += lastUserScore;
		}
		else {
			updateTweetsFromUser(lastUserName, lastUserScore, currentUserTypesAndDestinations, updatedTweetScores,
					lastUserTotalEdgeWeight);
		}

		database.updateBaseTweetScores(lambdaTweets, scoreTotalFromVerticesWithNoExit);
		database.updateTweetScores(updatedTweetScores);
	}

	private void updateUserScores() {
		database.acquireCursorForUpdatingUserScores();

		String userName = null;

		long tweetId = -1;
		long lastTweetId = -1;
		double lastTweetScore = -1;
		double lastUserTotalEdgeWeight = 0;

		double scoreTotalFromVerticesWithNoExit = 0;

		//Holds a record of the scores of all the users that we've updated the score for
		Map<String,Double> updatedUserScores = new HashMap<String,Double>();

		Set<Pair<Integer,String>> currentTweetTypesAndDestinations = new HashSet<Pair<Integer,String>>();
		
		while (-1 != (tweetId = database.getNextTweetIdFromCursor())) {

			if ((lastTweetId != tweetId) && (lastTweetId != -1)) {
				if (currentTweetTypesAndDestinations.isEmpty()) {
					scoreTotalFromVerticesWithNoExit += lastTweetScore;
				}
				else {
					updateUsersFromTweet(lastTweetId, lastTweetScore, currentTweetTypesAndDestinations,
							updatedUserScores, lastUserTotalEdgeWeight);
					currentTweetTypesAndDestinations.clear();
					lastUserTotalEdgeWeight = 0;
				}
			}

			userName = database.getNameFromCurrentCursorPos();
			if (null == userName) {
				throw new RuntimeException("Error retrieving userId while calculating user scores!");
			}

			Edge.Types type = Edge.idToEdgeType[database.getEdgeTypeFromCurrentCursorPos()];

			if (type.tweetToUserDir()) {
				if (currentTweetTypesAndDestinations.add(new Pair<Integer,String>(
						Integer.valueOf(type.id()),
						userName))) {
					lastUserTotalEdgeWeight += type.probability();
				}
			}

			lastTweetId = tweetId;
			lastTweetScore = database.getTweetScoreFromCurrentCursorPos();
		}

		//And update the last user
		if (currentTweetTypesAndDestinations.isEmpty()) {
			scoreTotalFromVerticesWithNoExit += lastTweetScore;
		}
		else {
			updateUsersFromTweet(lastTweetId, lastTweetScore, currentTweetTypesAndDestinations,
					updatedUserScores, lastUserTotalEdgeWeight);
		}

		database.updateBaseUserScores(lambdaUsers, scoreTotalFromVerticesWithNoExit);
		database.updateUserScores(updatedUserScores);
	}


	private void initializeUserScores() {

		database.acquireCursorForInitializingUserScores();

		String userName = null;

		/* This is a variation on the Adamic/Adair method of similarity
		 * as described in Liben-Nowell, 2007. Followees of
		 * distinguished_user (gamma(x)) intersection with followers of
		 * current_user (gamma(y))
		 * 
		 * gamma(z) = followers user z
		 */

		double maxScore = 0;
		
		HashMap<String,Double> scoreBatch = new HashMap<String,Double>(DatabaseInterface.MAX_DATABASE_BATCH_SIZE);

		while (null != (userName = database.getNextNameFromCursor())) {
			if (userName.equals(distinguishedUser)) {
				continue;
			}

			//Gets the follower counts of the overlap between the distinguished user's followees and this user's followers
			List<Integer> overlap = database.getFollowerCountsOfOverlappingUserSet(distinguishedUser, userName);
			if (overlap == null) {
				scoreBatch.put(userName, Double.valueOf(0));
				if (scoreBatch.size() > DatabaseInterface.MAX_DATABASE_BATCH_SIZE) {
					database.setOriginalUserScoreBatch(scoreBatch);
					scoreBatch.clear();
				}
				continue;
			}

			double score = 0;

			for (Integer i : overlap) {
				score += 1 / Math.log10(i.intValue());
			}

			scoreBatch.put(userName, Double.valueOf(score));
			if (scoreBatch.size() > DatabaseInterface.MAX_DATABASE_BATCH_SIZE) {
				database.setOriginalUserScoreBatch(scoreBatch);
				scoreBatch.clear();
			}

			if (score > maxScore) {
				maxScore = score;
			}
		}

		scoreBatch.put(distinguishedUser, Double.valueOf(maxScore * MAX_USER_SCORE_MULTIPLIER));
		database.setOriginalUserScoreBatch(scoreBatch);

		//The values will be normalized elsewhere
	}

	/*
	 * 
	 * Code for initializing tweet and user scores
	 * 
	 */
	
	
	private void initializeTweetScores() {

		/* If the distinguished user has retweeted anything, then those people are most
		 * indicative of content that he likes, so we combine their tweets as a reference
		 * document against which all tweets have their similarity compared. If not, then
		 * we'll just use the tweets of everyone that the distinguished user follows.
		 */

		database.acquireCursorForTweets(distinguishedUser);

		String combinedTweet = "";
		String tweet = null;

		while (null != (tweet = database.getNextTweetFromCursor())) {
			combinedTweet += " " + tweet;
		}

		Set<String> retweetedUsers = Tweet.findRetweetedUsers(combinedTweet);

		if (retweetedUsers.size() > 5) {
			//Our combined document for comparison will be all of these people's tweets, if there are enough of them
			database.acquireCursorForTweetsOfUsers(retweetedUsers);
		}
		else {
			database.acquireCursorForAllFollowerEdgeTweets(distinguishedUser, Edge.Types.EDGE_TYPE_FOLLOWER.id());
		}

		combinedTweet = "";
		tweet = null;
		while (null != (tweet = database.getNextTweetFromCursor())) {
			combinedTweet += " " + tweet;
		}
		
		if (combinedTweet.equals("")) {
			database.acquireCursorForTweets(distinguishedUser);
			while (null != (tweet = database.getNextTweetFromCursor())) {
				combinedTweet += " " + tweet;
			}
		}
		
		combinedTweet = combinedTweet.replaceAll("RT", "");
		combinedTweet = combinedTweet.replaceAll("rt", "");
		
		Document referenceDoc = new Document();
		referenceDoc.add(new Field("tweet", combinedTweet, Field.Store.YES, Field.Index.ANALYZED, TermVector.YES));

		//This is used only to get the term frequencies of the reference document
		Directory referenceIndex = new RAMDirectory();

		HashMap<Long,Double> scoreBatch = new HashMap<Long,Double>(DatabaseInterface.MAX_DATABASE_BATCH_SIZE);
		
		database.setTweetScoresAndOriginalScoresToZero();

		try {
			/* To ensure that stemming and stop words are identical to
			 * the main lucene index, we index the reference document in
			 * the same way, but in RAM and with only one document.
			 * 
			 * This is used purely to get the term frequencies using Lucene.
			 */
			IndexWriter writer = LuceneIndexManager.getIndexWriter(referenceIndex);
			writer.addDocument(referenceDoc);
			writer.close();

			indexMgr.createDocumentFrequency();

			String[] terms;
			int[] termFreqs;

			IndexReader referenceIndexReader = IndexReader.open(referenceIndex);
			TermFreqVector tf = referenceIndexReader.getTermFreqVector(referenceIndexReader.maxDoc() - 1, "tweet");

			terms = tf.getTerms();
			termFreqs = tf.getTermFrequencies();

			/* Note that we use the main index's document frequencies, which are
			 * valid for all terms of the reference document since the tweets
			 * which make up the reference document are contained within the
			 * main index. 
			 */
			DocVector referenceDocVector = new DocVector(terms,termFreqs,indexMgr.getDocumentFrequency(), database.getNumTweetVertices());

			for (int ii = 0 ; ii < indexMgr.reader.maxDoc(); ii++) {
				if (indexMgr.reader.isDeleted(ii)) {
					continue;
				}

				if (null == (tf = indexMgr.reader.getTermFreqVector(ii,"tweet"))) {
					continue;
				}

				terms = tf.getTerms();
				termFreqs = tf.getTermFrequencies();

				DocVector docVector = new DocVector(terms, termFreqs, indexMgr.getDocumentFrequency(), database.getNumTweetVertices());

				double similarity = docVector.cosineSimilarity(referenceDocVector);

				Document doc = indexMgr.reader.document(ii);
				Long tweetId = Long.valueOf(doc.get("tweetid"));

				scoreBatch.put(tweetId, Double.valueOf(similarity));
				if (scoreBatch.size() > DatabaseInterface.MAX_DATABASE_BATCH_SIZE) {
					database.setOriginalTweetScoreBatch(scoreBatch);
					scoreBatch.clear();
				}
			}
			
			if (!scoreBatch.isEmpty()) {
				database.setOriginalTweetScoreBatch(scoreBatch);
				scoreBatch.clear();
			}

			referenceIndexReader.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Couldn't initialize tweet scores!");
		}
	}
	
	/*
	 * 
	 * Code for running the actual algorithm
	 * 
	 */
	

	public void runAlgorithm() {
		System.out.println("Running the Co-HITS algorithm!");

		if (!database.originalUserScoreCalculated()) {
			initializeUserScores();
			database.normalizeUserScores();
		}

		if (!database.originalTweetScoreCalculated()) {
			initializeTweetScores();
			database.normalizeTweetScores();
		}

		//Sets score to original_score for doing multiple runs in a row
		database.resetScores();

		int iterations = 0;

		Date d1 = new Date();
		Date d2 = new Date();

		do {
			System.out.println("Iteration #" + (iterations + 1));

			updateTweetScores();
			updateUserScores();
		} while (++iterations < MAX_ITERATIONS);//*/

		d2 = new Date();

		System.out.println("Finished running Co-HITS after " + (iterations + 1) + " iterations");
		System.out.println("Started at " + d1 + " and finished at " + d2);
		System.out.println("Roughly " + ((float)(d2.getTime() - d1.getTime()) / 1000.0) + " seconds");
		System.out.println("LambdaTweets was " + lambdaTweets + " and lambdaUsers was " + lambdaUsers);
	}

	/*
	 * 
	 * Code for adding edges
	 * 
	 */
	
	private void addEdge(String userName, long tweetId, Edge.Types type, boolean createUserIfNeeded) {
		if (userName == null) {
			return;
		}

		if (createUserIfNeeded && !usersInTweets.contains(userName)) {
			usersInTweets.add(userName);

			userBatch.add(userName);
			if (userBatch.size() > DatabaseInterface.MAX_DATABASE_BATCH_SIZE) {
				database.addUserBatch(userBatch);
				userBatch.clear();
			}
		}

		edgeBatch.add(new Edge(userName, tweetId, type.id()));
		if (edgeBatch.size() > DatabaseInterface.MAX_DATABASE_BATCH_SIZE) {
			database.addEdgeBatch(edgeBatch);
			edgeBatch.clear();
		}
	}
	
	
	/*
	 * 
	 * Implementation for the tweets parser which is used to create
	 * the edges.
	 * 
	 */

	private StanfordTweetIndexer.StanfordParser tweetsParser = new StanfordTweetIndexer.StanfordParser() {

		@Override
		boolean tweetHandler(Tweet tweet) {
			
			if ((tweet.user.length() >= 32) || (tweet.tweet.length() >= 255)) {
				return false;
			}
			
			//addEdge creates the user if needed
			addEdge(tweet.getUser(),tweet.id, Edge.Types.EDGE_TYPE_AUTHORSHIP, true);

			Set<String> retweets = createBasicRetweetEdges(tweet.getTweet(), tweet.id);
			createBasicAtReplyEdge(tweet.getTweet(), tweet.id);
			createBasicMentionEdges(tweet.getTweet(), tweet.id, retweets);

			tweetBatch.add(tweet);
			if (tweetBatch.size() > DatabaseInterface.MAX_DATABASE_BATCH_SIZE) {
				database.addTweetBatch(tweetBatch);
				tweetBatch.clear();
			}

			return true;
		}
	};
}

