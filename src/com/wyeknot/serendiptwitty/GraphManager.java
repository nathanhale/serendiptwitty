package com.wyeknot.serendiptwitty;


import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

	//TODO: try this with some of these edges being directed

	public enum EdgeTypes {
		//Connects a tweet to the user who created it
		EDGE_TYPE_AUTHORSHIP (1),//, (1.0 / 24)),
		//Connects a tweet to all of the followers of the tweeter
		EDGE_TYPE_FOLLOWER   (2),//, (1.0 / 24)),
		//Connects the retweet to the original tweeter
		EDGE_TYPE_RETWEET    (3),//, (1.0 / 24)),
		//Connects the tweets of the followees of the person being retweeted to the retweeter
		EDGE_TYPE_RETWEET_FOLLOWEES (4),//, (3.0 / 24)),
		//Connects the tweets of the person being retweeted to the followers of the retweeter
		EDGE_TYPE_RETWEET_FOLLOWERS (5),//, (3.0 / 24)),
		//Connects the tweet to the person being mentioned
		EDGE_TYPE_MENTION (6),//, (1.0 / 24)),
		//Connects the tweets of the followees of the person being mentioned to the mentioner
		EDGE_TYPE_MENTION_FOLLOWEES (7),//, (3.0 / 24)),
		//Connects the tweets of the person being mentioned to the followers of the mentioner
		EDGE_TYPE_MENTION_FOLLOWERS (8),//, (3.0 / 24)),
		//Connects the tweet to the person being @replied to
		EDGE_TYPE_AT_REPLY (9),//, (1.0 / 24)),
		//Connects the tweets of the person being @replied to to the tweeter
		EDGE_TYPE_AT_REPLY_CONTENT (10),//, (1.0 / 24)),
		EDGE_TYPE_HASHTAG (11),//, (3.0 / 24)),
		EDGE_TYPE_CONTENT (12);//, (3.0 / 24));
		/*
		 * If another type is added after content, then numTypes must be updated accordingly! 
		 */

		private final int id;
		//private final double probability;
		//private static final int numTypes = EDGE_TYPE_CONTENT.id;

		private EdgeTypes(int id) {//, double probability) {
			this.id = id;
			//this.probability = probability;
		}

		public int id() { return id; }
		//public double probability() { return probability; }
	}

	/*private EdgeTypes[] idToEdgeType = {
			null,
			EdgeTypes.EDGE_TYPE_AUTHORSHIP,
			EdgeTypes.EDGE_TYPE_FOLLOWER,
			EdgeTypes.EDGE_TYPE_RETWEET,
			EdgeTypes.EDGE_TYPE_RETWEET_FOLLOWEES,
			EdgeTypes.EDGE_TYPE_RETWEET_FOLLOWERS,
			EdgeTypes.EDGE_TYPE_MENTION,
			EdgeTypes.EDGE_TYPE_MENTION_FOLLOWEES,
			EdgeTypes.EDGE_TYPE_MENTION_FOLLOWERS,
			EdgeTypes.EDGE_TYPE_AT_REPLY,
			EdgeTypes.EDGE_TYPE_AT_REPLY_CONTENT,
			EdgeTypes.EDGE_TYPE_HASHTAG,
			EdgeTypes.EDGE_TYPE_CONTENT
	};*/

	private static class Pair<F, S> {
		private F first;
		private S second;

		public Pair(F f, S s){
			this.first = f;
			this.second = s;
		}

		public F getFirst() { return first; }
		public S getSecond() { return second; }
	}

	public static class Edge {
		public String name;
		public long tweet;
		public int type;

		Edge(String name, long tweet, int type) {
			this.name = name;
			this.tweet = tweet;
			this.type = type;
		}
	}

	private final Pattern hashTagRegExPattern = Pattern.compile(" #[a-zA-Z0-9]*[a-zA-Z]");

	private final Pattern atReplyRegExPattern = Pattern.compile("^@[a-zA-Z][a-zA-Z0-9_]*");
	private final Pattern mentionRegExPattern = Pattern.compile("@[a-zA-Z][a-zA-Z0-9_]*");
	private final Pattern rtRegExPattern = Pattern.compile("RT @[a-zA-Z][a-zA-Z0-9_]*");
	private final Pattern rtRegExPattern2  = Pattern.compile("RT@[a-zA-Z][a-zA-Z0-9_]*");
	private final Pattern rtRegExPattern3  = Pattern.compile("RT: @[a-zA-Z][a-zA-Z0-9_]*");
	private final Pattern rtRegExPattern4  = Pattern.compile("via @[a-zA-Z][a-zA-Z0-9_]*");
	//This is the starting point of the actual name in each of the RT regex patterns
	private final int rtPatternNameStart = 4;
	private final int rtPattern2NameStart = 3;
	private final int rtPattern3NameStart = 5;
	private final int rtPattern4NameStart = 5;

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

	private static final int MAX_ITERATIONS = 15;

	//Values closer to 0 put more weight on the original score
	private static final double lambdaUsers = 0.85;
	private static final double lambdaTweets = 0.9;

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


	public void createGraph() {
		if (!database.tableHasRows("edges")) {

			usersInTweets.add(distinguishedUser);
			userBatch.add(distinguishedUser);

			for (String user : otherDistinguishedUsers) {
				user = user.toLowerCase();
				userBatch.add(user);
				usersInTweets.add(user);
			}

			Date d1 = new Date();
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
			
			//TODO: do the stuff from edgesCopy here

			Date d2 = new Date();

			System.out.println("Started creating the graph at " + d1 + " and finished at " + d2);
			System.out.println("Roughly " + ((float)(d2.getTime() - d1.getTime()) / 1000.0) + " seconds");

			//Could create the indexes on the edges in the code here, or could do it when the tables are created
		}
	}

	private void createFollowerRetweetAndMentionEdges() {
		System.out.println("create Follower, Retweet, And Mention Edges!");

		database.acquireCursorForTweetVertices();

		String tweet = null;

		Date d1 = new Date();
		System.out.println("Start at " + d1);

		int numTweets = 0;

		List<Long> curUserTweetIds = new ArrayList<Long>();
		String lastUser = null;

		while (null != (tweet = database.getNextTweetFromCursor())) {

			numTweets++;

			String tweeter = database.getNameFromCurrentCursorPos();
			long tweetId = database.getTweetIdFromCurrentCursorPos();

			if ((numTweets % 100) == 0) {
				Date d2 = new Date();
				System.out.print("tweet " + numTweets + " by " + tweeter);
				System.out.println("  ~" + ((float)(d2.getTime() - d1.getTime()) / 1000.0) + " seconds");
				System.out.flush();
			}

			if (!tweeter.equals(lastUser) && (lastUser != null)) {
				createFollowerEdgesForUser(lastUser,curUserTweetIds);
				curUserTweetIds.clear();
			}

			lastUser = tweeter;
			curUserTweetIds.add(Long.valueOf(tweetId));

			Set<String> retweets = findRetweetedUsers(tweet);
			String atReply = findAtReply(tweet);
			Set<String> mentions = findMentionedUsers(tweet, retweets);

			createRetweetEdgesForTweet(tweeter, tweetId, retweets);

			createMentionEdgesForTweet(tweeter, tweetId, mentions);

			if (null != atReply) {
				database.acquireInternalCursorForTweets(atReply);
				long internalTweetId = -1;
				while (-1 != (internalTweetId = database.getNextTweetIdFromInternalCursor())) {
					addEdge(tweeter, internalTweetId, EdgeTypes.EDGE_TYPE_AT_REPLY_CONTENT, false);
				}
			}
		}

		Date d2 = new Date();
		System.out.println("Finished creating Follower, Retweet, And Mention Edges!");
		System.out.println("Started at " + d1 + " and finished at " + d2);
		System.out.println("Roughly " + ((float)(d2.getTime() - d1.getTime()) / 1000.0) + " seconds");
	}

	private void createFollowerEdgesForUser(String user, List<Long> curUserTweetIds) {		
		database.acquireInternalCursorForFollowersInUserVertices(user);

		String internalUser = null;
		while (null != (internalUser = database.getNextNameFromInternalCursor())) {
			for (Long id : curUserTweetIds) {
				addEdge(internalUser, id.longValue(), EdgeTypes.EDGE_TYPE_FOLLOWER, false);
			}
		}
	}

	private void createRetweetEdgesForTweet(String tweeter, long tweetId, Set<String> retweets) {
		for (String retweetee : retweets) {
			//Connects the tweets of the followees of the person being retweeted to the retweeter
			database.acquireInternalCursorForFolloweesEdges(retweetee, tweeter,
					EdgeTypes.EDGE_TYPE_AUTHORSHIP.id(), EdgeTypes.EDGE_TYPE_RETWEET_FOLLOWEES.id());

			long internalTweetId = -1;
			while (-1 != (internalTweetId = database.getNextTweetIdFromInternalCursor())) {
				addEdge(tweeter, internalTweetId, EdgeTypes.EDGE_TYPE_RETWEET_FOLLOWEES, false);
			}

			//Connects the tweets of the person being retweeted to the followers of the retweeter				
			database.acquireInternalCursorForFollowersEdges(retweetee, tweeter,
					EdgeTypes.EDGE_TYPE_AUTHORSHIP.id(), EdgeTypes.EDGE_TYPE_RETWEET_FOLLOWERS.id());
			internalTweetId = -1;

			while (-1 != (internalTweetId = database.getNextTweetIdFromInternalCursor())) {
				String internalUserName = database.getNameFromCurrentInternalCursorPos();
				addEdge(internalUserName, internalTweetId, EdgeTypes.EDGE_TYPE_RETWEET_FOLLOWERS, false);	
			}
		}
	}

	private void createMentionEdgesForTweet(String tweeter, long tweetId, Set<String> mentions) {
		for (String mentionee : mentions) {
			//Connects the tweets of the followees of the person being mentioned to the mentioner
			database.acquireInternalCursorForFolloweesEdges(mentionee, tweeter,
					EdgeTypes.EDGE_TYPE_AUTHORSHIP.id(), EdgeTypes.EDGE_TYPE_MENTION_FOLLOWEES.id());


			long internalTweetId = -1;
			while (-1 != (internalTweetId = database.getNextTweetIdFromInternalCursor())) {
				addEdge(tweeter, internalTweetId, EdgeTypes.EDGE_TYPE_MENTION_FOLLOWEES, false);
			}

			//Connects the tweets of the person being mentioned to the followers of the mentioner				
			database.acquireInternalCursorForFollowersEdges(mentionee, tweeter,
					EdgeTypes.EDGE_TYPE_AUTHORSHIP.id(), EdgeTypes.EDGE_TYPE_MENTION_FOLLOWERS.id());
			internalTweetId = -1;

			while (-1 != (internalTweetId = database.getNextTweetIdFromInternalCursor())) {
				String internalUserName = database.getNameFromCurrentInternalCursorPos();
				addEdge(internalUserName, internalTweetId, EdgeTypes.EDGE_TYPE_MENTION_FOLLOWERS, false);	
			}
		}
	}

	private Set<String> findRetweetedUsers(String tweet) {
		Set<String> users = new HashSet<String>();

		Matcher m = rtRegExPattern.matcher(tweet);
		while (m.find()) {
			String match = m.group();
			users.add(match.substring(rtPatternNameStart).toLowerCase());
		}

		m = rtRegExPattern2.matcher(tweet);
		while (m.find()) {
			String match = m.group();
			users.add(match.substring(rtPattern2NameStart).toLowerCase());
		}

		m = rtRegExPattern3.matcher(tweet);
		while (m.find()) {
			String match = m.group();
			users.add(match.substring(rtPattern3NameStart).toLowerCase());
		}

		m = rtRegExPattern4.matcher(tweet);
		while (m.find()) {
			String match = m.group();
			users.add(match.substring(rtPattern4NameStart).toLowerCase());
		}

		return users;
	}

	private String findAtReply(String tweet) {
		Matcher m = atReplyRegExPattern.matcher(tweet);

		while (m.find()) {
			return m.group().substring(1).toLowerCase();
		}

		return null;
	}

	private Set<String> findMentionedUsers(String tweet, Set<String> retweetedUsers) {
		Set<String> users = new HashSet<String>();

		Matcher m = mentionRegExPattern.matcher(tweet);

		while (m.find()) {
			String match = m.group();
			String user = match.substring(1).toLowerCase();

			//this is the old trick to not match an RT -- search for RTs first and remove them
			//tweet = tweet.replace(match, " ");

			if (m.start() == 0) {
				//This is an @reply -- ignore it here
			}
			else if (!retweetedUsers.contains(user)) {
				users.add(user);
			}
		}

		return users;
	}

	private void createContentEdges() {
		String serializedClassifier = Recommender.nerClassifiersPath + "english.all.3class.distsim.crf.ser.gz";

		@SuppressWarnings("unchecked")
		AbstractSequenceClassifier<CoreLabel> classifier = CRFClassifier.getClassifierNoExceptions(serializedClassifier);

		/*
		 * These HashMaps have the entity/hashtag as the key, and then for each hashtag there
		 * is a List of tweetId/author pairs and a set of authors who have used this entity/hashtag.
		 * 
		 *  We need to keep track of the tweet id AND the author of each tweet because we don't
		 *  want to create a link between the author's own tweet and their vertex.
		 *  
		 *  Note: If we have a set of tweetids instead of a list then we can prevent
		 *  duplicate edges, but I think that the duplicate edges add extra value, so I don't
		 *  plan to implement it that way.
		 */		
		HashMap<String , Pair< List<Pair<Long,String>> , Set<String> > > entities =
				new HashMap<String,Pair<List<Pair<Long,String>>,Set<String>>>();
		HashMap<String , Pair< List<Pair<Long,String>> , Set<String> > > hashtags =
				new HashMap<String,Pair<List<Pair<Long,String>>,Set<String>>>();

		database.acquireCursorForTweetVertices();

		String tweet = null;
		
		int numTweets = 0;
		Date d1 = new Date();

		System.out.println("Start finding content edges at " + d1);
		
		while (null != (tweet = database.getNextTweetFromCursor())) {
			String tweeter = database.getNameFromCurrentCursorPos();
			Long tweetId = Long.valueOf(database.getTweetIdFromCurrentCursorPos());
			
			if ((++numTweets % 100) == 0) {
				Date d2 = new Date();
				System.out.println("Detecting entities for tweet " + numTweets + " tweetId is " + tweetId + " -- " + d2);
				System.out.println("Roughly " + ((float)(d2.getTime() - d1.getTime()) / 1000.0) + " seconds so far");
			}

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

					/* Can see if this words .beginPosition is one above the last word's .endPosition
					 * to ignore spaces. For now I'm not going to. This would eliminate some matches
					 */
				}
			}

			Set<String> hashtagsInTweet = findHashTags(tweet);
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
		
		System.out.println("There were " + entities.size() + " entities");

		for (Pair<List<Pair<Long,String>>,Set<String>> edges : entities.values()) {
			List<Pair<Long,String>> tweetIds = edges.getFirst();
			Set<String> authors = edges.getSecond();


			for (Pair<Long,String> tweetIdAndAuthor : tweetIds) {
				for (String author : authors) {
					if (!author.equals(tweetIdAndAuthor.getSecond())) {
						addEdge(author, tweetIdAndAuthor.getFirst(), EdgeTypes.EDGE_TYPE_CONTENT, false);
					}
				}
			}
		}
		
		System.out.println("There were " + hashtags.size() + " hashtags");

		for (Pair<List<Pair<Long,String>>,Set<String>> edges : hashtags.values()) {
			List<Pair<Long,String>> tweetIds = edges.getFirst();
			Set<String> authors = edges.getSecond();

			for (Pair<Long,String> tweetIdAndAuthor : tweetIds) {
				for (String author : authors) {
					if (!author.equals(tweetIdAndAuthor.getSecond())) {
						addEdge(author, tweetIdAndAuthor.getFirst(), EdgeTypes.EDGE_TYPE_HASHTAG, false);
					}
				}
			}
		}
		
		Date d2 = new Date();
		System.out.println("Finished adding content edges at " + d2 + "\nRoughly " + ((float)(d2.getTime() - d1.getTime()) / 1000.0) + " seconds passed in that time");
	}

	Set<String> createBasicRetweetEdges(String tweet, long curTweetId) {
		Set<String> retweets = findRetweetedUsers(tweet);
		for (String s : retweets) {
			addEdge(s, curTweetId, EdgeTypes.EDGE_TYPE_RETWEET, true);
		}

		return retweets;
	}

	void createBasicAtReplyEdge(String tweet, long curTweetId) {
		String atReply = findAtReply(tweet);
		if (null != atReply) {
			addEdge(atReply, curTweetId, EdgeTypes.EDGE_TYPE_AT_REPLY, true);
		}
	}

	void createBasicMentionEdges(String tweet, long curTweetId, Set<String> retweetNames) {
		Set<String> mentions = findMentionedUsers(tweet, retweetNames);
		for (String s : mentions) {
			addEdge(s, curTweetId, EdgeTypes.EDGE_TYPE_MENTION, true);
		}
	}

	private Set<String> findHashTags(String tweet) {
		Matcher m = hashTagRegExPattern.matcher(tweet);

		HashSet<String> hashes = new HashSet<String>();

		while (m.find()) {
			hashes.add(m.group().substring(2).toLowerCase());
		}

		return hashes;
	}

	//TODO: add comment separators to delineate which parts of the code are for which purpose

	private void updateTweetsFromUser(String userName, double userScore, List<Pair<Integer,Long>> edgeTypeAndDest,
			Map<Long,Double> updatedTweetScores) {

		//Can this be done more efficiently? I can think of a way in C, but it still requires iterating -- think on it
		//This applies to the tweet version as well
		/*int[] numEdgesPerType = new int[EdgeTypes.numTypes];

		for (Pair<Integer,Long> t : edgeTypeAndDest) {
			//Remember, edgeTypes are 1-based, but our array is zero-based
			numEdgesPerType[t.getFirst() - 1]++;
		}*/

		for (Pair<Integer,Long> t : edgeTypeAndDest) {

			//int numEdgesForThisType = numEdgesPerType[t.getFirst() - 1];

			//double chanceOfGoingToTweet = (1.0 / numEdgesForThisType) * idToEdgeType[t.getFirst()].probability();
			double chanceOfGoingToTweet = (1.0 / edgeTypeAndDest.size());
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

		//double doppelgangerScore = 0;

		/*for (EdgeTypes t :  EdgeTypes.values()) {
			if (numEdgesPerType[t.id() - 1] == 0) {
				doppelgangerScore += lambdaTweets * t.probability() * userScore;
			}
		}

		database.updateUserDoppelgangerScore(userName, doppelgangerScore);*/
	}

	private void updateUsersFromTweet(long tweetId, double tweetScore, List<Pair<Integer,String>> edgeTypeAndDest,
			Map<String,Double> updatedUserScores) {

		/* Each edge type is visited with a particular probability. */ 
		/*int[] numEdgesPerType = new int[EdgeTypes.numTypes];

		for (Pair<Integer,String> t : edgeTypeAndDest) {
			//Remember, edgeTypes are 1-based, but our array is zero-based
			numEdgesPerType[t.getFirst() - 1]++;
		}*/

		for (Pair<Integer,String> t : edgeTypeAndDest) {

			//int numEdgesForThisType = numEdgesPerType[t.getFirst() - 1];

			//double chanceOfGoingToUser = (1.0 / numEdgesForThisType) * idToEdgeType[t.getFirst()].probability();
			double chanceOfGoingToUser = 1.0 / edgeTypeAndDest.size();
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

		/*double doppelgangerScore = 0;

		for (EdgeTypes t :  EdgeTypes.values()) {
			if (numEdgesPerType[t.id() - 1] == 0) {
				doppelgangerScore += lambdaUsers * t.probability() * tweetScore;
			}
		}

		database.updateTweetDoppelgangerScore(tweetId, doppelgangerScore);*/
	}

	private int updateTweetScores() {
		
		Date d1 = new Date();
		System.out.println("acquireCursorForUpdating Tweet Scores at " + d1);
		database.acquireCursorForUpdatingTweetScores();
		Date d2 = new Date();
		System.out.print("acquired cursor at " + d2 + " after roughly " + ((float)(d2.getTime() - d1.getTime()) / 1000.0) + " seconds so far");

		long tweetId = -1;

		String userName = null;
		String lastUserName = null;
		double lastUserScore = -1;

		//Holds a record of the scores of all the tweets that we've updated the score for
		Map<Long,Double> updatedTweetScores = new HashMap<Long,Double>();

		List<Pair<Integer,Long>> currentUserEdgeTypesAndDestinations = new ArrayList<Pair<Integer,Long>>();

		int numEdges = 0;
		
		while (null != (userName = database.getNextNameFromCursor())) {

			if ((numEdges % 500000) == 0) {
				d2 = new Date();
				System.out.print("examining an edge from user " + userName + " after roughly " + ((float)(d2.getTime() - d1.getTime()) / 1000.0) + " seconds so far");
				System.out.println(" -- tweetScoresSize is " + updatedTweetScores.size());
			}
			
			if (!userName.equals(lastUserName) && (lastUserName != null)) {
				updateTweetsFromUser(lastUserName, lastUserScore, currentUserEdgeTypesAndDestinations, updatedTweetScores);
				currentUserEdgeTypesAndDestinations.clear();
			}

			tweetId = database.getTweetIdFromCurrentCursorPos();
			if (-1 == tweetId) {
				throw new RuntimeException("Error retrieving tweetId while calculating tweet scores!");
			}

			currentUserEdgeTypesAndDestinations.add(new Pair<Integer,Long>(
					new Integer(database.getEdgeTypeFromCurrentCursorPos()),
					Long.valueOf(tweetId)));

			lastUserName = userName;
			lastUserScore = database.getUserScoreFromCurrentCursorPos();
			
			numEdges++;
		}

		//And update the last tweet
		updateTweetsFromUser(lastUserName, lastUserScore, currentUserEdgeTypesAndDestinations, updatedTweetScores);

		return database.updateTweetScores(updatedTweetScores, lambdaTweets);
	}

	private int updateUserScores() {
		Date d1 = new Date();
		System.out.println("acquireCursorForUpdating User Scores at " + d1);
		database.acquireCursorForUpdatingUserScores();
		Date d2 = new Date();
		System.out.print("acquired cursor at " + d2 + " after roughly " + ((float)(d2.getTime() - d1.getTime()) / 1000.0) + " seconds so far");

		String userName = null;

		long tweetId = -1;
		long lastTweetId = -1;
		double lastTweetScore = -1;

		//Holds a record of the scores of all the users that we've updated the score for
		Map<String,Double> updatedUserScores = new HashMap<String,Double>();

		List<Pair<Integer,String>> currentTweetEdgeTypesAndDestinations = new ArrayList<Pair<Integer,String>>();

		int numEdges = 0;
		
		while (-1 != (tweetId = database.getNextTweetIdFromCursor())) {

			
			if ((numEdges % 500000) == 0) {
				d2 = new Date();
				System.out.print("examining an edge from tweet " + tweetId + " after roughly " + ((float)(d2.getTime() - d1.getTime()) / 1000.0) + " seconds so far");
				System.out.println(" -- userScoresSize is " + updatedUserScores.size());
			}
			
			if ((lastTweetId != tweetId) && (lastTweetId != -1)) {
				updateUsersFromTweet(lastTweetId, lastTweetScore, currentTweetEdgeTypesAndDestinations, updatedUserScores);
				currentTweetEdgeTypesAndDestinations.clear();
			}

			userName = database.getNameFromCurrentCursorPos();
			if (null == userName) {
				throw new RuntimeException("Error retrieving userId while calculating user scores!");
			}

			currentTweetEdgeTypesAndDestinations.add(new Pair<Integer,String>(
					new Integer(database.getEdgeTypeFromCurrentCursorPos()),
					userName));

			lastTweetId = tweetId;
			lastTweetScore = database.getTweetScoreFromCurrentCursorPos();
		}

		//And update the last user
		updateUsersFromTweet(lastTweetId, lastTweetScore, currentTweetEdgeTypesAndDestinations, updatedUserScores);

		return database.updateUserScores(updatedUserScores, lambdaUsers);
	}

	//TODO: get rid of all of the timing printouts

	private void initializeUserScores() {

		System.out.println("Initializing scores... " + new Date());

		database.acquireCursorForInitializingUserScores();

		String userName = null;

		//This is a variation on the Adamic/Adair method of similarity as described in Liben-Nowell, 2007
		//Followees of distinguished_user (gamma(x)) intersection with followers of current_user (gamma(y))
		//  -- gamma(z) = followers user z

		int numUsers = 0;

		Date d1 = new Date();

		while (null != (userName = database.getNextNameFromCursor())) {
			if (userName.equals(distinguishedUser)) {
				continue;
			}

			//Gets the follower counts of the overlap between the distinguished user's followees and this user's followers
			List<Integer> overlap = database.getFollowerCountsOfOverlappingUserSet(distinguishedUser, userName);
			if (overlap == null) {
				database.setOriginalUserScore(userName, 0);
				continue;
			}

			double score = 0;

			for (Integer i : overlap) {
				//A future improvement might test to see if doing this without the log improves the results at all
				score += 1 / Math.log10(i.intValue());
			}

			if ((++numUsers % 100) == 0) {
				System.out.print("\tUser: " + numUsers + " (" + userName + "): ");
				Date d2 = new Date();
				System.out.print("roughly " + ((float)(d2.getTime() - d1.getTime()) / 1000.0) +
						" seconds  so far -- score for this user is ");
				System.out.println(score);
			}//*/


			database.setOriginalUserScore(userName, score);
		}

		Date d2 = new Date();
		System.out.println("\t\tRoughly " + ((float)(d2.getTime() - d1.getTime()) / 1000.0) + " seconds");
	}

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

		Set<String> retweetedUsers = findRetweetedUsers(combinedTweet);

		if (retweetedUsers.size() > 5) {
			//Our combined document for comparison will be all of these people's tweets, if there are enough of them
			database.acquireCursorForTweetsOfUsers(retweetedUsers);
		}
		else {
			Date d1 = new Date();
			System.out.println("Start acquiring cursor for all follower edge tweets at " + d1);
			database.acquireCursorForAllFollowerEdgeTweets(distinguishedUser, EdgeTypes.EDGE_TYPE_FOLLOWER.id());
			Date d2 = new Date();
			System.out.println("Finish acquiring cursor for all follower edge tweets at " + d2 + " -- Roughly " + ((float)(d2.getTime() - d1.getTime()) / 1000.0) + " seconds");
		}

		combinedTweet = "";
		tweet = null;
		while (null != (tweet = database.getNextTweetFromCursor())) {
			combinedTweet += " " + tweet;
		}
		
		//TODO: remove this
		/*
		if (combinedTweet.equals("")) {
			database.setTweetScoresToEven();
			return;
		}*/

		Document referenceDoc = new Document();
		referenceDoc.add(new Field("tweet", combinedTweet, Field.Store.YES, Field.Index.ANALYZED, TermVector.YES));

		combinedTweet = combinedTweet.replaceAll("RT", "");
		combinedTweet = combinedTweet.replaceAll("rt", "");

		Directory referenceIndex = new RAMDirectory();

		database.setTweetScoresToZero();

		try {
			/* To ensure that stemming and stop words are identical to the main lucene index, we index
			 * the reference document in the same way, but in RAM and with only one document.
			 */
			IndexWriter writer = LuceneIndexManager.getIndexWriter(referenceIndex);
			writer.addDocument(referenceDoc);
			writer.close();

			indexMgr.createTermFrequency();

			String[] terms;
			int[] termFreqs;

			IndexReader referenceIndexReader = IndexReader.open(referenceIndex);
			TermFreqVector tf = referenceIndexReader.getTermFreqVector(referenceIndexReader.maxDoc() - 1, "tweet");

			System.out.println("Reference doc is " + combinedTweet);
			
			terms = tf.getTerms();
			termFreqs = tf.getTermFrequencies();

			DocVector referenceDocVector = new DocVector(terms,termFreqs,indexMgr.getTermFrequency());
			
			for (int ii = 0 ; ii < indexMgr.reader.maxDoc(); ii++) {
				if (indexMgr.reader.isDeleted(ii)) {
					continue;
				}

				if (null == (tf = indexMgr.reader.getTermFreqVector(ii,"tweet"))) {
					continue;
				}

				terms = tf.getTerms();
				termFreqs = tf.getTermFrequencies();

				DocVector docVector = new DocVector(terms, termFreqs, indexMgr.getTermFrequency());

				double similarity = docVector.cosineSimilarity(referenceDocVector);

				Document doc = indexMgr.reader.document(ii);
				Long tweetId = Long.valueOf(doc.get("tweetid"));

				database.setOriginalTweetScore(tweetId.longValue(), similarity);
				
				if ((ii % 1000) == 0) {
					System.out.println("Initializing Tweet " + ii);
				}
			}

			referenceIndexReader.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Couldn't initialize tweet scores!");
		}
	}

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

		int tweetUpdates = -1;
		int userUpdates = -1;

		int iterations = 0;

		Date d1 = new Date();

		do {
			System.out.println("Iteration #" + (iterations + 1));

			tweetUpdates = updateTweetScores();
			userUpdates = updateUserScores();
		} while (((tweetUpdates != 0) || (userUpdates != 0)) && (++iterations < MAX_ITERATIONS));//*/

		Date d2 = new Date();

		System.out.println("Finished running Co-HITS after " + (iterations + 1) + " iterations");
		System.out.println("Started at " + d1 + " and finished at " + d2);
		System.out.println("Roughly " + ((float)(d2.getTime() - d1.getTime()) / 1000.0) + " seconds");
	}

	private void addEdge(String userName, long tweetId, EdgeTypes type, boolean createUserIfNeeded) {
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

	private StanfordTweetIndexer.StanfordParser tweetsParser = new StanfordTweetIndexer.StanfordParser() {

		@Override
		boolean tweetHandler(Tweet tweet) {

			//addEdge creates the user if needed
			addEdge(tweet.getUser(),tweet.id, EdgeTypes.EDGE_TYPE_AUTHORSHIP, true);

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

