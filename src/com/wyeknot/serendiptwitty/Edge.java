package com.wyeknot.serendiptwitty;

public class Edge {
	
	public enum Types {
		//Connects a tweet to the user who created it
		
		EDGE_TYPE_AUTHORSHIP (1, 1, true, true),
		//Connects a tweet to all of the followers of the tweeter
		EDGE_TYPE_FOLLOWER   (2, 1, true, false),
		//Connects the retweet to the original tweeter
		EDGE_TYPE_RETWEET    (3, 1, false, false),
		//Connects the tweets of the followees of the person being retweeted to the retweeter
		EDGE_TYPE_RETWEET_FOLLOWEES (4, 1, true, false),
		//Connects the tweets of the person being retweeted to the followers of the retweeter
		EDGE_TYPE_RETWEET_FOLLOWERS (5, 1, true, false),
		//Connects the tweet to the person being mentioned
		EDGE_TYPE_MENTION (6, 1, false, false),
		//Connects the tweets of the followees of the person being mentioned to the mentioner
		EDGE_TYPE_MENTION_FOLLOWEES (7, 1, true, false),
		//Connects the tweets of the person being mentioned to the followers of the mentioner
		EDGE_TYPE_MENTION_FOLLOWERS (8, 1, true, false),
		//Connects the tweet to the person being @replied to
		EDGE_TYPE_AT_REPLY (9, 1, false, false),
		//Connects the tweets of the person being @replied to to the tweeter
		EDGE_TYPE_AT_REPLY_CONTENT (10, 1, true, false),
		EDGE_TYPE_HASHTAG (11, 1, true, true),
		EDGE_TYPE_CONTENT (12, 1, true, true);

		private final int id;
		private final double probability;
		private final boolean tweetToUserDir;
		private final boolean userToTweetDir;

		private Types(int id, double probability, boolean tweetToUser, boolean userToTweet) {
			this.id = id;
			this.probability = probability;
			this.tweetToUserDir = tweetToUser;
			this.userToTweetDir = userToTweet;
		}

		public int id() { return id; }
		public double probability() { return probability; }
		public boolean tweetToUserDir() { return tweetToUserDir; }
		public boolean userToTweetDir() { return userToTweetDir; }
	}
	
	public static final Types[] idToEdgeType = {
		null,
		Types.EDGE_TYPE_AUTHORSHIP,
		Types.EDGE_TYPE_FOLLOWER,
		Types.EDGE_TYPE_RETWEET,
		Types.EDGE_TYPE_RETWEET_FOLLOWEES,
		Types.EDGE_TYPE_RETWEET_FOLLOWERS,
		Types.EDGE_TYPE_MENTION,
		Types.EDGE_TYPE_MENTION_FOLLOWEES,
		Types.EDGE_TYPE_MENTION_FOLLOWERS,
		Types.EDGE_TYPE_AT_REPLY,
		Types.EDGE_TYPE_AT_REPLY_CONTENT,
		Types.EDGE_TYPE_HASHTAG,
		Types.EDGE_TYPE_CONTENT
	};
	
	
	public String name;
	public long tweet;
	public int type;

	Edge(String name, long tweet, int type) {
		this.name = name;
		this.tweet = tweet;
		this.type = type;
	}
}
