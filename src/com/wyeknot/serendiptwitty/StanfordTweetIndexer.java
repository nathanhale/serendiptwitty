package com.wyeknot.serendiptwitty;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;


public class StanfordTweetIndexer {

	private static final String USER_PREFIX = "U";
	private static final String TIMESTAMP_PREFIX = "T";
	private static final String TWEET_PREFIX = "W";
	
	public static final String STANFORD_TWEETS_FILENAME = "tweets2009-12-pt1.txt";

	public static final String TWEET_NOT_AVAILABLE_STRING = "No Post Title";

	public static final long MAXIMUM_USER_ID = 61578414;
	public static final long TWEET_NUMBER_BASE = 0;

	static String getUserFromAddress(String address) {
		String[] parts = address.split("/");
		if (parts.length < 3) {
			return null;
		}
		else {
			return parts[parts.length - 1];
		}
	}

	public static abstract class StanfordParser {
		public static final int ALL_TWEETS_VAL = -2;

		abstract boolean tweetHandler(Tweet tweet);
		
		private long curTweetId = StanfordTweetIndexer.TWEET_NUMBER_BASE + 1;

		public void indexTweets(String path, int numTweetsRemaining) {
			System.out.println("indexing tweets");

			try {
				FileInputStream fileStream = new FileInputStream(path + STANFORD_TWEETS_FILENAME);
				InputStreamReader in = new InputStreamReader(fileStream, "UTF-8");

				BufferedReader reader = new BufferedReader(in);
				String line = null;

				Tweet tweet = new Tweet(curTweetId);

				boolean keepIndexing = true;

				while (((line = reader.readLine()) != null) && keepIndexing) {
					if (tweet.isComplete()) {

						boolean tweetHandled = tweetHandler(tweet);

						if (numTweetsRemaining != ALL_TWEETS_VAL) {

							if (tweetHandled) {
								//Could combine numTweetsRemaining and curTweetId, obviously, but this is more clear
								numTweetsRemaining--;
								curTweetId++;
								if ((numTweetsRemaining % 500) == 0) {
									System.out.println("Just indexed a tweet with " + numTweetsRemaining + " remaining: " + tweet);
								}

								if (numTweetsRemaining == 0) {
									keepIndexing = false;
								}
							}
						}

						tweet = new Tweet(curTweetId);
					}

					String[] parts = line.split("\t");

					if (parts.length < 2) {
						continue;
					}

					if (parts[0].equals(USER_PREFIX)) {
						String user = getUserFromAddress(parts[1]);

						if (null == user) {
							System.err.println("got a null user: " + parts[1]);
						}

						tweet.setUser(user);
					} else if (parts[0].equals(TWEET_PREFIX)) {

						//This works on the premise that the tweet is always last, which probably isn't especially good to rely on
						if (parts[1].equals(TWEET_NOT_AVAILABLE_STRING)) {
							tweet = new Tweet(curTweetId);
							continue;
						}

						tweet.setTweet(parts[1]);
					} else if (parts[0].equals(TIMESTAMP_PREFIX)) {
						tweet.setTimestampString(parts[1]);
					} else {
						continue;
					}
				}

				reader.close();
				in.close();
				fileStream.close();

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
