package com.wyeknot.serendiptwitty;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tweet {
	long id;
	String user;
	Timestamp timestamp;
	String tweet;

	public static final SimpleDateFormat stanfordTweetDateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


	private static final Pattern hashTagRegExPattern = Pattern.compile(" #[a-zA-Z0-9]*[a-zA-Z]");

	private static final Pattern atReplyRegExPattern = Pattern.compile("^@[a-zA-Z][a-zA-Z0-9_]*");
	private static final Pattern mentionRegExPattern = Pattern.compile("@[a-zA-Z][a-zA-Z0-9_]*");
	private static final Pattern rtRegExPattern = Pattern.compile("RT @[a-zA-Z][a-zA-Z0-9_]*");
	private static final Pattern rtRegExPattern2  = Pattern.compile("RT@[a-zA-Z][a-zA-Z0-9_]*");
	private static final Pattern rtRegExPattern3  = Pattern.compile("RT: @[a-zA-Z][a-zA-Z0-9_]*");
	private static final Pattern rtRegExPattern4  = Pattern.compile("via @[a-zA-Z][a-zA-Z0-9_]*");
	//This is the starting point of the actual name in each of the RT regex patterns
	private static final int rtPatternNameStart = 4;
	private static final int rtPattern2NameStart = 3;
	private static final int rtPattern3NameStart = 5;
	private static final int rtPattern4NameStart = 5;

	
	
	public Tweet(long id) {
		this.id = id;
		user = null;
		timestamp = null;
		tweet = null;
	}
	
	public long getId() {
		return id;
	}
	
	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public void setTimestampString(String timestamp) {
		this.timestamp = getTimestampFromString(timestamp);
	}

	public static Timestamp getTimestampFromString(String timestamp) {
		try {
			return new Timestamp(stanfordTweetDateParser.parse(timestamp).getTime());
		} catch (ParseException e) {
			e.printStackTrace();
			System.err.println("\nCouldn't parse date " + timestamp + "!!!");
		}
		
		return null;
	}

	public String getTweet() {
		return tweet;
	}

	public void setTweet(String tweet) {
		this.tweet = tweet;
	}

	public boolean isComplete() {
		return ((null != user) && (timestamp != null) && (tweet != null));
	}

	public String toString() {
		return user + " on " + timestamp + ": " + tweet;
	}
	
	
	public static Set<String> findRetweetedUsers(String tweet) {
		Set<String> users = new HashSet<String>();

		Matcher m = rtRegExPattern.matcher(tweet);
		while (m.find()) {
			String match = m.group();
			String user = match.substring(rtPatternNameStart).toLowerCase();
			if (user.length() >= 32) {
				continue;
			}
			users.add(user);
		}

		m = rtRegExPattern2.matcher(tweet);
		while (m.find()) {
			String match = m.group();
			String user = match.substring(rtPattern2NameStart).toLowerCase();
			if (user.length() >= 32) {
				continue;
			}
			users.add(user);
		}

		m = rtRegExPattern3.matcher(tweet);
		while (m.find()) {
			String match = m.group();
			String user = match.substring(rtPattern3NameStart).toLowerCase();
			if (user.length() >= 32) {
				continue;
			}
			users.add(user);
		}

		m = rtRegExPattern4.matcher(tweet);
		while (m.find()) {
			String match = m.group();
			String user = match.substring(rtPattern4NameStart).toLowerCase();
			if (user.length() >= 32) {
				continue;
			}
			users.add(user);
		}

		return users;
	}

	public static String findAtReply(String tweet) {
		Matcher m = atReplyRegExPattern.matcher(tweet);

		String user = null;
		
		while (m.find()) {
			user = m.group().substring(1).toLowerCase();
			if (user.length() > 32) {
				user = null;
			}
		}

		return user;
	}

	public static Set<String> findMentionedUsers(String tweet, Set<String> retweetedUsers) {
		Set<String> users = new HashSet<String>();

		Matcher m = mentionRegExPattern.matcher(tweet);

		while (m.find()) {
			String match = m.group();
			String user = match.substring(1).toLowerCase();

			if (m.start() == 0) {
				//This is an @reply -- ignore it here
			}
			else if (!retweetedUsers.contains(user) && (user.length() < 32)) {
				users.add(user);
			}
		}

		return users;
	}
	
	public static Set<String> findHashTags(String tweet) {
		Matcher m = hashTagRegExPattern.matcher(tweet);

		HashSet<String> hashes = new HashSet<String>();

		while (m.find()) {
			hashes.add(m.group().substring(2).toLowerCase());
		}

		return hashes;
	}

}
