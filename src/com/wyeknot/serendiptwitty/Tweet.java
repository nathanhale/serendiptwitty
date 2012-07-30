package com.wyeknot.serendiptwitty;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Tweet {
	long id;
	String user;
	Timestamp timestamp;
	String tweet;

	public static final SimpleDateFormat stanfordTweetDateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	Tweet(long id) {
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
}
