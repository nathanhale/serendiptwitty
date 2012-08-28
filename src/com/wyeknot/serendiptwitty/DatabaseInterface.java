package com.wyeknot.serendiptwitty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;


//Not thread safe!!
public class DatabaseInterface {

	private Connection dbConnection;
	private ResultSet curResults = null;
	private PreparedStatement curStatement = null;

	private ResultSet curInternalResults = null;
	private PreparedStatement curInternalStatement = null;

	public static final int MAX_DATABASE_BATCH_SIZE = 50000;

	private int numTweetVertices = 0;
	private int numUserVertices = 0;

	public DatabaseInterface() throws SQLException {
		String connectionURL = "jdbc:postgresql://localhost:5432/tweet";
		dbConnection = DriverManager.getConnection(connectionURL,"twitter","tweets357");
	}

	public void clusterEdges() {
		try {
			PreparedStatement st = dbConnection.prepareStatement("CLUSTER idx_edge_name ON edges;");
			st.execute();
			st.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void clusterUsers() {
		try {
			PreparedStatement st = dbConnection.prepareStatement("CLUSTER user_vertices_pkey ON user_vertices;");
			st.execute();
			st.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void clusterTweetsByAuthor() {
		try {
			PreparedStatement st = dbConnection.prepareStatement("CLUSTER idx_tweet_author ON tweet_vertices;");
			st.execute();
			st.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void clusterTweetsById() {
		try {
			PreparedStatement st = dbConnection.prepareStatement("CLUSTER tweet_vertices_pkey ON tweet_vertices;");
			st.execute();
			st.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void createAndClusterEdgesTweetIds() {
		try {
			dbConnection.setAutoCommit(false);

			PreparedStatement st = dbConnection.prepareStatement("CREATE TABLE edges_tweetids AS (SELECT * FROM EDGES);");
			st.execute();
			st.close();

			dbConnection.commit();
			dbConnection.setAutoCommit(true);

			st = dbConnection.prepareStatement("CREATE INDEX idx_edges_tweetids_tweetids ON edges_tweetids USING btree (tweetid);");
			st.execute();
			st.close();

			st = dbConnection.prepareStatement("CREATE INDEX idx_edges_tweetids_names ON edges_tweetids USING btree (name);");
			st.execute();
			st.close();

			st = dbConnection.prepareStatement("CREATE INDEX idx_edges_tweetids_types ON edges_tweetids USING btree (type);");
			st.execute();
			st.close();

			st = dbConnection.prepareStatement("CLUSTER idx_edges_tweetids_tweetids ON edges_tweetids;");
			st.execute();
			st.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void analyzeTables() {
		try {
			PreparedStatement st = dbConnection.prepareStatement("ANALYZE user_vertices;");
			st.execute();
			st.close();
			st = dbConnection.prepareStatement("ANALYZE tweet_vertices;");
			st.execute();
			st.close();
			st = dbConnection.prepareStatement("ANALYZE edges;");
			st.execute();
			st.close();
			st = dbConnection.prepareStatement("ANALYZE edges_tweetids;");
			st.execute();
			st.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	private int countTweetVertices() {
		try {
			PreparedStatement st = dbConnection.prepareStatement("SELECT COUNT(*) FROM tweet_vertices;");

			ResultSet result = st.executeQuery();
			result.next();
			int numRows = result.getInt(1);
			
			st.close();

			return numRows;
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return 0;
	}

	private int countUserVertices() {
		try {
			PreparedStatement st = dbConnection.prepareStatement("SELECT COUNT(*) FROM user_vertices;");

			ResultSet result = st.executeQuery();
			result.next();
			int numRows = result.getInt(1);

			st.close();

			return numRows;
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return 0;
	}
	
	public int getNumTweetVertices() {
		if (numTweetVertices == 0) {
			numTweetVertices = countTweetVertices();
		}

		return numTweetVertices;
	}
	
	public int getNumUserVertices() {
		if (numUserVertices == 0) {
			numUserVertices = countUserVertices();
		}

		return numUserVertices;
	}

	public void acquireCursorForTweetsOfUsers(Set<String> users) {
		try {
			closeCursor();

			String nameParams = "";
			for (int ii = 0 ; ii < users.size() ; ii++) {
				if (ii == 0) {
					nameParams += " name=?";
				}
				else {
					nameParams += " OR name=?";
				}
			}

			curStatement = dbConnection.prepareStatement("SELECT * FROM tweet_vertices WHERE" + nameParams + ";",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			curStatement.setFetchSize(10000);

			int curPos = 1;
			for (String user : users) {
				curStatement.setString(curPos++, user);
			}

			curResults = curStatement.executeQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void acquireCursorForAllFollowerEdgeTweets(String user, int followerEdgeType) {
		try {
			closeCursor();
			curStatement = dbConnection.prepareStatement("SELECT T.* FROM edges E, tweet_vertices T WHERE E.name=? AND E.type=? AND E.tweetid=T.tweetid;",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			curStatement.setFetchSize(10000);
			curStatement.setString(1, user);
			curStatement.setInt(2, followerEdgeType);

			curResults = curStatement.executeQuery();		
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void acquireCursorForTweets(String user) {
		try {
			closeCursor();
			curStatement = dbConnection.prepareStatement("SELECT * FROM tweet_vertices WHERE name=?;",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			curStatement.setFetchSize(10000);
			curStatement.setString(1, user);
			curResults = curStatement.executeQuery();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void acquireInternalCursorForTweets(String author) {
		try {
			closeInternalCursor();
			curInternalStatement = dbConnection.prepareStatement("SELECT * FROM tweet_vertices WHERE name=?;",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			curInternalStatement.setFetchSize(10000);
			curInternalStatement.setString(1, author);
			curInternalResults = curInternalStatement.executeQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public void acquireInternalCursorForFolloweesEdges(String retweetee, String retweeter, int edgeTypeAuthorship, int edgeTypeFollowees) {
		try {
			closeInternalCursor();
			curInternalStatement = dbConnection.prepareStatement("SELECT T.tweetid FROM namenetwork_followers F, tweet_vertices T WHERE T.name=F.username AND F.followername=?;",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			curInternalStatement.setFetchSize(10000);
			curInternalStatement.setString(1, retweetee);
			curInternalResults = curInternalStatement.executeQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void acquireInternalCursorForFollowersEdges(String retweetee, String retweeter, int edgeTypeAuthorship, int edgeTypeFollowers) {
		try {
			closeInternalCursor();

			curInternalStatement = dbConnection.prepareStatement("SELECT T.tweetid, U.name FROM (SELECT tweetid FROM tweet_vertices WHERE name=?) T, (SELECT U.name FROM user_vertices U, namenetwork N WHERE N.username=? AND N.followername=U.name) U;",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			curInternalStatement.setFetchSize(10000);
			curInternalStatement.setString(1, retweetee);
			curInternalStatement.setString(2, retweeter);
			curInternalResults = curInternalStatement.executeQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public long getNextTweetIdFromInternalCursor() {
		try {
			if ((curInternalResults == null) || curInternalResults.isClosed()) {
				return -1;
			}

			if (!curInternalResults.next()) {
				return -1;
			}

			return curInternalResults.getLong("tweetid");
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	}

	public String getNameFromCurrentInternalCursorPos() {
		try {
			if ((curInternalResults == null) || curInternalResults.isClosed()) {
				return null;
			}

			if (curInternalResults.isBeforeFirst()) {
				return null;
			}

			return curInternalResults.getString("name");
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public void acquireCursorForTweetVertices() {
		try {
			closeCursor();
			//This is ASC because of the way that it is used for creating content edges
			curStatement = dbConnection.prepareStatement("SELECT * FROM tweet_vertices ORDER BY name ASC;",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			curStatement.setFetchSize(10000);
			curResults = curStatement.executeQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getNextTweetFromCursor() {
		try {
			if ((curResults == null) || curResults.isClosed()) {
				return null;
			}

			if (!curResults.next()) {
				return null;
			}

			return curResults.getString("tweet");
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	//The ORDER BY statement here is essential because of the way the algorithm is set up.
	public void acquireCursorForUpdatingTweetScores() {
		try {
			closeCursor();

			dbConnection.setAutoCommit(false);
			curStatement = dbConnection.prepareStatement("SELECT E.*, U.score as user_score FROM edges E, user_vertices U WHERE E.name=U.name ORDER BY E.name;",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			curStatement.setFetchSize(100000);
			curResults = curStatement.executeQuery();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//The ORDER BY statement here is essential because of the way the algorithm works.
	public void acquireCursorForUpdatingUserScores() {
		try {
			closeCursor();

			dbConnection.setAutoCommit(false);

			curStatement = dbConnection.prepareStatement("SELECT E.*, T.score as tweet_score FROM edges_tweetids E, tweet_vertices T WHERE E.tweetid=T.tweetid ORDER BY E.tweetid;",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			curStatement.setFetchSize(100000);

			curResults = curStatement.executeQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void acquireCursorForInitializingUserScores() {
		try {
			closeCursor();
			curStatement = dbConnection.prepareStatement("SELECT * FROM user_vertices;",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			curStatement.setFetchSize(10000);
			curResults = curStatement.executeQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public List<Integer> getFollowerCountsOfOverlappingUserSet(String distinguishedUser, String otherUser) {
		try {
			PreparedStatement st = dbConnection.prepareStatement("SELECT C.count FROM ((SELECT username AS name FROM namenetwork_followers WHERE followername=?) INTERSECT (SELECT followername AS name FROM namenetwork WHERE username=?)) S, followercount C WHERE S.name=C.username;",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			st.setFetchSize(1000000);			
			st.setString(1,distinguishedUser);
			st.setString(2,otherUser);

			ResultSet result = st.executeQuery();

			ArrayList<Integer> followerCounts = new ArrayList<Integer>();

			while (result.next()) {
				followerCounts.add(new Integer(result.getInt(1)));
			}

			st.close();

			return followerCounts;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}


	public void resetScores() {
		try {
			PreparedStatement st;

			st = dbConnection.prepareStatement("UPDATE user_vertices SET score=original_score;");
			st.executeUpdate();
			st = dbConnection.prepareStatement("UPDATE tweet_vertices SET score=original_score;");
			st.executeUpdate();

			st.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Couldn't normalize user scores!");
		}
	}

	//Should only be done once, after the initial user scoring method is undertaken
	public void normalizeUserScores() {

		try {
			PreparedStatement st = dbConnection.prepareStatement("UPDATE user_vertices SET original_score=(original_score / S.sum) FROM (SELECT SUM(original_score) FROM user_vertices) S;");
			st.executeUpdate();

			st.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Couldn't normalize user scores!");
		}
	}


	public void normalizeTweetScores() {
		try {
			PreparedStatement st = dbConnection.prepareStatement("UPDATE tweet_vertices SET original_score=(original_score / S.sum) FROM (SELECT SUM(original_score) FROM tweet_vertices) S;");
			st.executeUpdate();

			st.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Couldn't normalize tweet scores!");
		}
	}

	public boolean originalUserScoreCalculated() {
		try {
			PreparedStatement st = dbConnection.prepareStatement("SELECT SUM(original_score) FROM user_vertices;");

			ResultSet result = st.executeQuery();
			result.next();
			double sum = result.getDouble(1);

			st.close();

			return (Math.rint(sum) == 1);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean originalTweetScoreCalculated() {
		try {
			PreparedStatement st = dbConnection.prepareStatement("SELECT SUM(original_score) FROM tweet_vertices;");

			ResultSet result = st.executeQuery();
			result.next();
			double sum = result.getDouble(1);

			st.close();

			System.out.println("original_score for tweets sum was " + sum);

			return (Math.rint(sum) == 1);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	private int executeUpdateScoresBatch(PreparedStatement st) {
		int updates = 0;

		try {
			int[] results = st.executeBatch();
			for (int ii = 0 ; ii < results.length ; ii++) {
				updates += results[ii];
			}

			st.close();
			return updates;

		} catch(Exception e) {
			e.printStackTrace();
			return 0;
		}
	}

	//The scores passed in already have the lambdaTweet factor included in them
	public int updateTweetScores(Map<Long,Double> scores) {
		PreparedStatement st = null;

		int updates = 0;
		int curBatchCount = 0;

		try {
			dbConnection.setAutoCommit(true);

			for (Long tweetId : scores.keySet()) {
				if (null == st) {
					st = dbConnection.prepareStatement("UPDATE tweet_vertices SET score=(score + ?) WHERE tweetid=?;");
					curBatchCount = 0;
				}

				double score = scores.get(tweetId);

				st.setDouble(1, score);
				st.setLong(2, tweetId);

				st.addBatch();
				curBatchCount++;

				if (curBatchCount > DatabaseInterface.MAX_DATABASE_BATCH_SIZE) {
					updates += executeUpdateScoresBatch(st);
					st = null;
				}
			}

			if (null != st) {
				updates += executeUpdateScoresBatch(st);
			}

			System.out.println("there were " + updates + " tweet updates out of " +
					scores.keySet().size() + " tweets this iteration");

			return updates;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}

	//The scores passed in already have the lambdaUser factor included in them
	public int updateUserScores(Map<String,Double> scores) {
		PreparedStatement st = null;

		int updates = 0;
		int curBatchCount = 0;

		try {
			dbConnection.setAutoCommit(true);

			for (String userName : scores.keySet()) {
				if (null == st) {
					st = dbConnection.prepareStatement("UPDATE user_vertices SET score=(score + ?) WHERE name=?;");
					curBatchCount = 0;
				}

				double score = scores.get(userName);

				st.setDouble(1, score);
				st.setString(2, userName);

				st.addBatch();
				curBatchCount++;

				if (curBatchCount > DatabaseInterface.MAX_DATABASE_BATCH_SIZE) {
					updates += executeUpdateScoresBatch(st);
					st = null;
				}
			}

			if (null != st) {
				updates += executeUpdateScoresBatch(st);
			}

			System.out.println("there were " + updates + " user updates out of " +
					scores.keySet().size() + " users this iteration");

			return updates;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}

	public void updateBaseTweetScores(double lambdaTweets, double totalAmount) {

		double originalScoreFactor = 1 - lambdaTweets;

		try {
			PreparedStatement st = dbConnection.prepareStatement("UPDATE tweet_vertices SET score=(? + (original_score * ?));");
			st.setDouble(1, (lambdaTweets * (totalAmount / (double)getNumTweetVertices())));
			st.setDouble(2, originalScoreFactor);
			st.executeUpdate();
			st.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void updateBaseUserScores(double lambdaUsers, double totalAmount) {

		double originalScoreFactor = 1 - lambdaUsers;

		try {
			PreparedStatement st = dbConnection.prepareStatement("UPDATE user_vertices SET score=(? + (original_score * ?));");
			st.setDouble(1, (lambdaUsers * (totalAmount / (double)getNumUserVertices())));
			st.setDouble(2, originalScoreFactor);
			st.executeUpdate();
			st.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}


	public long getNextTweetIdFromCursor() {
		try {
			if ((curResults == null) || curResults.isClosed()) {
				return -1;
			}

			if (!curResults.next()) {
				return -1;
			}

			return curResults.getLong("tweetid");
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	}

	public String getNextNameFromCursor() {
		try {
			if ((curResults == null) || curResults.isClosed()) {
				return null;
			}

			if (!curResults.next()) {
				return null;
			}

			return curResults.getString("name");
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String getNextNameFromInternalCursor() {
		try {
			if ((curInternalResults == null) || curInternalResults.isClosed()) {
				return null;
			}

			if (!curInternalResults.next()) {
				return null;
			}

			return curInternalResults.getString("name");
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}


	public String getNameFromCurrentCursorPos() {
		try {
			if ((curResults == null) || curResults.isClosed()) {
				return null;
			}

			if (curResults.isBeforeFirst()) {
				return null;
			}

			return curResults.getString("name");
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}


	public long getTweetIdFromCurrentCursorPos() {
		try {
			if ((curResults == null) || curResults.isClosed()) {
				return -1;
			}

			if (curResults.isBeforeFirst()) {
				return -1;
			}

			return curResults.getLong("tweetid");
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	}

	public double getUserScoreFromCurrentCursorPos() {
		try {
			if ((curResults == null) || curResults.isClosed()) {
				return -1;
			}

			if (curResults.isBeforeFirst()) {
				return -1;
			}

			return curResults.getDouble("user_score");
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	}

	public double getTweetScoreFromCurrentCursorPos() {
		try {
			if ((curResults == null) || curResults.isClosed()) {
				return -1;
			}

			if (curResults.isBeforeFirst()) {
				return -1;
			}

			return curResults.getDouble("tweet_score");
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	}

	public int getEdgeTypeFromCurrentCursorPos() {
		try {
			if ((curResults == null) || curResults.isClosed()) {
				return -1;
			}

			if (curResults.isBeforeFirst()) {
				return -1;
			}

			return curResults.getInt("type");
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	}

	boolean setOriginalUserScoreBatch(Map<String,Double> scores) {
		try {
			PreparedStatement st = dbConnection.prepareStatement("UPDATE user_vertices set original_score=?, score=? where name=?;");

			for (String userName : scores.keySet()) {
				st.setDouble(1, scores.get(userName));
				st.setDouble(2, scores.get(userName));
				st.setString(3, userName);
				st.addBatch();
			}

			int[] results = st.executeBatch();
			for (int ii = 0 ; ii < results.length ; ii++) {
				if (results[ii] != 1) {
					System.out.println("Couldn't add user " + (ii + 1) + " of " + results.length);
					return false;
				}
			}

			st.close();

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}


	void setTweetScoresAndOriginalScoresToZero() {
		try {
			PreparedStatement st = dbConnection.prepareStatement("UPDATE tweet_vertices SET score=0, original_score=0;");
			st.executeUpdate();
			st.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	boolean setOriginalTweetScoreBatch(Map<Long,Double> scores) {
		try {
			PreparedStatement st = dbConnection.prepareStatement("UPDATE tweet_vertices set original_score=?, score=? where tweetid=?;");

			for (Long tweet : scores.keySet()) {
				st.setDouble(1, scores.get(tweet));
				st.setDouble(2, scores.get(tweet));
				st.setLong(3, tweet);

				st.addBatch();
			}

			int[] results = st.executeBatch();
			for (int ii = 0 ; ii < results.length ; ii++) {
				if (results[ii] != 1) {
					System.out.println("Couldn't add tweet " + (ii + 1) + " of " + results.length);
					return false;
				}
			}

			st.close();

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	boolean addTweetBatch(Set<Tweet> tweets) {
		try {
			PreparedStatement st = dbConnection.prepareStatement("INSERT INTO tweet_vertices (tweetid, name, date, tweet, score, original_score) VALUES (?, ?, ?, ?, ?, ?);");

			for (Tweet tweet : tweets) {
				st.setLong(1, tweet.getId());
				st.setString(2, tweet.getUser());
				st.setTimestamp(3, tweet.getTimestamp());
				st.setString(4, tweet.getTweet());
				st.setDouble(5, 0);
				st.setDouble(6, 0);

				st.addBatch();
			}

			int[] results = st.executeBatch();
			for (int ii = 0 ; ii < results.length ; ii++) {
				if (results[ii] != 1) {
					System.out.println("Couldn't add tweet " + (ii + 1) + " of " + results.length);
					return false;
				}
			}

			st.close();

			return true;
		}
		catch (SQLException e) {
			e.printStackTrace();
			while (e != null) {  
				e.printStackTrace();
				e = e.getNextException();  
				System.out.println("");  
			}  

			return false;
		}
	}

	boolean addUserBatch(Set<String> userNames) {
		try {
			PreparedStatement st = dbConnection.prepareStatement("INSERT INTO user_vertices (name, score, original_score) VALUES (?, ?, ?);");

			for (String user : userNames) {
				st.setString(1, user);
				st.setDouble(2, GraphManager.DEFAULT_ORIGINAL_SCORE);
				st.setDouble(3, GraphManager.DEFAULT_ORIGINAL_SCORE);

				st.addBatch();
			}


			int[] results = st.executeBatch();
			for (int ii = 0 ; ii < results.length ; ii++) {
				if (results[ii] != 1) {
					System.out.println("Couldn't add user " + (ii + 1) + " of " + results.length);
					return false;
				}
			}

			st.close();

			return true;
		}
		catch (SQLException e) {
			e.printStackTrace();
			while (e != null) {  
				e.printStackTrace();
				e = e.getNextException();  
				System.out.println("");  
			}  

			return false;
		}
	}

	boolean addEdgeBatch(Set<Edge> edges) {
		try {
			PreparedStatement st = dbConnection.prepareStatement("INSERT INTO edges (name, tweetid, type) VALUES (?, ?, ?);");

			for (Edge edge : edges) {
				st.setString(1, edge.name);
				st.setLong(2, edge.tweet);
				st.setInt(3, edge.type);

				st.addBatch();
			}

			int[] results = st.executeBatch();
			for (int ii = 0 ; ii < results.length ; ii++) {
				if (results[ii] != 1) {
					System.out.println("Couldn't add edge " + (ii + 1) + " of " + results.length);
					return false;
				}
			}

			return true;
		}
		catch (SQLException e) {
			e.printStackTrace();
			while (e != null) {  
				e.printStackTrace();
				e = e.getNextException();  
				System.out.println("");  
			}  

			return false;
		}
	}

	//This wouldn't be safe to use if the user could set which table to look into
	boolean tableHasRows(String tableName) {
		PreparedStatement st;
		try {
			st = dbConnection.prepareStatement("SELECT COUNT(*) FROM " + tableName + ";");

			ResultSet result = st.executeQuery();
			result.next();
			int numRows = result.getInt(1);

			st.close();

			if (numRows > 0) {
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return false;
	}

	public void close() {
		try {
			closeCursor();
			dbConnection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}


	public void closeCursor() {
		if (null != curStatement) {
			try {
				curStatement.close(); //Closes the result set, too
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public void closeInternalCursor() {
		if (null != curInternalStatement) {
			try {
				curInternalStatement.close(); //Closes the result set, too
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public void acquireInternalCursorForFollowersInUserVertices(String userName) {
		try {
			closeInternalCursor();
			curInternalStatement = dbConnection.prepareStatement("SELECT U.name FROM user_vertices U, namenetwork N WHERE U.name=N.followername AND N.username=?;",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			curInternalStatement.setFetchSize(100000);
			curInternalStatement.setString(1, userName);
			curInternalResults = curInternalStatement.executeQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getNextFollowerNameFromCursor() {
		try {
			if ((curResults == null) || curResults.isClosed()) {
				return null;
			}

			if (!curResults.next()) {
				return null;
			}

			return curResults.getString("followername");
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String getNextFolloweeNameFromCursor() {
		try {
			if ((curResults == null) || curResults.isClosed()) {
				return null;
			}

			if (!curResults.next()) {
				return null;
			}

			return curResults.getString("username");
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	//Note: this doesn't advance the cursor as the others do
	public long getUserIdFromCurrentCursorPos() {
		try {
			if ((curResults == null) || curResults.isClosed()) {
				return -1;
			}

			if (curResults.isBeforeFirst()) {
				return -1;
			}

			return curResults.getLong("userid");
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	}
}
