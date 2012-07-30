package com.wyeknot.serendiptwitty;

import java.awt.EventQueue;
import java.util.HashSet;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ling.CoreLabel;


public class Recommender /*extends JFrame*/ {
/*
	private JPanel contentPane;
	private JTextField userName;
	private JButton btnSelectUser;
	
	private static final long serialVersionUID = 3443550362808672703L;
	private JScrollPane tweetsScrollPane;
	private JTextPane tweetsPane; //*/

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
	
	public static final int NUM_TWEETS_TO_INDEX = 5000;
	

	/**
	 * Launch the application.
	 */
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
					//Recommender frame = new Recommender();
					//frame.recommend();
					//frame.setVisible(true);

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
		graph = new GraphManager(database, indexMgr, "chrissaad", otherDistinguishedUsers);
		graph.createGraph();
		graph.runAlgorithm();
	}

	/**
	 * Create the frame.
	 */
	public Recommender() {
/*
		setTitle("Network Explorer");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 500, 500);//*/

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
		
/*
		String serializedClassifier = Recommender.nerClassifiersPath + "english.all.3class.distsim.crf.ser.gz";

		classifier = CRFClassifier.getClassifierNoExceptions(serializedClassifier);
		
		Action selectAction = new AbstractAction("Select User") {
			private static final long serialVersionUID = 1402645048605384524L;

			public void actionPerformed(ActionEvent e) {
				loadTweets(userName.getText().toLowerCase());
			}
		};

		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		GridBagLayout gbl_contentPane = new GridBagLayout();
		gbl_contentPane.columnWidths = new int[]{289, 85, 0};
		gbl_contentPane.rowHeights = new int[]{0, 0, 0, 0};
		gbl_contentPane.columnWeights = new double[]{1.0, 1.0, Double.MIN_VALUE};
		gbl_contentPane.rowWeights = new double[]{0.0, 1.0, 1.0, Double.MIN_VALUE};
		contentPane.setLayout(gbl_contentPane);

		userName = new JTextField();		
		userName.setAction(selectAction);

		GridBagConstraints gbc_userName = new GridBagConstraints();
		gbc_userName.insets = new Insets(0, 0, 5, 5);
		gbc_userName.fill = GridBagConstraints.HORIZONTAL;
		gbc_userName.gridx = 0;
		gbc_userName.gridy = 0;
		contentPane.add(userName, gbc_userName);
		userName.setColumns(10);

		btnSelectUser = new JButton();//selectAction);
		btnSelectUser.setText("Search");

		GridBagConstraints gbc_btnSelectUser = new GridBagConstraints();
		gbc_btnSelectUser.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnSelectUser.insets = new Insets(0, 0, 5, 0);
		gbc_btnSelectUser.gridx = 1;
		gbc_btnSelectUser.gridy = 0;
		contentPane.add(btnSelectUser, gbc_btnSelectUser);

		tweetsScrollPane = new JScrollPane();
		tweetsScrollPane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
		GridBagConstraints gbc_tweetsScrollPane = new GridBagConstraints();
		gbc_tweetsScrollPane.gridwidth = 2;
		gbc_tweetsScrollPane.gridheight = 2;
		gbc_tweetsScrollPane.fill = GridBagConstraints.BOTH;
		gbc_tweetsScrollPane.gridx = 0;
		gbc_tweetsScrollPane.gridy = 1;
		contentPane.add(tweetsScrollPane, gbc_tweetsScrollPane);
		
		tweetsPane = new JTextPane();
		tweetsPane.setEditable(false);
		tweetsScrollPane.setViewportView(tweetsPane);//*/
	}

/*
	public void loadTweets(String queryTerm) {

		System.out.println("start loading tweets at " + new Date());

		try {
			ScoreDoc[] hits = indexMgr.runQuery(queryTerm);//LuceneIndexManager.TWEET_FIELD_NAME + ":" + queryTerm);

			String result = queryTerm + " has " + hits.length + " tweets in our database:\n";
			tweetsPane.setText(result);
			System.out.println(result);

			SimpleAttributeSet dateAttribute = new SimpleAttributeSet();
			StyleConstants.setBold(dateAttribute, true);
			StyleConstants.setItalic(dateAttribute, true);

			for( int i = 0 ; i < hits.length ; ++i ) {
				Document doc = indexMgr.getDocumentFromDocId(hits[i].doc);

				javax.swing.text.Document tweetPaneDoc = tweetsPane.getDocument();

				tweetPaneDoc.insertString(tweetPaneDoc.getLength(), "\n\n" + doc.get("date"), dateAttribute);
				tweetPaneDoc.insertString(tweetPaneDoc.getLength(), "\n" + doc.get("user"), dateAttribute);
				tweetPaneDoc.insertString(tweetPaneDoc.getLength(), "\n" + doc.get("tweet"), null);
				
				List<List<CoreLabel>> out = classifier.classify(doc.get("tweet"));
				for (List<CoreLabel> sentence : out) {
					for (CoreLabel word : sentence) {
						String type = word.get(AnswerAnnotation.class);
						String entityText = word.word() + "/" + type + " ";
						if (!type.equals("O")) {
							//System.out.println(entityText);
							tweetPaneDoc.insertString(tweetPaneDoc.getLength(), "\n" + entityText, null);
						}
					}
				}
			}

			//Scroll back to the top -- need to invoke later because things are still messing with it if we do it immediately
			javax.swing.SwingUtilities.invokeLater(new Runnable() {
				public void run() { 
					tweetsScrollPane.getViewport().setViewPosition(new Point(0,0));
				}
			});
		}
		catch (Exception e) {
			e.printStackTrace();
			System.err.println("\nCouldn't load tweets for query " + userName.getText());
		}

		System.out.println("finish loading tweets at " + new Date());
	}
//*/
}




