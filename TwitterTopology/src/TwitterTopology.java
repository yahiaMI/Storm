/**
 * This topology contains a spout and two bolts. The spout emits Tweets.
 * The first bolt extracts only the text from the tweet. The second bolt
 * analyzes the tweet text sentiment using VADER sentiment analysis
 * tool. VADER is a tool from the library NLTK. Bolt Tweet Output scoring example
 * is ["0.159","0.0","0.841"] for negative, positive, neutral scoring
 */
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class TwitterTopology {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		TopologyBuilder builder = new TopologyBuilder();

		/* Attributes that let us use Twitter API */
		String consumerKey = "wdEKGv6cWKU6b99FxJO2O6MPi";
		String consumerSecret = "6hM2pdQYPONSWodilXr79V0SBcgP4pKmTnWBWDHnDkhd8QQjQQ";
		String accessToken = "2422433857-4HDMbKOOqUp9hO3eExdFWk7radR1tWkXkLERmaD";
		String accessTokenSecret = "fC6FttldiQchNnJ4MXfjUR5Nw9OifAyFP89anHZU6hWnS";

		/* Keywords for twitter filter */
		String[] keyWords = { "nyc" };

		TwitterSpout TwSpout = new TwitterSpout(consumerKey, consumerSecret,
				accessToken, accessTokenSecret, keyWords);
		builder.setSpout("tweet", TwSpout);

		/*
		 * The bolt "tweetText" declares that it wants to read all the tuples
		 * emitted by the spout "tweet" using a shuffle
		 * grouping."shuffle grouping" means that tuples should be randomly
		 * distributed from the input tasks to the bolt's tasks. *
		 */
		builder.setBolt("tweetText", new TwitterTextBolt()).shuffleGrouping(
				"tweet");
		builder.setBolt("scoring", new SentimentAnalysisBolt())
				.shuffleGrouping("tweetText");

		Config conf = new Config();

		/* We log every message emitted by every component */
		conf.setDebug(true);

		/*
		 * We specify how many processes or workers we want allocated around the
		 * cluster to execute the topology.
		 */
		conf.setNumWorkers(3);

		LocalCluster cluster = new LocalCluster();

		/* We submit the topology TwitterTopology to the LocalCluster */
		cluster.submitTopology("TwitterTopology", conf,
				builder.createTopology());

		/* The topology will run for 10 seconds */
		Utils.sleep(10000);

		cluster.killTopology("TwitterTopology");
		cluster.shutdown();

	}

}
