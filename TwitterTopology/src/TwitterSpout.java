/**
 *  This spout emits Tweets. 
 *
 **/

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

@SuppressWarnings("serial")
public class TwitterSpout extends BaseRichSpout {

	/* The collector is used to emit tuples from this spout */
	SpoutOutputCollector _collector;

	/* A queue to store Twitter Status */
	LinkedBlockingQueue<Status> queue = null;

	TwitterStream _twitterStream;

	/* Keywords for twitter filter */
	String[] keyWords;

	/* Attributes that lets us use Twitter API */
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;

	public TwitterSpout(String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret, String[] keyWords) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keyWords = keyWords;
	}

	public TwitterSpout() {
		// TODO Auto-generated constructor stub
	}

	@Override
	/*
	 * The open function is called when a task for this component is initialized
	 * within a worker on the cluster
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		/* The collector is used to emit tuples from this spout */
		_collector = collector;

		/* We create a queue that will contain the status */
		queue = new LinkedBlockingQueue<Status>(1000);

		/* We set the Twitter API connection configuration */
		ConfigurationBuilder builder = new ConfigurationBuilder();
		builder.setDebugEnabled(true);
		builder.setJSONStoreEnabled(true);
		builder.setOAuthConsumerKey(consumerKey);
		builder.setOAuthConsumerSecret(consumerSecret);
		builder.setOAuthAccessToken(accessToken);
		builder.setOAuthAccessTokenSecret(accessTokenSecret);

		/* We create a Twitter stream */
		_twitterStream = new TwitterStreamFactory(builder.build())
				.getInstance();

		/* We add a listner and specify the on sucess function (on Status) */
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {

				if (queue.size() < 1000) {

					/* We add the Twitter status to the queue */
					queue.offer(status);
				} else {
					/* The queue is full */
					System.out
							.println("Queue is now full, the following message is dropped: "
									+ status);
				}

			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception ex) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

		};

		_twitterStream.addListener(listener);

		if (keyWords.length == 0) {

			/* Starts listening on random sample of all public statuses. */
			_twitterStream.sample();
		}

		else {

			/* We create a new filter query */
			FilterQuery query = new FilterQuery().track(keyWords);
			/*
			 * Start consuming public statuses that match one or more filter
			 * predicates.
			 */
			_twitterStream.filter(query);
		}

	}

	@Override
	public void nextTuple() {

		/**
		 * We get the head of the queue. It's a twitter4j.Status (It's single
		 * Tweet, specified by the id parameter. The Tweet’s author is embedded
		 * within the tweet..)
		 * 
		 * If it's null we sleep, else, we emit it.
		 */

		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
			_collector.emit(new Values(ret));

		}
	}

	@Override
	public void close() {
		_twitterStream.shutdown();
	}

	@Override
	/*
	 * Storm has determined that the tuple emitted by this spout with the msgId
	 * identifier has been fully processed.
	 */
	public void ack(Object id) {
	}

	@Override
	/*
	 * The tuple emitted by this spout with the msgId identifier has failed to
	 * be fully processed.
	 */
	public void fail(Object id) {
	}

	@Override
	/* We declare the output fields */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
