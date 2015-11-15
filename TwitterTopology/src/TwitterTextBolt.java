/**
 * TwitterText bolt extracts the text content from a tweet
 */

import java.util.Map;

import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TwitterTextBolt extends BaseRichBolt {

	private static final long serialVersionUID = -8787685074623819603L;

	OutputCollector _collector;

	@Override
	/*
	 * The prepare method provides the bolt with an OutputCollector that is used
	 * for emiting tuples from this bolt
	 * 
	 * The prepare implementation simply saves the OutputCollector as an
	 * instance variable to be used later on the execute method.
	 */
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {

		_collector = collector;
	}

	@Override
	/*
	 * The execute method receives a tuple from one of the bolt's inputs. The
	 * TwitterText grabs the first field from the tuple and casts it to a
	 * twitter4j.Status (It's single Tweet, specified by the id parameter. The
	 * Tweet’s author is embedded within the tweet..) Then, it gets the tweet
	 * text.
	 */
	public void execute(Tuple tuple) {

		Status TwR = (Status) tuple.getValue(0);

		_collector.emit(tuple, new Values(TwR.getText()));

	}

	@Override
	/*
	 * The declareOutputFields method declares that the TwitterText emits
	 * 1-tuples with one field called "TweetText".
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("TweetText"));

	}

}