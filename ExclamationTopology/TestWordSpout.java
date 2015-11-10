/*
 * TestWordSpout in this topology emits a random word from the list ["nathan", "mike", "jackson", "golda", "bertels"] as a 1-tuple every 100ms.
 * This is a basic example of a Storm topology http://storm.apache.org/tutorial.html
 */

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TestWordSpout extends BaseRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8570964067717740180L;

	SpoutOutputCollector _collector;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void close() {

	}

	public void nextTuple() {
		Utils.sleep(100);
		final String[] words = new String[] { "nathan", "mike", "jackson",
				"golda", "bertels" };
		final Random rand = new Random();
		final String word = words[rand.nextInt(words.length)];
		_collector.emit(new Values(word));
	}

	public void ack(Object msgId) {

	}

	public void fail(Object msgId) {

	}

	/*
	 * The declareOutputFields method declares that the TestWordSpout emits
	 * 1-tuples with one field called "word".
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}