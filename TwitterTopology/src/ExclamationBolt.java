/*
 * ExclamationBolt appends the string "!!!" to its input
 * This is a basic example of a Storm topology http://storm.apache.org/tutorial.html
 */

import java.util.Map;

import twitter4j.Status;
import twitter4j.TwitterStream;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExclamationBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 48218013271799132L;
	OutputCollector _collector;

	@Override
	/*
	 * The prepare method provides the bolt with an OutputCollector that is used
	 * for emitting tuples from this bolt
	 * 
	 * The prepare implementation simply saves the OutputCollector as an
	 * instance variable to be used later on in the execute method.
	 */
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {

		_collector = collector;
	}

	@Override
	/*
	 * The execute method receives a tuple from one of the bolt's inputs. The
	 * ExclamationBolt grabs the first field from the tuple and emits a new
	 * tuple with the string "!!!" appended to it.
	 */
	public void execute(Tuple tuple) {

		Status TwR = (Status)tuple.getValue(0);
		
		
		
		_collector
				.emit(tuple, new Values(TwR.getText()+ "!!!"));
		/*
		 * The tuple is acked. It's a part of Storm's reliability API for
		 * guaranteeing no data loss It sends the tuple to the acker bolt which
		 * keeps track of all the tuples
		 */
		_collector.ack(tuple);
	}

	@Override
	/*
	 * The declareOutputFields method declares that the ExclamationBolt emits
	 * 1-tuples with one field called "word".
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}