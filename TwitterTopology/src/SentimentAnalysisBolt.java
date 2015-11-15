/**
 * This bolt triggers SentimentAnalysisBolt.py which analyzes the
 * tweet text sentiment using VADER sentiment analysis tools. VADER is a
 * tool from the NLTK tool.
 * 
 */
import java.util.Map;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class SentimentAnalysisBolt extends ShellBolt implements IRichBolt {

	private static final long serialVersionUID = 7306880943293169357L;

	public SentimentAnalysisBolt() {
		/*
		 * We specify the python file that will handle the bolt. It should be in
		 * the multilang/resources. In local mode, the multilang should be added
		 * in the class path
		 */
		super("python", "SentimentAnalysisBolt.py");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		/*
		 * The scoring fields returned by SentimentAnalysisBolt.py are negative,
		 * positive, neutral scoring
		 */
		declarer.declare(new Fields("neg", "pos", "neu"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}