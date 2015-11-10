/*
 * This topology contains a spout and two bolts. The spout emits words, and each bolt appends the string "!!!" to its input. 
 * This is a basic example of a Storm topology http://storm.apache.org/tutorial.html
 * 
 */
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class ExclamationTopology {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		TopologyBuilder builder = new TopologyBuilder();

		/*
		 * This topology contains a spout and two bolts. The spout emits words,
		 * and each bolt appends the string "!!!" to its input.
		 */

		/*
		 * These methods take as input a user-specified id, an object containing
		 * the processing logic, and the amount of parallelism (the threads
		 * number). Below, the spout is given id "words" and the bolts are given
		 * ids "exclaim1" and "exclaim2".
		 */
		builder.setSpout("word", new TestWordSpout(), 10);

		/*
		 * component "exclaim1" declares that it wants to read all the tuples
		 * emitted by component "words" using a shuffle
		 * grouping."shuffle grouping" means that tuples should be randomly
		 * distributed from the input tasks to the bolt's tasks.
		 */
		builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping(
				"word");
		builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping(
				"exclaim1");

		Config conf = new Config();

		// We log every message emitted by every component
		conf.setDebug(true);

		/*
		 * We specify how many processes or workers we want allocated around the
		 * cluster to execute the topology.
		 */
		conf.setNumWorkers(2);

		LocalCluster cluster = new LocalCluster();

		// We submit the topology test to the LocalCluster
		cluster.submitTopology("test", conf, builder.createTopology());

		// The topology will run for 10 seconds
		Utils.sleep(10000);

		cluster.killTopology("test");
		cluster.shutdown();

	}

}
