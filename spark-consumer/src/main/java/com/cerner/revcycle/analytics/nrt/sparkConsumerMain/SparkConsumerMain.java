package com.cerner.revcycle.analytics.nrt.sparkConsumerMain;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.cerner.revcycle.analytics.nrt.sparkConsumerConstants.SparkConsumerConstants;
import com.cerner.revcycle.analytics.nrt.sparkConsumerUtility.SparkConsumerUtility;

import kafka.serializer.StringDecoder;
import scala.Tuple2;;

/**
 * The Consumer class consumes the data from the kafka topic for further
 * processing
 * 
 * @author Uma Sivakumar (US067736)
 * @author Swapnil Srivastava (SS067726)
 * @author Sanjana M (SM068520)
 *
 */
public class SparkConsumerMain {

	final static Logger logger = Logger.getLogger(SparkConsumerMain.class);

	/**
	 * The main method of Consumer Class.
	 *
	 * @param args the arguments
	 * @throws InterruptedException the interrupted exception
	 * @throws IOException 
	 */
	public static void main(String[] args) throws InterruptedException, ConnectException, IOException {
		logger.info("Inside Main Method of Consumer Class");
		
	
		SparkConf sparkConf = new SparkConf().setAppName("ClaimsConsumer").setMaster("local[*]");

		SparkConsumerUtility.getConsumerUtility();

		/**
		 * set streaming context for spark streaming for data to be consumed every 50
		 * seconds
		 */
		logger.info("Setting streaming context");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaStreamingContext sparkStreamingContext = new JavaStreamingContext(sparkContext,
				new Duration(SparkConsumerUtility.setDuration));

		/**
		 * mapping of the keys to their values is done
		 */
		logger.info("Mapping");
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put(SparkConsumerConstants.SERVER, SparkConsumerUtility.server);
		kafkaParams.put(SparkConsumerConstants.GROUP_ID, SparkConsumerUtility.groupId);
		Set<String> topic = Collections.singleton(SparkConsumerUtility.topicName);

		JavaPairInputDStream<String, String> kafkaSparkPairInputDStream = null;

		try {
			kafkaSparkPairInputDStream = KafkaUtils.createDirectStream(sparkStreamingContext, String.class,
					String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topic);
		} catch (ConnectException c) {
			logger.error("Kafka connection error");
			System.exit(0);
		}

		/**
		 * key-value pairs are mapped and consumed by sparkInputStream
		 */
		logger.info("Mapped values consumed");
		JavaDStream<String> kafkaSparkInputDStream = kafkaSparkPairInputDStream
				.map(new Function<Tuple2<String, String>, String>() {

					/**
					 * 
					 * The serialization at runtime associates with each serializable class a
					 * version number, called a serialVersionUID, which is used during
					 * deserialization to verify that the sender and receiver of a serialized object
					 * have loaded classes for that object that are compatible with respect to
					 * serialization.
					 */
					private static final long serialVersionUID = 1L;

					/**
					 * the tuple containing the string is returned
					 */
					public String call(Tuple2<String, String> tuple2) {
						return tuple2._2();
					}
				});

		
		/**
		 * 
		 * each row of data consumed is outputted in the form of a list, which is
		 * repartitioned and data is saved to a text file
		 */
		logger.info("Outputting the row of data");
		kafkaSparkInputDStream.foreachRDD(javaRdd -> {	
			javaRdd.coalesce(1).saveAsTextFile(SparkConsumerUtility.outputFileName);
			javaRdd.foreach(rdd -> {
				System.out.println("rdd: " + rdd);
			});
		});

		/**
		 * streaming context starts when the program runs and keeps looking for string
		 * output till the program terminates
		 */
		logger.info("sparkStreamingContext started");
		sparkStreamingContext.start();

		logger.info("sparkStreamingContext awaiting termination");
		sparkStreamingContext.awaitTermination();

		logger.info("Exited Main Method of Consumer Class");
	}
}