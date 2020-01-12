package com.cerner.revcycle.analytics.nrt.sparkConsumerUtility;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * @author Uma Sivakumar (US067736)
 * @author Swapnil Srivastava (SS067726)
 * @author Sanjana M (SM068520)
 *
 */
public class SparkConsumerUtility {

	final static Logger logger = Logger.getLogger(SparkConsumerUtility.class); 


	public static String server = null;
	public static String groupId = null;
	public static String topicName = null;
	public static String outputFileName = null;
	public static String durationToPoll = null;
	public static int setDuration = 0;
	
	public static void getConsumerUtility()
	{
		logger.info("Entering into getConsumerUtility()");
		Properties props = new Properties();
		
			/**
			 * Loading the logger properties
			 */
			logger.info("Loading the logger file");	
			try {
				props.load(new FileInputStream("/home/uma/eclipse-workspace/consumer/src/main/resources/properties/log4j.properties"));
			} catch (FileNotFoundException e) {
				logger.error("FileNotFound error");
			} catch (IOException e) {
				logger.error(e);
			}
			PropertyConfigurator.configure(props);
			
			/**
        	 * Loading the properties file 
        	 */
			logger.info("Loading the properties file");	
			FileInputStream input = null;
			try {
				input=new FileInputStream("/home/uma/eclipse-workspace/spark-consumer/src/main/resources/properties/config.properties");
			} catch (FileNotFoundException e) {
				logger.error("FileNotFound error");
			}
        	try {
				props.load(input);
			} catch (IOException e) {
				logger.error(e);
			}
        	
        	
        	server = props.getProperty("server");
        	groupId = props.getProperty("groupId");
        	topicName = props.getProperty("topicName");
        	outputFileName = props.getProperty("outputFileName");
        	durationToPoll = props.getProperty("durationToPoll");
        	setDuration = Integer.parseInt(durationToPoll);
		
        	logger.info("Exiting from getConsumerUtility()");
	}

}