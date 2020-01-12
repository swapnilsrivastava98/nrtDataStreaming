package com.cerner.revcycle.analytics.nrt.hbaseTableTriggerUtility;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.cerner.revcycle.analytics.nrt.hbaseTableTriggerConstants.HbaseTableTriggerConstants;

/**
 * The Class HbaseTableTriggerUtility
 * 
 * 
 * @author Sanjana M (SM068520)
 * @author Uma Sivakumar (US067736)
 * @author Swapnil Srivastava (SS067726)
 *
 */
public class HbaseTableTriggerUtility 
{
	
	final static Logger logger = Logger.getLogger(HbaseTableTriggerUtility.class); 
	public static String topicName;
	static Properties properties;
	public static String columnInfo;
	
	public static Properties getCoprocessorUtility()
	{
		logger.info("Inside HbaseTableTriggerUtility Method");
		Properties props = new Properties();
		FileInputStream input = null;
		
			/**
			 * Loading the logger properties
			 */
			logger.info("Loading the logger file");	
			try {
				props.load(new FileInputStream("/home/uma/eclipse-workspace/hbase-table-triggers/src/main/resources/properties/log4j.properties"));
			} catch (FileNotFoundException e1) {
				logger.error("Error: Log4j.properties not found");
			} catch (IOException e1) {
				logger.error(e1);
			}
			
			PropertyConfigurator.configure(props);
			/**
        	 * Loading the properties file 
        	 */
			logger.info("Loading the properties file");	
        	try {
				input=new FileInputStream("/home/uma/eclipse-workspace/hbase-table-triggers/src/main/resources/properties/config.properties");
			} catch (FileNotFoundException e) {
				logger.error("Error: Config.properties not found");
			}
        	try {
				props.load(input);
			} catch (IOException e) {
				logger.error(e);
			}
        	
        	try {
				input.close();
			} catch (IOException e) {
				logger.error(e);
			}
        		
		logger.info("Exited HbaseTableTriggerUtility Method");
		return props;
	}
	
	public static Properties getKafkaProperties()
	{
		logger.info("Inside getKafkaProperties()");
		Properties props = HbaseTableTriggerUtility.getCoprocessorUtility();
		
		if (properties == null)
		{
			properties = new Properties();
			try {

				/**
				 * fetching data from properties file from utility package
				 */
				logger.info("Fetching data from properties file from utility package");
				topicName = props.getProperty("topicName");
			
				/**
				 * Connecting to Kafka server
				 * 
				 */
				logger.info("Connecting to Kafka server");
				properties.put(HbaseTableTriggerConstants.SERVER, props.getProperty("server"));
				properties.put(HbaseTableTriggerConstants.CLIENT_ID, props.getProperty("clientId"));
				properties.put(HbaseTableTriggerConstants.KEY_SERIALIZER, props.getProperty("keySerializer"));
				properties.put(HbaseTableTriggerConstants.VALUE_SERIALIZER, props.getProperty("valueSerializer"));
				
			} 
			catch (Exception e) 
			{
				logger.error(e);
				logger.warn(e);
			}
		}
		
		logger.info("Exiting getKafkaProperties()");
		return properties;
	}
	
	public static String getColumnInfo()
	{
		Properties props = HbaseTableTriggerUtility.getCoprocessorUtility();
		/**
    	 * getting columnInfo from config properties file
    	 */	
		columnInfo=props.getProperty("columnInfo");
		return columnInfo;
	}
	
}
