package com.cerner.revcycle.analytics.nrt.utility;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * HbaseClientManipulationUtility class contains Utilities required for the Hbase table
 * for insertion/updation and deletion of records 
 * 
 * @author Swapnil Srivastava (SS067726)
 * @author Sanjana M (SM068520)
 * @author Uma Sivakumar (US067736)
 *
 */
public class HbaseClientManipulationUtility {
	
	/**
	 * declaring variables
	 */
	public static String tableName = null;
    static String zookeeperServer = null;
    static String zookeeperClientPort = null;
    public static String row = null;
    public static String columnInfo = null;
    public static String columnData = null;
	
    /**
     * adds Hbase configuration files to a configuration
     */
	static Configuration config = HBaseConfiguration.create();
	
	static Connection connection = null;
    static Table table = null;
	
	/**
	 * instantiating logger class
	 */
    final static Logger logger = Logger.getLogger(HbaseClientManipulationUtility.class); 
       
	public static Table getHbaseClientManipulationUtility()
	{
	    	logger.info("Inside Hbase Client Manipulation Utility method");
	    	
	    	/**
	    	 * instantiating Properties class
	    	 */
		    Properties props = new Properties();
		
			/**
			 * loading the logger properties
			 */
			logger.info("Loading the logger file");	
			try {
				props.load(new FileInputStream("/home/uma/eclipse-workspace/hbase-client-manipulation/src/main/resources/properties/log4j.properties"));
			} catch (FileNotFoundException e) {
				logger.error("Logger properties file not found" + e);
		
			} catch (IOException e) {
				logger.error("Logger properties I/O error" + e);
			}
			PropertyConfigurator.configure(props);	
			
			/**
        	 * loading the properties file 
        	 */
			logger.info("Loading the properties file");	
			FileInputStream input = null;
			try {
				input=new FileInputStream("/home/uma/eclipse-workspace/hbase-client-manipulation/src/main/resources/properties/config.properties");
			} catch (FileNotFoundException e) {
				logger.error("Logger properties file not found" + e);
			}
			try {
				props.load(input);
			} catch (IOException e) {
				logger.error("Logger properties I/O error" + e);
			}
			
			
			/**
		     * fetching data from properties file
		     */
		    tableName = props.getProperty("tableName");
		    zookeeperServer = props.getProperty("zookeeperServer");
		    zookeeperClientPort = props.getProperty("zookeeperClientPort"); 
		    row = props.getProperty("row");
		    columnInfo = props.getProperty("columnInfo");
		    columnData = props.getProperty("columnData");
		    
		    /**
	    	 * setting zookeeper configuration properties
	    	 */
			logger.info("Setting zookeeper configuration");
	        config.set("hbase.zookeeper.quorum", zookeeperServer);
	        config.set("hbase.zookeeper.property.clientPort", zookeeperClientPort );
	        
	    	logger.info("Establishing connection to Hbase");
            try {
				connection = ConnectionFactory.createConnection(config);
			} catch (IOException e) {
				logger.error("Error establishing Hbase connection I/O error" + e);
			}
            
            logger.info("Establishing connection to table");
            try {
				table = connection.getTable(TableName.valueOf(tableName));
			} catch (IOException e) {
				logger.error("Error establishing table connection I/O error" + e);
			}
	        
	        logger.info("Exited Hbase Client Manipulation Utility method");
			
			/**
			 * returns Table Object 
			 */
		    return table; 		   
	}
				
}

