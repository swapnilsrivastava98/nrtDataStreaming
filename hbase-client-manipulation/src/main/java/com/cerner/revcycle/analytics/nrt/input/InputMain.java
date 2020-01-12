package com.cerner.revcycle.analytics.nrt.input;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.cerner.revcycle.analytics.nrt.utility.HbaseClientManipulationUtility;


/**
 * The InputMain class is used to insert data records into Hbase table
 * 
 * @author Swapnil Srivastava (SS067726)
 * @author Sanjana M (SM068520)
 * @author Uma Sivakumar (US067736)
 */
public class InputMain extends BaseRegionObserver{
	
	/**
	 * instantiating logger class
	 */
	final static Logger logger = Logger.getLogger(InputMain.class); 
	
	/**
     * declaring properties object
     */
	static Properties props;
	
	 /**
     * setting up Hbase connection     
     */
    static Connection connection = null;
    static Table table = null;
	
    public static void main(String[] args) {
    	
    	logger.info("inside main method of InputMain class");
    		
    	/**
    	 * calling getHbaseClientManipulationUtility method 
    	 * from HbaseClientManipulationUtility class and storing 
    	 * the object in table variable
    	 */
    	table = HbaseClientManipulationUtility.getHbaseClientManipulationUtility();
    	     	
	     	/**
	     	 * instantiating the object of input class
	     	 */
	        InputMain object = new InputMain();
	        
	        /**
	         * calling the insert method
	         */
			logger.info("calling insert method");
	        object.insertRecords();
	        
    	    logger.info("exited Main method of InputMain Class");
    }
    
    
    /**
     * Insert records into Hbase table
     */
    public void insertRecords() {
    	
    	logger.info("inside insertRecords Method");
    	
    	/**
    	 * calling variables from the HbaseClientManipulationUtility method
    	 */
    	String columnInfo = HbaseClientManipulationUtility.columnInfo;
    	String columnData = HbaseClientManipulationUtility.columnData;
			
			
		try {
			int counter=0;

			/**
			 * splitting the string columnData into string array columns
			 */
			String claimsData[] = columnData.split(",");
		
			logger.info("inside for loop for processing claims data");
			for(int i=0; i<100; i++)
			{   
		    	String itrObject = Integer.toString(i+1);
		    	/**
		    	 * fetching claims data columns
		    	 */
		        String claims[] ={ 
		    	   claimsData[0]+" "+itrObject, claimsData[1], claimsData[2], claimsData[3], claimsData[4], claimsData[5], claimsData[6]+" "+ itrObject  
		    	};
	            
	         
		     Put patient = new Put(Bytes.toBytes(claims[0]));  
		   	 
		     /**
		      * splitting the columnInfo into rows based upon the delimiter 
		      */
		 	 String columns [] = columnInfo.split("%%");	
	         String tName [];
	         String tFamily [];
	        
	     
			 for(int j=1; j<columns.length;j++){
				 
			   
			   Map<String,String> map = new HashMap<String,String>();
			  
			   /**
			    * splitting up the different rows into further rows of columnFamily and columnName  
			    */
			   tFamily = columns[j].split("::");
		
			   tName = tFamily[1].split("\\|\\|");
		 
			    
			   
			    for(int count=counter,k=0;k<tName.length;k++,count++){
			    	
			    /**
			     * mapping the key-value pairs	
			     */
			    map.put(tFamily[0],tName[k]);
			    
			    /**
			     * converting to Set so that we can traverse  
			     */
			    Set<Entry<String, String>> set=map.entrySet();
			  
	            Iterator<Entry<String, String>> itr=set.iterator();
	            
	            while(itr.hasNext()){  
	            
	           /**
		        * Converting to Map.Entry so that we can get key and value separately  
		        */
				@SuppressWarnings("rawtypes")
				Map.Entry entry=(Map.Entry)itr.next();  
	           
				/**
				 * converting the key-value to bytes for inserting into the table
				 */
	            byte key[] = entry.getKey().toString().getBytes();
	            byte value[] = entry.getValue().toString().getBytes();
	        
	            /**
	             * inserting the columns into table
	             */
	            logger.info("inserting the columns into table");
	            patient.addColumn(key, value , Bytes.toBytes(claims[count]));
	           }
		     }
			  counter=3;
	        } 
			counter=0;
                

	         try {
	        	   /**
	        	    * putting the table patient into Hbase table
	        	    */
	               logger.info("puts the table data the Hbase table");
					table.put(patient);
				} catch (IOException e) {
				    logger.error("error putting the table data in the Hbase table" + e);
				}
	         try {
	        	    /**
	        	     * putting data into Hbase in interval of 10 seconds
	        	     */
				    Thread.sleep(10000);
				} catch (InterruptedException e) {
					logger.error("error in thread" + e);
				}
	               
			}
                     
           }finally {    	   
	                if (table != null) {
	                    try {
							table.close();
						} catch (IOException e) {
							logger.info("Table not closed "+e);
						}
	                }
	                if (connection != null && !connection.isClosed()) {
	                    try {
							connection.close();
						} catch (IOException e) {
							logger.info("Connection not closed "+e);
						}	
	                }      
	        }  
           logger.info("exited insertRecords Method");
    }
}

