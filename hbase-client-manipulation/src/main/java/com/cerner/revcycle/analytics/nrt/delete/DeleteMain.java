package com.cerner.revcycle.analytics.nrt.delete;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.cerner.revcycle.analytics.nrt.utility.HbaseClientManipulationUtility;


/**
 * The DeleteMain class is used to delete records from Hbase table 
 * 
 * @author Swapnil Srivastava (SS067726)
 * @author Sanjana M (SM068520)
 * @author Uma Sivakumar (US067736)
 *
 */
public class DeleteMain extends BaseRegionObserver{
	
	/**
	 * instantiating logger class
	 */
	final static Logger logger = Logger.getLogger(DeleteMain.class); 
	
    /**
     * declaring properties object
     */
	static Properties props;
	
	 /**
     * setting up Hbase connection     
     */
    Connection connection = null;
    static Table table = null;
	
    public static void main(String[] args) {
    	logger.info("inside main method of DeleteMain class");
    	
    	/**
    	 * calling getHbaseClientManipulationUtility method 
    	 * from HbaseClientManipulationUtility class and storing 
    	 * the object in config variable
    	 */
    	table = HbaseClientManipulationUtility.getHbaseClientManipulationUtility();
		
    	 	
	     	/**
	     	 * instantiating the object of DeleteRecord class
	     	 */
	        DeleteMain object = new DeleteMain();
	        
	        /**
	         * calling the deleteRecord method
	         */
			logger.info("calling deleteRecords method");
	        object.deleteRecords();
	        
    	logger.info("exited main method of DeleteMain class");
    }
    
    
    /**
     * Deleting records from Hbase table
     */
    public void deleteRecords() {
    	logger.info("inside deleteRecords Method");
    	    
         try {
            
            /**
             * deleting claims data by the row number
             */
			logger.info("creates deletion operation for the specified row");
            Delete delete = new Delete(Bytes.toBytes(HbaseClientManipulationUtility.row));
          
            try {
            	logger.info("deleting claims data row");
				table.delete(delete);
			} catch (IOException e) {
				logger.error("error in deleting row" + e);
			}
           
        } finally {
            try {
                if (table != null) {
                	 try {
							table.close();
						} catch (IOException e) {
							logger.info("Table not closed"+e);
						}
                }
 
                if (connection != null && !connection.isClosed()) {
                	 try {
							connection.close();
						} catch (IOException e) {
							logger.info("Connection not closed"+e);
						}		
                }
            } catch (Exception e2) {
            	logger.error(e2);
 			    logger.warn(e2);
               
            }
        } 
        
         logger.info("exited deleteRecords Method");
    }
}


