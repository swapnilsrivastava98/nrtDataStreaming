package com.cerner.revcycle.analytics.nrt.hbaseTableTrigger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.cerner.revcycle.analytics.nrt.hbaseTableTriggerUtility.HbaseTableTriggerUtility;

/**
 * The Class HbaseTableTrigger. HbaseTableTrigger triggers the coprocessor
 * whenever data is inserted,updated or deleted in the Hbase table
 * 
 * 
 * @author Sanjana M (SM068520)
 * @author Uma Sivakumar (US067736)
 * @author Swapnil Srivastava (SS067726)
 *
 */
public class HbaseTableTrigger extends BaseRegionObserver {

	final static Logger logger = Logger.getLogger(HbaseTableTrigger.class);
	
	/**
	 * initializing kafka producer
	 */
	public KafkaProducer<String, String> kafkaProducer = null;

	/**
	 * instantiating Json object
	 */
	JSONObject jsonObject = new JSONObject();
	
	/**
	 * fetching columnInfo details from utility package
	 */
	String columnInfo = HbaseTableTriggerUtility.getColumnInfo();
	
	/**
	 * Post put triggered when insertion occurs in Hbase PostPut method is triggered
	 * after data is inserted/modified after running the input class. put object
	 * contains the new data to be pushed to table
	 *
	 * @param en         the environment object
	 * @param put        the put object
	 * @param edit       the edit
	 * @param durability the durability
	 * @throws IOException      Signals that an I/O exception has occurred
	 * @throws ConnectException Signal that a Kafka connection error has occurred.
	 */
	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> en, Put put, WALEdit edit, Durability durability)
			throws IOException, ConnectException {
		RegionCoprocessorEnvironment env = en.getEnvironment();
		logger.info("entering postput()");
		
		/**
		 * inserted/updated components are sent into the kafka topic
		 */			
		logger.info("setting up kafka connection by calling getKafkaProperties method from utiity package");
		try {
			kafkaProducer = new KafkaProducer<String,String>(HbaseTableTriggerUtility.getKafkaProperties());
		} catch (ConnectException e) {
			logger.error("Error: Could not connect to Kafka server");
		}

		String tName[];
		String tFamily[];	
		/**
		 * splitting the columnInfo into 2 rows containing column families and column data
		 */
		String columns[] = columnInfo.split("%%");
		
		/**
		 * Hbase table information is set up for insertion/updation
		 */
		logger.info("Setting up Hbase table");
		/**
		 * getting the inserted/updated data into the row
		 */
		byte[] row = put.getRow();
		Get get = new Get(row);

		for (int j = 1; j < columns.length; j++) {
		
			/**
			 * creating the HashMap of column family and column data pair
			 */
			Map<String, String> map = new HashMap<String, String>();
			
			byte key[] = new byte[10];
			byte value[] = new byte[10];

			/**
			 * splitting up the different rows into further rows of columnFamily and
			 * columnName
			 */
			tFamily = columns[j].split("::");
			tName = tFamily[1].split("\\|\\|");

			/**
			 * getting the column family after splitting the columnInfo into column family and column names
			 */
			get.addFamily(Bytes.toBytes(tFamily[0]));

			for (int k = 0; k < tName.length; k++) 
			{
				
				/**
				 * mapping the key-value pairs
				 */
				map.put(tFamily[0], tName[k]);

				/**
				 * converting to Set so that we can traverse
				 */
				Set<Entry<String, String>> set = map.entrySet();

				/**
				 * iterating through the set of key-value pairs
				 */
				Iterator<Entry<String, String>> itr = set.iterator();

				while (itr.hasNext()) 
				{
					/**
					 * Converting to Map.Entry so that we can get key and value separately
					 */
					@SuppressWarnings("rawtypes")
					Map.Entry entry = (Map.Entry) itr.next();

					/**
					 * converting the key-value to bytes for inserting/updating into the table
					 */
					key = entry.getKey().toString().getBytes();
					value = entry.getValue().toString().getBytes();
					
					/**
					 * getting the column containing key-value pairs
					 */
					get.addColumn(key, value);
                    
				}
				
				/**
				 * inserted/updated data in bytes is stored in the result object
				 */
				logger.info("Retrieving inserted/updated data from Hbase table");
				Result result = null;
				try {
					result = env.getRegion().get(get);
				} catch (IOException e) {
					logger.error("Error: Could not retrieve data from Hbase Table");
				}

				
					
					byte[] patientValue = result.getValue(key, value);
					/**
					 * converting the result data into string
					 */
					String patient = Bytes.toString(patientValue);
					
	                /**
	                 * putting the inserted/updated data into JsonObject instance
	                 */		
					logger.info("putting data into jsonObject instance");
					jsonObject.put(Bytes.toString(value), patient);		
			}
		}

		try {
			/**
			 * inserted/updated components are sent into the kafka topic
			 */
			logger.error("Sending inserted/updated data to Kafka topic");
			kafkaProducer.send(
					new ProducerRecord<String, String>(HbaseTableTriggerUtility.topicName, jsonObject.toString()));
		} catch (ConnectException e) {
			logger.error("Error: Could not send inserted/updated data to the Kafka Topic");
		}

		/**
		 * closing the kafka connection
		 */
		kafkaProducer.close();
		
		logger.info("exiting postput()");
	}
	
	/**
	 * 
	 * preDelete put triggered when insertion occurs in Hbase preDelete method is
	 * triggered when deletion occurs in Hbase put object contains the new data to
	 * be pushed to table
	 *
	 * @param en         the environment object
	 * @param put        the put object
	 * @param edit       the edit
	 * @param durability the durability
	 * @throws IOException Signals that an I/O exception has occurred.
	 * 
	 */
	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> en, Delete delete, WALEdit edit, Durability durability) throws ConnectException {
		RegionCoprocessorEnvironment env = en.getEnvironment();
		
	logger.info("entering preDelete()");
		
		/**
		 * deleted components are sent into the kafka topic
		 */			
		logger.info("setting up kafka connection by calling getKafkaProperties method from utiity package");
		try {
			kafkaProducer = new KafkaProducer<String,String>(HbaseTableTriggerUtility.getKafkaProperties());
		} catch (ConnectException e) {
			logger.error("Error: Could not connect to Kafka server");
		}

		String tName[];
		String tFamily[];	
		/**
		 * splitting the columnInfo into 2 rows containing column families and column data
		 */
		String columns[] = columnInfo.split("%%");
		
		/**
		 * Hbase table information is set up for deletion
		 */
		logger.info("Setting up Hbase table");
		/**
		 * getting the deleted data into the row
		 */
		byte[] row = delete.getRow();
		Get get = new Get(row);

		for (int j = 1; j < columns.length; j++) {
		
			/**
			 * creating the HashMap of column family and column data pair
			 */
			Map<String, String> map = new HashMap<String, String>();
			
			byte key[] = new byte[10];
			byte value[] = new byte[10];

			/**
			 * splitting up the different rows into further rows of columnFamily and
			 * columnName
			 */
			tFamily = columns[j].split("::");
			tName = tFamily[1].split("\\|\\|");

			/**
			 * getting the column family after splitting the columnInfo into column family and column names
			 */
			get.addFamily(Bytes.toBytes(tFamily[0]));

			for (int k = 0; k < tName.length; k++) 
			{
				
				/**
				 * mapping the key-value pairs
				 */
				map.put(tFamily[0], tName[k]);

				/**
				 * converting to Set so that we can traverse
				 */
				Set<Entry<String, String>> set = map.entrySet();

				/**
				 * iterating through the set of key-value pairs
				 */
				Iterator<Entry<String, String>> itr = set.iterator();

				while (itr.hasNext()) 
				{
					/**
					 * Converting to Map.Entry so that we can get key and value separately
					 */
					@SuppressWarnings("rawtypes")
					Map.Entry entry = (Map.Entry) itr.next();

					/**
					 * converting the key-value to bytes for deleting from the table
					 */
					key = entry.getKey().toString().getBytes();
					value = entry.getValue().toString().getBytes();
					
					/**
					 * getting the column containing key-value pairs
					 */
					get.addColumn(key, value);
                    
				}
				
				/**
				 * deleted data in bytes is stored in the result object
				 */
				logger.info("Retrieving data for deletion from Hbase table");
				Result result = null;
				try {
					result = env.getRegion().get(get);
				} catch (IOException e) {
					logger.error("Error: Could not retrieve data for deletion from Hbase Table");
				}

				
					
					byte[] patientValue = result.getValue(key, value);
					/**
					 * converting the result data into string
					 */
					String patient = Bytes.toString(patientValue);
					
	                /**
	                 * putting the data for deletion into JsonObject instance
	                 */		
					logger.info("putting data for deletion into jsonObject instance");
					jsonObject.put(Bytes.toString(value), patient);		
			}
		}

		try {
			/**
			 * deleted components are sent into the kafka topic
			 */
			logger.error("Sending deleted data to Kafka topic");
			kafkaProducer.send(
					new ProducerRecord<String, String>(HbaseTableTriggerUtility.topicName, jsonObject.toString()));
		} catch (ConnectException e) {
			logger.error("Error: Could not send inserted/updated data to the Kafka Topic");
		}

		/**
		 * closing the kafka connection
		 */
		kafkaProducer.close();
		
		logger.info("exiting preDelete()");
	}


}