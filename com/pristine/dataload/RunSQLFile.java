package com.pristine.dataload;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class RunSQLFile {
	private static Logger  logger = Logger.getLogger("ProductGrouping");
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		PropertyConfigurator.configure("log4j.properties");
		PropertyManager.initialize("analysis.properties");
		logger.info("Running SQL File .....");
		
		if ( args.length > 2 ){
			logger.info("Incorrect parameters - only file name is input");
			System.exit(-1);
		}
		
		Connection conn = null;
		try{
			conn = DBManager.getConnection();
			boolean commitFlag = false;
			if( PropertyManager.getProperty("DATALOAD.COMMIT", "").equalsIgnoreCase("TRUE"))
				commitFlag = true;
			
			processFile( args[0], conn, commitFlag);
			
			if( commitFlag){
				logger.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, "Data Load");
			}
			else{
				logger.info("Rolling back transacation");
				PristineDBUtil.rollbackTransaction(conn, "Data Load");
			}
		}catch(GeneralException ge){
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			logger.error("General Exception in Analysis", ge);
		}catch(Exception ge){
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			logger.error("Runtime Exception", ge);
		}finally{
			PristineDBUtil.close(conn);
			logger.info("Product Grouping Analysis completed");
		}
		
	}
	
	private static void processFile( String fileName, Connection conn, boolean commitFlag) 
		throws Exception, GeneralException {
		
		FileInputStream fstream = new FileInputStream(fileName);
	    // Get the object of DataInputStream
	    DataInputStream in = new DataInputStream(fstream);
	        BufferedReader br = new BufferedReader(new InputStreamReader(in));
	    String strLine;
	    //Read File Line By Line
	    int count = 0;
	    while ((strLine = br.readLine()) != null)   {
	      // Print the content on the console
	      //System.out.println (strLine);
	      StringBuffer sb = new StringBuffer(strLine.replace(';', ' '));
	     
	      logger.info( "Processing: " + sb.toString());
	      PristineDBUtil.execute(conn, sb, "Processing SQL File");
	      count++;
	      if( count%1 ==0){
	    	  logger.info(count + " no of records processed");
	    	  if( commitFlag)
	    		  PristineDBUtil.commitTransaction(conn, "Data Load");
	      }
//	      if ( count == 5)
//	    	  break;
	    }
	    //Close the input stream
	    in.close();
	    
	}

}
