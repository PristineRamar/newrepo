package com.pristine.service.offermgmt.promotion;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.promotion.PromoEngineDataDTO;

public class DatabasePingThread extends Thread{
	
	private static Logger logger = Logger.getLogger("DummyThread");
	private int TIMEOUT = 5 ; //4 hours
	
	private Connection conn;
	private PromoEngineDataDTO promoDTO;
	//private volatile boolean isPredictionComplete = false;
	
	public DatabasePingThread(Connection conn, PromoEngineDataDTO promoDTO){
		this.conn = conn;
		this.promoDTO = promoDTO;
	};
	
	public void run(){
		
		//To implement a timeout on very long process 
		// Current set as 4 hours for prediction process
		long startTime = System.currentTimeMillis();

	       System.out.println("Dummy Thread for databse ping started.");  
//	       while(!promoDTO.isPredictionComplete){
//	    	   try {
//	    		   Thread.sleep(45000);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//	    	   long estimatedTime = System.currentTimeMillis() - startTime;
//	    	   if(estimatedTime > (TIMEOUT*60*60*1000)){
//	    		   logger.debug("The Thread is exiting as Prediction is exceeding more than 4 hours");
//	    		  break;
//	    	   }
//	    	   
//	    	   pingDatabase();
//	       }  
	       logger.debug("Dummy Thread for databse ping ended."); 
	}
	
	private void pingDatabase(){
			PreparedStatement stmt = null;
//			try {
//				logger.debug("pinging Database ..");
//				// At product major category level, location level can be storeList or Zone
//				String query = "SELECT 1 FROM DUAL";
//				stmt = conn.prepareStatement(query);
//				ResultSet rs = stmt.executeQuery();
//				rs.close();
//				stmt.close();
//			} catch (Exception e) {
//				logger.error("Error while pinging database..");
//				promoDTO.isPredictionComplete = true;
//			} 
	}
}
