package com.pristine.dataload.offermgmt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.ArrayList;
//import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

public class ExportPrices {
	private static Logger logger = Logger.getLogger("ExportPrices");
	
	private Connection conn = null;
	private String weekStartDate = null;
	
	public ExportPrices(){
		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-export-prices.properties");
		if(conn == null){
			try {
				conn = DBManager.getConnection();
				String date = DateUtil.dateToString(DateUtil.getNextSunday(new Date()), Constants.APP_DATE_FORMAT);
				System.out.println(date);
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}
	
	public static void main(String[] args) {
		ExportPrices export = new ExportPrices();
		int runId = -1;
		if(args.length < 1){
			System.out.println("Run Id is Mandatory");
		}else{
			try{
				runId = Integer.parseInt(args[0]);
			}catch(Exception e){
				System.out.println("Please enter an integer value");
				e.printStackTrace();
			}
		}
		export.export(runId);
	}
	
	public void export(int runId){
		try{
			PricingEngineDAO peDAO = new PricingEngineDAO();
			ArrayList<PRItemDTO> recommendationList = peDAO.getRecommendation(conn, runId);
			PRStrategyDTO strategy = peDAO.getStrategyDetails(conn, new Long(recommendationList.get(0).getStrategyId()).intValue());
			
			List<String> compStores = new ArrayList<String>();
			if(strategy.getLocationLevelId() == Constants.STORE_LEVEL_ID){
				StoreDAO storeDAO = new StoreDAO();
				StoreDTO sDTO = storeDAO.getStoreInfo(conn, strategy.getLocationId());
				compStores.add(sDTO.strNum);
			}else{
				RetailPriceZoneDAO zoneDAO = new RetailPriceZoneDAO();
				compStores = zoneDAO.getStoresInZone(conn, strategy.getLocationId());
			}
			logger.info("Current Dir - " + System.getProperty("user.dir"));
			for(String compStrNo : compStores){
				String filePath = System.getProperty("user.dir") + "\\Files\\IMS" + "_" + compStrNo + "_" + runId + ".csv";
				FileWriter fw = null;
				PrintWriter pw = null;
				File file = new File(filePath);
				if (!file.exists())
					fw = new FileWriter(filePath);
				else
					fw = new FileWriter(filePath, true);
							    
				pw = new PrintWriter(fw);
				
				for(PRItemDTO item : recommendationList){
					writePrice(pw, item, compStrNo, runId, strategy.getStartCalendarId());
				}
				
				pw.flush();
				fw.flush();
				pw.close();
				fw.close();
			}
		}catch(IOException exception){
			logger.error("Error in writing prices - " + exception);
		}catch(GeneralException exception){
			logger.error("Error in exporting prices - " + exception);
		}
	}
	
	private void writePrice(PrintWriter pw, PRItemDTO item, String compStrNo, int runId, int strategyStartCalendarId) {
		pw.print(padLeading(compStrNo, 6));
		pw.print("|");
		pw.print(padLeading(item.getRetailerItemCode(), 10));
		pw.print("_003|");
//		pw.print(padLeading(String.valueOf(item.getRecommendedRegMultiple()), 2));
		pw.print(padLeading(String.valueOf(item.getRecommendedRegPrice().multiple), 2));
		pw.print("|");
		String price = String.valueOf(item.getRecommendedRegPrice());
		pw.print(padLeading(price.substring(0, price.indexOf(".")), 4) + "." + padTrailing(price.substring(price.indexOf(".") + 1, price.length()), 2));
		pw.print("|");
		if(weekStartDate == null)
			weekStartDate = getDate(strategyStartCalendarId);
		pw.print(weekStartDate);
		pw.print("|");
		pw.println(padLeading(String.valueOf(runId), 5));
	}
	
	public static String padLeading(String text, int padLength) {
		String retText = text;
		if(text.length() < padLength){	
			StringBuffer strNoPad = new StringBuffer (); 
			for ( int i = 0; i < padLength - text.length() ; i++)
				strNoPad.append('0');
			retText = strNoPad.toString() + text;
		}
		return retText;
	}
	
   public static String padTrailing(String text, int padLength) {
		String retText = text;
	
		if(text.length() < padLength){
			StringBuffer strNoPad = new StringBuffer (); 
			for ( int i = 0; i < padLength - text.length() ; i++)
				strNoPad.append('0');
			retText = text + strNoPad.toString();
		}
		
		return retText;
   }

   public String getDate(int strategyStartCalendarId) {
	   String date = "";
	   try{
		   RetailCalendarDAO rDAO = new RetailCalendarDAO();
		   RetailCalendarDTO rDto = rDAO.getCalendarDetail(conn, strategyStartCalendarId);
		   String sStartDate = rDto.getStartDate();
		   if(PrestoUtil.compareTo(DateUtil.toDate(sStartDate), new Date()) > 0){
			   date = sStartDate;
		   }else{
			   date = DateUtil.dateToString(DateUtil.getNextSunday(new Date()), Constants.APP_DATE_FORMAT);
		   }
	   }catch(GeneralException exception){
		   logger.error("Error when retrieving calendar details - " + exception);
	   }
	   return date;
   }
}
