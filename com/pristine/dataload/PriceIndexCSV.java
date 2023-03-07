package com.pristine.dataload;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PriceIndexCSV {
	

	private static Logger  logger = Logger.getLogger("TopsDataLoad");

	ArrayList<CompetitiveDataDTO> baseCompDataList = new ArrayList<CompetitiveDataDTO>();
	HashMap <Integer, CompetitiveDataDTO> compStoreDataMap = new HashMap <Integer, CompetitiveDataDTO> (); 
	ScheduleDAO schDAO = null;
	CompetitiveDataDAO compDAO = null;
	
	static Connection conn = null;
	static int baseStoreId;
	static String baseStartDate;
	static String baseEndDate;
	
	static int compStoreId;
	static String compStartDate;
	static String compEndDate;
	
	static boolean detail = false;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j.properties");
		logger.info("Price Index CSV started");
		PropertyManager.initialize("analysis.properties");
		
		try{
			conn = DBManager.getConnection();
			
			if (args.length >= 5){
				baseStoreId = Integer.parseInt(args[0]);
				baseStartDate = args[1];
				baseEndDate = args[2];
				
				compStoreId = Integer.parseInt(args[3]);
				compStartDate = args[4];
				compEndDate = baseEndDate;
				
				if( args.length > 5)
					detail = true;
				PriceIndexCSV indexCSV= new PriceIndexCSV();
				indexCSV.generateIndexCSV();
				logger.info("Price Index CSV done");
						
			}else{
				logger.info(" PriceIndexCSV baseStrId baseStartDate baseEndDate compStrId CompStartDate [detail] ");
			}
			
		

			
		}catch(GeneralException ge){
			logger.error("Error in Load", ge);
			System.exit(1);
		}catch(IOException ioe){
			logger.error("File Exception", ioe);
			System.exit(1);
		}
		finally{
			PristineDBUtil.close(conn);
		}
	}
	
	private void generateIndexCSV() throws GeneralException, IOException{
		
		schDAO = new ScheduleDAO ();
		compDAO = new CompetitiveDataDAO (conn); 
		
		//get the Base Store schedule id
		ArrayList<ScheduleInfoDTO> baseSchList = schDAO.getSchedulesForStore(conn, baseStoreId, -1, baseStartDate, baseEndDate);
		
		//get the comp Store schedule Id
		ArrayList<ScheduleInfoDTO> compSchList = schDAO.getSchedulesForStore(conn, compStoreId, -1, compStartDate, compEndDate);
		
		logger.info("Size of Base schedule " + baseSchList.size());
		logger.info("Size of Comp schedule " + compSchList.size());
		
		//get the base store compData
		if( baseSchList.size() > 0){
			int baseScheduleId = baseSchList.get(0).getScheduleId();
			
			baseCompDataList = compDAO.getCompDataForDebug(conn, baseScheduleId, -1, -1, -1, false);
			logger.info("Size of Base Store - Data Sch: " + baseScheduleId + " Size: " + baseCompDataList.size());
			
			for (ScheduleInfoDTO compSchedule : compSchList) {
				ArrayList<CompetitiveDataDTO> compStrDataList = compDAO.getCompDataForDebug(conn, compSchedule.getScheduleId(), -1, -1, -1, false);
				logger.info("Size of Comp Store - Data Sch: " + compSchedule.getScheduleId() + " Size: " + compStrDataList.size());
				
				for (CompetitiveDataDTO compStrData : compStrDataList) {
					
					if( !compStoreDataMap.containsKey(compStrData.itemcode))
						compStoreDataMap.put(compStrData.itemcode, compStrData);
				}
			}
			
		}
		//Print header for CSV
		String fileName = detail? "PriceIndexDetail.csv":"PriceIndex.csv";
		BufferedWriter out = new BufferedWriter(new FileWriter("C:/Presto/" + fileName));
		//Base Store, dept, item name, size, uom, retailer_item_code, upc, reg, sale, movement, comp str reg, sale 
	    out.write("Base Store, Dept, Item, Size, UOM,  Lir Id, Item Code, UPC,  Reg, Sale, Lesser Price, Movement, Comp Str, Reg, Sale, Lesser Price \r\n");
	    

		
		for (CompetitiveDataDTO baseStrData : baseCompDataList) {
			if( compStoreDataMap.containsKey(baseStrData.itemcode)){
				CompetitiveDataDTO compStrData = compStoreDataMap.get(baseStrData.itemcode);
				if( baseStrData.lirId > 0 && detail){
					printAtItemLevel(out,baseStrData, compStrData);
				}
				else
					printToCSV(out, baseStrData,compStrData);
			}
				
		}
		
		//get the CompStore data
		//populate the hashmap
		
		//iterate through the BaseStore
		//Print if the element exists in both stores
		//if retlirId > 0, then print 
		
		
		out.close();
		
	}
	
	private void printAtItemLevel(BufferedWriter out, CompetitiveDataDTO baseStrData, CompetitiveDataDTO compStrData) 
		throws IOException, GeneralException{
		
		ArrayList<CompetitiveDataDTO> compDataItemList = 
				compDAO.getCompDataForDebug(conn, baseStrData.scheduleId, -1, -1, baseStrData.lirId, true);
		
		for (CompetitiveDataDTO baseStrItemData : compDataItemList) 
			printToCSV( out, baseStrItemData, compStrData);
	}
	
	//Base Store, dept, item name, size, uom, retailer_item_code, upc, reg, sale, movement, comp str reg, sale
	private void printToCSV(BufferedWriter out, CompetitiveDataDTO baseStrData, CompetitiveDataDTO compStrData) throws IOException {
		
		StringBuffer sb = new StringBuffer();
		sb.append(baseStrData.compStrNo).append(",");
		sb.append(baseStrData.deptName).append(",");
		if( baseStrData.itemName.charAt(0) == '+')
			baseStrData.itemName = baseStrData.itemName.substring(1);
		sb.append(baseStrData.itemName).append(",");
		sb.append(baseStrData.size).append(",");
		sb.append(baseStrData.uom).append(",");
		if( baseStrData.lirId > 0)
			sb.append(baseStrData.lirId);
		sb.append(",");
		sb.append(baseStrData.retailerItemCode).append(",");
		sb.append(baseStrData.upc).append(",");
		sb.append(baseStrData.regPrice).append(",");
		sb.append(baseStrData.fSalePrice).append(",");
		if( baseStrData.fSalePrice > 0 && baseStrData.fSalePrice < baseStrData.regPrice)
			sb.append(baseStrData.fSalePrice).append(",");
		else
			sb.append(baseStrData.regPrice).append(",");

		sb.append(baseStrData.quantitySold).append(",");
		
		sb.append(compStrData.compStrNo).append(",");
		sb.append(compStrData.regPrice).append(",");
		sb.append(compStrData.fSalePrice).append(",");
		if( compStrData.fSalePrice > 0 && compStrData.fSalePrice < compStrData.regPrice)
			sb.append(compStrData.fSalePrice).append("");
		else
			sb.append(compStrData.regPrice).append("");
		
		sb.append("\r\n");
		out.write(sb.toString());
	}
}
