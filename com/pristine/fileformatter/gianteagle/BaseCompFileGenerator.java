package com.pristine.fileformatter.gianteagle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class BaseCompFileGenerator {
	
	private static Logger logger = Logger.getLogger("BaseCompFileGenerator");
	private static String OUTPUT_FILE_PATH = "OUTPUT_PATH=";
	private static String REG_EFF_DATE = "REG_EFF_DATE=";
	private static String WEEK_START_DATE = "WEEK_START_DATE=";
	private static String STORE_NO = "STORE_NO=";
	private String outputPath;
	private String RefEffDate;
	private String WeekStartDate;
	private String StoreNo;
	private Connection _conn = null;
	private PrintWriter pw = null;
	private FileWriter fw = null;
	private String rootPath;
	private static int chainID;
	
	private BaseCompFileGenerator() {
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
	}
	
	public static void main(String[] args) throws IOException {
		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-base-comp-file-generator.properties");
		BaseCompFileGenerator baseCompFileGenerator = new BaseCompFileGenerator();
		
		for(String arg: args){
			if(arg.startsWith(OUTPUT_FILE_PATH)){
				baseCompFileGenerator.outputPath =  arg.substring(OUTPUT_FILE_PATH.length());
			}
			if(arg.startsWith(STORE_NO)){
				baseCompFileGenerator.StoreNo =  arg.substring(STORE_NO.length());
			}
			if (!(Constants.NEXT_WEEK.equalsIgnoreCase(args[2])
					|| Constants.CURRENT_WEEK.equalsIgnoreCase(args[2])
					|| Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[2]) || Constants.LAST_WEEK
						.equalsIgnoreCase(args[2]))) {
				logger.info("Invalid Argument, args[2] should be lastweek/currentweek/nextweek/specificweek");
				System.exit(-1);
			}
			
			String dateStr = null;
			Calendar c = Calendar.getInstance();
			c.set(Calendar.DAY_OF_WEEK, Calendar.THURSDAY);
			
			DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			
			if (Constants.CURRENT_WEEK.equalsIgnoreCase(args[2])) {
				dateStr = dateFormat.format(c.getTime());
			} else if (Constants.NEXT_WEEK.equalsIgnoreCase(args[2])) {
				c.add(Calendar.DATE, 7);
				dateStr = dateFormat.format(c.getTime());
		    } else if (Constants.LAST_WEEK.equalsIgnoreCase(args[2])) {
				c.add(Calendar.DATE, -7);
				dateStr = dateFormat.format(c.getTime());
			} else if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[2])) {
				dateStr = args[3];
			}
			
			baseCompFileGenerator.RefEffDate =  dateStr;
			baseCompFileGenerator.WeekStartDate =  dateStr;
			chainID = Integer.parseInt(PropertyManager.getProperty("SUBSCRIBER_CHAIN_ID"));
			
			logger.debug("RefEffDate - " + baseCompFileGenerator.RefEffDate);
			logger.debug("WeekStartDate - " + baseCompFileGenerator.WeekStartDate);
		}
		baseCompFileGenerator.generateCompFile();
	}
	
	private void generateCompFile() throws IOException{
		
		Logger logger = Logger.getLogger("generateCompFile");
		
		RetailPriceDAO objRetailPriceDao = new RetailPriceDAO();
		List<RetailPriceDTO> priceList = null;
		
		try {
			
			_conn = DBManager.getConnection();
			
			outputPath = rootPath + "/" + outputPath + "/" + "BaseStoreCompData.txt";
			
			File file = new File(outputPath);
			
			if (!file.exists())
				fw = new FileWriter(outputPath);
			else
				fw = new FileWriter(outputPath, true);
			 
			 pw = new PrintWriter(fw);
			 
			 priceList = objRetailPriceDao.getBaseStoreCompDetails(_conn, WeekStartDate);
			 
			 HashMap<String, List<RetailPriceDTO>> groupByItem = (HashMap<String, List<RetailPriceDTO>>) priceList.stream().collect(Collectors.groupingBy(RetailPriceDTO::getItemcode));
			 
			 List<RetailPriceDTO> finalList = new ArrayList<>();
			 groupByItem.forEach((itemCode, priceCol) -> {
				 if(priceCol.size() > 1){
					 priceCol.forEach(price -> {
						 //logger.debug("Item Code - " + itemCode + " -- " + price.getLevelTypeId() + " -- " + Constants.ZONE_LEVEL_TYPE_ID);
						 if(price.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID){
							 finalList.add(price);
						 }
					 });
				 }else{
					 finalList.addAll(priceCol);
				 }
			 });
			 
			for (int j = 1; j < finalList.size(); j++) {
				 
				 RetailPriceDTO priceListlst = finalList.get(j);
				 
				    pw.print(StoreNo); // Store Number # 1
					pw.print("|");
					pw.print(RefEffDate);// Reg Price Effective Date
					pw.print("|");
					pw.print(priceListlst.getRetailerItemCode());// Retailer Item Code
					pw.print("|");
					pw.print("");
					pw.print("|");
					pw.print(priceListlst.getRegQty());// Regular Quantity
					pw.print("|");
					pw.print(priceListlst.getRegPrice());// Regular Price
					pw.print("|");
					pw.print("");
					pw.print("|");
					pw.print(priceListlst.getUpc());// UPC
					pw.print("|");
					pw.print("N");
					pw.print("|");
					pw.print("");
					pw.print("|");
					if(priceListlst.getSaleQty() > 0)
					{
						pw.print(priceListlst.getSaleQty());// Sale Quantity
					}
					else
					{
						pw.print("");
					}
					pw.print("|");
					if(priceListlst.getSalePrice() > 0)
					{
						pw.print(priceListlst.getSalePrice());// Sale Price
					}
					else
					{
						pw.print("");
					}
					pw.print("|");
					if(priceListlst.getSalePrice() > 0)
					{
						pw.print(RefEffDate);;// Sale Price Effective Date
					}
					else
					{
						pw.print("");
					}
					pw.println("       "); // spaces
			 }
			 
			    pw.flush();
				fw.flush();
				pw.close();
				fw.close();
			 
			 
		} catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			System.exit(1);
		}
		finally {
			PristineDBUtil.close(_conn);
		}	
	 }
}
