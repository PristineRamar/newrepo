package com.pristine.fileformatter.autozone;

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
import com.pristine.fileformatter.autozone.BaseZoneCompFileGenerator;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class BaseZoneCompFileGenerator {
	
	private static Logger logger = Logger.getLogger("BaseZoneCompFileGenerator");
	private static String OUTPUT_FILE_PATH = "OUTPUT_PATH=";
	private static String REG_EFF_DATE = "REG_EFF_DATE=";
	private static String WEEK_START_DATE = "WEEK_START_DATE=";
	private static String STORE_NO = "STORE_NO=";
	private static String ZONE_NO = "ZONE_NO=";
	private String outputPath;
	private String RefEffDate;
	private String WeekStartDate;
	private String StoreNo;
	private String ZoneNo;
	private Connection _conn = null;
	private PrintWriter pw = null;
	private FileWriter fw = null;
	private String rootPath;
	private static int chainID;
	
	private BaseZoneCompFileGenerator() {
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
	}

	public static void main(String[] args) throws IOException {

		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-base-zone-comp-file-generator.properties");
		BaseZoneCompFileGenerator baseZoneCompFileGenerator = new BaseZoneCompFileGenerator();
		
		for(String arg: args){
			if(arg.startsWith(OUTPUT_FILE_PATH)){
				baseZoneCompFileGenerator.outputPath =  arg.substring(OUTPUT_FILE_PATH.length());
			}
			if(arg.startsWith(STORE_NO)){
				baseZoneCompFileGenerator.StoreNo =  arg.substring(STORE_NO.length());
			}
			if(arg.startsWith(ZONE_NO)){
				baseZoneCompFileGenerator.ZoneNo =  arg.substring(ZONE_NO.length());
			}
			
			if (!(Constants.NEXT_WEEK.equalsIgnoreCase(args[3])
					|| Constants.CURRENT_WEEK.equalsIgnoreCase(args[3])
					|| Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[3]) || Constants.LAST_WEEK
						.equalsIgnoreCase(args[3]))) {
				logger.info("Invalid Argument, args[3] should be lastweek/currentweek/nextweek/specificweek");
				System.exit(-1);
			}
			
			String dateStr = null;
			Calendar c = Calendar.getInstance();
			int dayIndex = 0;
			if(dayIndex > 0)
				c.set(Calendar.DAY_OF_WEEK, dayIndex + 1);
			else
				c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
			
			DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			
			if (Constants.CURRENT_WEEK.equalsIgnoreCase(args[3])) {
				dateStr = dateFormat.format(c.getTime());
			} else if (Constants.NEXT_WEEK.equalsIgnoreCase(args[3])) {
				c.add(Calendar.DATE, 7);
				dateStr = dateFormat.format(c.getTime());
		    } else if (Constants.LAST_WEEK.equalsIgnoreCase(args[3])) {
				c.add(Calendar.DATE, -7);
				dateStr = dateFormat.format(c.getTime());
			} else if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[3])) {
				dateStr = args[4];
			}
			
			baseZoneCompFileGenerator.RefEffDate =  dateStr;
			baseZoneCompFileGenerator.WeekStartDate =  dateStr;
			chainID = Integer.parseInt(PropertyManager.getProperty("SUBSCRIBER_CHAIN_ID"));
			
			logger.debug("RefEffDate - " + baseZoneCompFileGenerator.RefEffDate);
			logger.debug("WeekStartDate - " + baseZoneCompFileGenerator.WeekStartDate);
			logger.debug("ZoneNo - " + baseZoneCompFileGenerator.ZoneNo);
			logger.debug("chainID - " + chainID);
			
		}
		
		baseZoneCompFileGenerator.generateCompFile();

	}
	
	private void generateCompFile() throws IOException{
        Logger logger = Logger.getLogger("generateCompFile");
		
		RetailPriceDAO objRetailPriceDao = new RetailPriceDAO();
		List<RetailPriceDTO> priceList = null;
		
		try {

			_conn = DBManager.getConnection();
			
			outputPath = rootPath + "/" + outputPath + "/" + "BaseZoneCompData.csv";
			
			File file = new File(outputPath);
			
			if (!file.exists())
				fw = new FileWriter(outputPath);
			else
				fw = new FileWriter(outputPath, true);
			 
			 pw = new PrintWriter(fw);
			 
			 priceList = objRetailPriceDao.getBaseZoneCompDetails(_conn, WeekStartDate, ZoneNo, chainID);
			 
			 pw.println("comp_Store|price_check_date|item|item_desc|regular_qty|regular_retail|item_size|upc|outside_indicator|additonal_info|sale_qty|sale_retail|sale_date|part_number");
			 //pw.println("       ");
			 
			 for (int j = 1; j < priceList.size(); j++) {
				 
				 RetailPriceDTO priceListlst = priceList.get(j);
				 
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
				 pw.print("");
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
				pw.print("|");
				pw.print("");
				pw.println(""); // spaces
			 }
			 
			pw.flush();
			fw.flush();
			pw.close();
			fw.close();
		
		}catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			System.exit(1);
		}
		finally {
			PristineDBUtil.close(_conn);
		}	
	}

}
