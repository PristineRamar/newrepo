/**
 * 
 */
package com.pristine.fileformatter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.fileformatter.AholdMovementFile;
import com.pristine.exception.GeneralException;
import com.pristine.oos.MovementExpectation;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * @author Nagarajan 
 * Converts Ahold movement file to pristine standard retail
 * price and retail cost format
 */
public class AholdMovFileToRetailPriceAndCost extends PristineFileParser {

	private static Logger logger = Logger
			.getLogger("log4j-AholdMovFileToRetailPriceAndCost.properties");
	static HashMap<Integer, String> allColumns = new HashMap<Integer, String>();
	private static int stopCount;
	private static String inputRelativePath, retailPriceOutputPath, retailCostOutputPath, weekDescription;
	private String rootPath;
	private String processType = "";
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private static boolean specificWeek =false;
	private int ignoredItemCount = 0;
	private int totalItemCount = 0;
	
	private boolean writePriceIntoDB = false;
	
	private Connection conn = null;

	/**
	 * @param args
	 * @throws GeneralException
	 * @throws IOException
	 */
	public static void main(String[] args) throws GeneralException, IOException {
		// TODO Auto-generated method stub
		String fileFullPath;
		PropertyConfigurator.configure("log4j-AholdMovFileToRetailPriceAndCost.properties");
		stopCount = Integer.parseInt(PropertyManager.getProperty(
				"DATALOAD.STOP_AFTER", "-1"));

		if (args.length < 2) {
			logger.info("Invalid Arguments,  args[0] - Movement File Relative Path, args[1] - Retail Price Output File Relative Path, " +
					 "args[2] - Retail Cost Output File Relative Path, " +
					"args[3] - Week Description as in movement file");
			System.exit(-1);
		}
		AholdMovFileToRetailPriceAndCost aholdMovFileToRetailPrice = new AholdMovFileToRetailPriceAndCost();
		inputRelativePath = args[0];
		retailPriceOutputPath = args[1];
		retailCostOutputPath = args[2];
		if( args.length == 4 && args[3]!="")
		{
			specificWeek = true;
			weekDescription = args[3];
		}
		aholdMovFileToRetailPrice.processFile();
	}

	/***
	 * Fill all possible columns of the csv file with key
	 */
	private void fillAllColumns() {
		allColumns.put(1, "CUSTOM_ATTR2_DESC");
		allColumns.put(2, "UPC_CD");
		allColumns.put(3, "LIST_MEMBER");		
		allColumns.put(4, "OPCO_LOC_CD");
		allColumns.put(5, "WK_KEY");
		allColumns.put(6, "WK_DESC");
		allColumns.put(7, "SCANNEDUNITS");
		allColumns.put(8, "GROSSDOLLARS");
		allColumns.put(9, "NETDOLLARS");
		allColumns.put(10, "RETAILWEIGHT");
		allColumns.put(11, "POUNDORUNITS");
		allColumns.put(12, "EXTLEVEL3ELEMENTCOST");
		allColumns.put(13, "EXTLEVEL4ELEMENTCOST");
	}

	/**
	 * Parse file and create retail price and retail cost file
	 * @throws GeneralException
	 * @throws IOException
	 */
	private void processFile() throws GeneralException, IOException {

		headerPresent = true;
		conn = DBManager.getConnection();
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		
		String writeToDB = PropertyManager.getProperty("AHOLDMOVFILETORETAILPRICEANDCOST.WRITE_SALE_PRICE_TO_DB","FALSE");
		if(writeToDB.equalsIgnoreCase("TRUE")){
			writePriceIntoDB = true;
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			retailPriceDAO.truncateRetailSalePrice(conn);
			retailPriceDAO = null;
		}
		
		fillAllColumns();
		String fieldNames[] = new String[allColumns.size()];
		int i = 0;
		for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
			fieldNames[i] = columns.getValue();
			i++;
		}

		logger.info("Processing Retail Price Started... ");
		processType = "RETAILPRICE";
		parseFile(fieldNames);
		pw.flush();
		pw.close();
		fw.close();
		logger.info("Total Number of Items " + totalItemCount);
		totalItemCount = 0;		
		logger.info("Number of Ignored Items " + ignoredItemCount);
		ignoredItemCount = 0;
		logger.info("Processing Retail Price Completed... ");

		PristineDBUtil.commitTransaction(conn, "Retail Sale Price Commit");
		
		logger.info("Processing Retail Cost Started... ");
		processType = "RETAILCOST";
		fw = null;
		pw=null;
		parseFile(fieldNames);
		pw.flush();
		pw.close();
		fw.close();
		logger.info("Processing Retail Cost Completed... ");
		logger.info("Total Number of Items " + totalItemCount);
		logger.info("Number of Ignored Items " + ignoredItemCount);
		
		PristineDBUtil.close(conn);
	}

	private void parseFile(String fieldNames[]) throws GeneralException,
			IOException {

		try {
			String outputPath = "";
			ArrayList<String> fileList = getFiles(inputRelativePath);

			if (processType.equals("RETAILPRICE")) {
				//Changed on 16th Oct 2012, to read multiple movement file
				//and put in single retail price file
				//Open file in append mode, if already exists. 
				outputPath = rootPath + "/" + retailPriceOutputPath
						+ "/RetailPrice.csv";
				File file = new File(outputPath);
				if (!file.exists())
					fw = new FileWriter(outputPath);
				else
					fw = new FileWriter(outputPath, true);

				pw = new PrintWriter(fw);
				 
			}
			if (processType.equals("RETAILCOST")) {
				//Changed on 16th Oct 2012, to read multiple movement file
				//and put in single retail cost file
				//Open file in append mode, if already exists. 
				outputPath = rootPath + "/" + retailCostOutputPath
						+ "/RetailCost.csv";
				File file = new File(outputPath);
				if (!file.exists())
					fw = new FileWriter(outputPath);
				else
					fw = new FileWriter(outputPath, true);				
				 
				pw = new PrintWriter(fw);
			}			

			for (int j = 0; j < fileList.size(); j++)
				parseDelimitedFile(AholdMovementFile.class, fileList.get(j),
						'|', fieldNames, stopCount);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			pw.flush();
			pw.close();
			fw.close();

		} finally {
			 
		}
	}

	public void processRecords(List listObject) throws GeneralException {
		try {
			if (processType.equals("RETAILPRICE"))
				createRetailPrice(listObject);
			if (processType.equals("RETAILCOST"))
				createRetailCost(listObject);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Create Retail Price File
	 * @param listObject
	 * @throws IOException
	 */
	private void createRetailPrice(List listObject) throws IOException, GeneralException {	
		// Changes to insert sale price data into RETAIL_SALE_PRICE_TEMP
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		List<RetailPriceDTO> retailPriceDTOList = new ArrayList<RetailPriceDTO>();
		// Changes to insert sale price data into RETAIL_SALE_PRICE_TEMP - Ends
		
		try {		 
			List<AholdMovementFile> aholdMovFile = (List<AholdMovementFile>) listObject;
			
			RetailPriceDTO retailPriceDTO = null;// Changes to insert sale price data into RETAIL_SALE_PRICE_TEMP
			
			for (int j = 0; j < aholdMovFile.size(); j++) {
				AholdMovementFile aholdMovementFile = aholdMovFile.get(j);
				
				if(specificWeek)
				{
					//Take only the specified week
					if (!aholdMovementFile.getWK_DESC().equals(weekDescription)) {	
					continue;
					}
					 
				}
			 

				// upc|item code|Level|Zone#/Store#|Regular retail effective date|Regular retail|Regular retail qty|Sale retail Start date
				// |Sale retail End date|Sale retail|Sale retail qty
				// Retail price structure
				try {					 					
						totalItemCount++;
						aholdMovementFile.initializeVariables();
						
						//Changed on 28th Nov from function isIgnorableItem() to isIgnoreItemForRetailPrice()
						//Now the items are ignored based on type of file (cost/price)
						if(!aholdMovementFile.isIgnoreItemForRetailPrice())
						{
							pw.print(aholdMovementFile.getUPC_CD()); // UPC
							pw.print("|");
							pw.print(""); // item code
							pw.print("|");
							pw.print("2"); // Level
							pw.print("|");
							pw.print(aholdMovementFile.getOPCO_LOC_CD());        //Zone#/Store#
							pw.print("|");
							pw.print(aholdMovementFile.regRetailEffDate());      //Regular retail effective date
							pw.print("|");
							pw.print(aholdMovementFile.regularRetail());         //Regular retail
							pw.print("|");
							pw.print(aholdMovementFile.regRetailQty());          //Regular retail qty
							pw.print("|");
							pw.print(aholdMovementFile.saleRetailStartDate());   //Sale retail Start date
							pw.print("|");
							pw.print(aholdMovementFile.saleRetailEndDate());     //Sale retail End date
							pw.print("|");
							pw.print(aholdMovementFile.saleRetail());            //Sale retail
							pw.print("|");
							pw.println(aholdMovementFile.saleRetailQty());       //Sale retail qty
							
							if(writePriceIntoDB)
								if(aholdMovementFile.isItemOnSale()){
									retailPriceDTO = new RetailPriceDTO();
									retailPriceDTO.setUpc(aholdMovementFile.getUPC_CD());
									retailPriceDTO.setLevelId(aholdMovementFile.getOPCO_LOC_CD());
									float salePrice = Float.valueOf(aholdMovementFile.saleRetail());
									if(salePrice < -999.99 || salePrice > 999.99){
										continue;
									}
									retailPriceDTO.setSalePrice(Float.valueOf(aholdMovementFile.saleRetail()));
									retailPriceDTO.setSaleQty(Integer.valueOf(aholdMovementFile.saleRetailQty()));
									retailPriceDTO.setSaleStartDate(aholdMovementFile.saleRetailStartDate());
									retailPriceDTO.setSaleEndDate(aholdMovementFile.saleRetailEndDate());
									retailPriceDTOList.add(retailPriceDTO);
								}
						}
						else
						{
							ignoredItemCount++;
							logger.debug("Item ignored");
							logger.debug(aholdMovementFile.getUPC_CD() + "|" + aholdMovementFile.getOPCO_LOC_CD() + "|" 
							+ aholdMovementFile.getWK_KEY() + "|" + aholdMovementFile.getWK_DESC() 
							+ "|" + aholdMovementFile.getSCANNEDUNITS() + "|" + aholdMovementFile.getGROSSDOLLARS()
							+ "|" + aholdMovementFile.getNETDOLLARS() + "|" + aholdMovementFile.getRETAILWEIGHT()
							+ "|" + aholdMovementFile.getPOUNDORUNITS() + "|" + aholdMovementFile.getEXTLEVEL3ELEMENTCOST()
							+ "|" + aholdMovementFile.getEXTLEVEL4ELEMENTCOST());
						}

				} catch (ParseException e) {
					logger.error("Skipped: Error while processing retail price for item. UPC: " + aholdMovementFile.getUPC_CD() + "Store" + aholdMovementFile.getOPCO_LOC_CD());
				}
			}
			
			retailPriceDAO.insertRetailSalePriceData(conn, retailPriceDTOList);
		} finally {
			retailPriceDTOList = null;
			retailPriceDAO = null;
		}
	}

	/**
	 * Create Retail Cost file
	 * @param listObject
	 * @throws IOException
	 */	
	private void createRetailCost(List listObject) throws IOException {	
		try {			
			List<AholdMovementFile> aholdMovFile = (List<AholdMovementFile>) listObject;
			for (int j = 0; j < aholdMovFile.size(); j++) {
				AholdMovementFile aholdMovementFile = aholdMovFile.get(j);
				
				if(specificWeek)
				{
					//Take only the specified week
					if (!aholdMovementFile.getWK_DESC().equals(weekDescription)) {	
					continue;
					}
					 
				}

				// upc|item code|Level|Zone#/Store#|list cost effective date|list cost|deal cost start date|deal cost End date|deal Cost
				// Retail cost structure
				try {
						totalItemCount++;
					//if (aholdMovementFile.getWK_DESC().equals("CW:2012-05-19")) {
						aholdMovementFile.initializeVariables();	
						
						//Changed on 28th Nov from function isIgnorableItem() to isIgnoreItemForRetailCost()
						//Now the items are ignored based on type of file (cost/price)
						if(!aholdMovementFile.isIgnoreItemForRetailCost())
						{
							pw.print(aholdMovementFile.getUPC_CD()); // UPC
							pw.print("|");
							pw.print(""); // item code
							pw.print("|");
							pw.print("2"); // Level
							pw.print("|");
							pw.print(aholdMovementFile.getOPCO_LOC_CD());      //Zone#/Store#
							pw.print("|");
							pw.print(aholdMovementFile.listCostEffDate());     //list cost effective date
							pw.print("|");
							pw.print(aholdMovementFile.listCost());            //list cost
							pw.print("|");
							pw.print(aholdMovementFile.dealCostStartDate());   //deal cost start date
							pw.print("|");
							pw.print(aholdMovementFile.dealCostEndDate());     //deal cost End date
							pw.print("|");
							pw.println(aholdMovementFile.dealCost());          //deal Cost
						}
						else
						{
							ignoredItemCount++;
							logger.debug("Item ignored as there is no movement");
							logger.debug(aholdMovementFile.getUPC_CD() + "|" + aholdMovementFile.getOPCO_LOC_CD() + "|" 
							+ aholdMovementFile.getWK_KEY() + "|" + aholdMovementFile.getWK_DESC() 
							+ "|" + aholdMovementFile.getSCANNEDUNITS() + "|" + aholdMovementFile.getGROSSDOLLARS()
							+ "|" + aholdMovementFile.getNETDOLLARS() + "|" + aholdMovementFile.getRETAILWEIGHT()
							+ "|" + aholdMovementFile.getPOUNDORUNITS() + "|" + aholdMovementFile.getEXTLEVEL3ELEMENTCOST()
							+ "|" + aholdMovementFile.getEXTLEVEL4ELEMENTCOST());
						}
					//}
				} catch (ParseException e) {
					logger.error("Skipped: Error while processing retail cost for item. UPC: " + aholdMovementFile.getUPC_CD() + "Store" + aholdMovementFile.getOPCO_LOC_CD());
				}
			}

		} finally {			
		}
		}
}
