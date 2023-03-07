package com.pristine.service;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.PriceHistoryDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.PriceChangeHistoryDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.pricingalert.ExportToExcel;
import com.pristine.util.Constants;
import com.pristine.util.NumberUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;


//Parameters are
//CATEGORY_ID
//ZONE_ID
//COMP_STR_NO
//START_DATE
//END_DATE
//IMS_TABLE (optional)
//RELATIVE_PATH (optional)
public class PriceChangeHistory {


	private static Logger logger = Logger.getLogger("PriceChangeHistory");
	private Connection conn = null;
	
	//Input variables
	private int zoneId;
	private int categoryId;
	private String compStrNo;
	private String startDate;
	private String endDate;
	private String IMS_TABLE ="SYNONYM_IMS_WEEKLY_ZONE";
	private double ANNUAL_REV = 10000;

	private int NO_OF_DAYS_BACK = 180;
	private String RELATIVE_PATH = "PriceChangeReport";
	//Report variables
	private String compStrName;
	private int compStrId;
	private String zoneNo;
	private String categoryName;
	//Items in Category
	private List<ItemDTO> itemsInCategory;
	private List<ProductMetricsDataDTO> imsHistoryList;
	
	private HashMap<String, Double> compPriceMap;
	private HashMap<Integer, ItemDTO> itemMap;
	private HashMap<Integer, List<ProductMetricsDataDTO> > imsHistoryMap;
	private List<PriceChangeHistoryDTO> pcHistoryList = new ArrayList<PriceChangeHistoryDTO>();  
	
	public static void main(String[] args) {
		PriceChangeHistory pch = new PriceChangeHistory();
		pch.generateHistory(args);
	}

	public PriceChangeHistory() {
		PropertyConfigurator.configure("log4j.properties");
		PropertyManager.initialize("analysis.properties");
		 
		
	}
	private  void generateHistory(String[] args) {
		
		try {
			logger.info("Price History Started ...");
			conn = DBManager.getConnection();
			//Collect the parameters
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];

				if (arg.startsWith("ZONE_ID")) {
					zoneId = Integer.parseInt(arg.substring("ZONE_ID=".length()));
				}

				if (arg.startsWith("CATEGORY_ID")) {
					categoryId = Integer.parseInt(arg.substring("CATEGORY_ID=".length()));
				}
				
				if (arg.startsWith("COMP_STR_NO")) {
					compStrNo = arg.substring("COMP_STR_NO=".length());
				}

				if (arg.startsWith("START_DATE")) {
					startDate = arg.substring("START_DATE=".length());
				}

				if (arg.startsWith("END_DATE")) {
					endDate = arg.substring("END_DATE=".length());
				}
				
				if (arg.startsWith("IMS_TABLE")) {
					IMS_TABLE = arg.substring("IMS_TABLE=".length());
				}
				
				if (arg.startsWith("RELATIVE_PATH")) {
					RELATIVE_PATH = arg.substring("RELATIVE_PATH=".length());
				}
			}

			if( startDate == null || endDate == null ) {
				logger.error("Both START_DATE and END_DATE are mandotory");
			}else if (categoryId==0){
				logger.error("CATEGORY_ID is mandotory");
			}else if (zoneId==0){
				logger.error("ZONE_ID is mandotory");
			}else{
				//do the processing
				getBasicData();
				organizeData();
				
				for(int itemCode : imsHistoryMap.keySet()){
					prepareData(itemCode);
				}
				logger.info("# of item to Report :" + pcHistoryList.size());
				createExcelReport(RELATIVE_PATH);
			}
			
			logger.info("Price History End ...");
		}catch(GeneralException | Exception ex ){
			logger.error("Error in Price History Collection Transaction is rollbacked - ", ex);
		}finally{
			PristineDBUtil.close(conn);
		}
	}

	private void prepareData(int itemCode) {
		PriceChangeHistoryDTO pchdto = new PriceChangeHistoryDTO();
		ItemDTO item = itemMap.get(itemCode);
		pchdto.setItemCode(item.itemCode);
		pchdto.setRetailerItemCode(item.retailerItemCode);	
		pchdto.setCategoryName(categoryName);
		pchdto.setItemName(item.itemName);
		pchdto.setLirName(item.likeItemGrp);
		List<ProductMetricsDataDTO> imsItemHistory = imsHistoryMap.get(itemCode);
		pchdto.setNumofWeeksAnalyzed(imsItemHistory.size());
		ProductMetricsDataDTO currentIMSRec = imsItemHistory.get(imsItemHistory.size()-1);
		pchdto.setRegPrice(currentIMSRec.getRegularPrice());
		pchdto.setListCost(currentIMSRec.getListPrice());
		if( pchdto.getRegPrice() > 0 && pchdto.getListCost() >0 ){
			double marginPct = (pchdto.getRegPrice() - pchdto.getListCost())*100/pchdto.getRegPrice();
			pchdto.setMarginPct(NumberUtil.RoundDouble(marginPct, 2));
		}
		
		ProductMetricsDataDTO imsPrevious = null;
		int noOfPriceChanges = 0;
		String mostCurrentPrChgDate = "";
		int noOfCostChanges = 0;
		String mostCurrentCostChgDate = "";
		int noOfWeeksOnSale = 0;
		double dealCostWhenLastSale = 0;
		double mostRecentSalePrice = 0;
		String mostRecentSaleOn = "";
		for( ProductMetricsDataDTO imsCurrent : imsItemHistory ){
			if (imsPrevious == null){
				imsPrevious = imsCurrent;
				continue;
			}
			
			if( (Math.abs((imsCurrent.getRegularPrice() - imsPrevious.getRegularPrice())) >0.03)
					&& (imsCurrent.getRegularPrice() > 0 && imsPrevious.getRegularPrice() > 0)){
				noOfPriceChanges++;
				mostCurrentPrChgDate = imsCurrent.getStartDate(); 
			}
			
			//Compare Cost
			if( (Math.abs((imsCurrent.getListPrice() - imsPrevious.getListPrice())) >0.03)
					&& (imsCurrent.getListPrice() > 0 && imsPrevious.getListPrice() > 0)){
				noOfCostChanges++;
				mostCurrentCostChgDate = imsCurrent.getStartDate(); 
			}

			if((imsCurrent.getRegularPrice() - imsCurrent.getSalePrice()) > 0.1 && imsCurrent.isSaleFlag().equals("Y") ){
				noOfWeeksOnSale++;
				mostRecentSalePrice = imsCurrent.getSalePrice();
				dealCostWhenLastSale = imsCurrent.getDealPrice();
				mostRecentSaleOn = imsCurrent.getStartDate();
			}
			imsPrevious = imsCurrent;
		}

		pchdto.setNumofPriceChanges(noOfPriceChanges);
		pchdto.setRecentPriceChangeDate(mostCurrentPrChgDate);
		pchdto.setNumofCostChanges(noOfCostChanges);
		pchdto.setRecenCostChangeDate(mostCurrentCostChgDate);
		pchdto.setNumofWeeksOnSale(noOfWeeksOnSale);
		pchdto.setRecentSalePrice(mostRecentSalePrice);
		pchdto.setDealCostWhenLastSale(dealCostWhenLastSale);
		pchdto.setRecentSalePriceOn(mostRecentSaleOn);
		
		
		//52 week Revenue
		double annualRevenue=0;
		for( int i = imsItemHistory.size() - 1, count=0; i>=0 && count<52; i--, count++){
			annualRevenue+=imsItemHistory.get(i).getTotalRevenue(); 
		}
		pchdto.setAnnualRevenue(annualRevenue);
	
		
		//Comp Price
		String compPriceKey;
		if( item.getLikeItemId() > 0) 
			compPriceKey = "LIR" + item.getLikeItemId();
		else 
			compPriceKey = "ITM" + item.getItemCode();
		
		if( compPriceMap.containsKey(compPriceKey))
			pchdto.setCurrentCompPrice(compPriceMap.get(compPriceKey));
		
		
		if(pchdto.getAnnualRevenue() > ANNUAL_REV){
			pcHistoryList.add(pchdto);
		}
	}

	private void organizeData() {
		// TODO Auto-generated method stub
		itemMap = new HashMap<Integer, ItemDTO> ();
		for( ItemDTO item : itemsInCategory){
			itemMap.put(item.getItemCode(), item);
		}

		imsHistoryMap = new  HashMap<Integer, List<ProductMetricsDataDTO>> ();
		for( ProductMetricsDataDTO imsRecord : imsHistoryList){
			int itemCode = imsRecord.getProductId();
			List<ProductMetricsDataDTO> itemHistoryList;
			if( imsHistoryMap.containsKey(itemCode))
				itemHistoryList = imsHistoryMap.get(itemCode);
			else{
				itemHistoryList = new ArrayList<ProductMetricsDataDTO>();
				imsHistoryMap.put(itemCode, itemHistoryList);
			}
			itemHistoryList.add(imsRecord);
		}
	}

	private void getBasicData() throws GeneralException {
		PriceHistoryDAO phdao = new PriceHistoryDAO ();
		// Get the Comp Store Name
		if( compStrNo != null){
			compStrName = phdao.getCompStrName(conn, compStrNo);
			logger.debug("Comp Str No " + compStrName);
			
			
			compStrId = phdao.getCompStrId(conn, compStrNo);
			logger.debug("Comp Str Id " + compStrId);
		}
		
		//Get the Zone Num
		zoneNo = phdao.getZoneNum(conn, zoneId);
		logger.debug("Zone Num is " + zoneNo);		
		//Get the items in the category
		ItemDAO itemdao = new ItemDAO();
		itemsInCategory = itemdao.getItemDetailsInACategory(conn, Constants.CATEGORYLEVELID, categoryId);
		logger.info("# of Items in category " + itemsInCategory.size());
		
		//getCategoryName
		categoryName = phdao.getProductGroupName(conn, Constants.CATEGORYLEVELID, categoryId);
		logger.debug("Category Name is " + categoryName);
		
		//Get Data from IMS table
		 imsHistoryList = phdao.getIMSHistory(conn,Constants.CATEGORYLEVELID, 
				 				categoryId, Constants.ZONE_LEVEL_ID, zoneId, startDate, endDate, IMS_TABLE);
		 logger.info("# of ims history records " + imsHistoryList.size());
			
		 //Get Comp Info
		 compPriceMap = phdao.getCompetitorRetails(conn, compStrId, Constants.CATEGORYLEVELID, categoryId, NO_OF_DAYS_BACK);
		 logger.info("# of comp records " + compPriceMap.size());
	}
	
	public void createExcelReport(String relativePath) throws GeneralException {
	
		String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		
		String baseFolder = rootPath + "/" + relativePath;
		//Get the Template file
		String pgTempFile = baseFolder +"/Template/PriceChangeReportTemplate.xls";
		String pgDestFile = baseFolder +"/PCHReport_" + categoryName + "_Zone_" + zoneNo + ".xls";
    
		ExportToExcel excelExport = new ExportToExcel();
		boolean copyStatus = excelExport.copyExcel(pgTempFile, pgDestFile);
		if( !copyStatus){
			logger.error("Unable to create Excel file");
			return;
		}
		
		try{
			InputStream inp = new FileInputStream(pgDestFile);    
			    
			HSSFWorkbook wb = new HSSFWorkbook(inp);    
			HSSFSheet sheet = wb.getSheetAt(0);
			HSSFRow headerRow = sheet.getRow(0);
			
			setCellValue(headerRow,1, startDate + " - " + endDate);
			setCellValue(headerRow,4, zoneNo);
			setCellValue(headerRow,7, compStrName);
			
			//loop here to print the items
			int rowToStart = 2;
			for(PriceChangeHistoryDTO priceChangeHistory: pcHistoryList){
				int colCount = -1;
				HSSFRow itemRow = sheet.getRow(rowToStart);
				if(itemRow == null){
					itemRow = sheet.createRow(rowToStart);
				}
				rowToStart++;
				setCellValue(itemRow,++colCount, priceChangeHistory.getRetailerItemCode());
				setCellValue(itemRow,++colCount, priceChangeHistory.getLirName());
				setCellValue(itemRow,++colCount, priceChangeHistory.getItemName());
				setCellValue(itemRow,++colCount, priceChangeHistory.getRegPrice());
				setCellValue(itemRow,++colCount, priceChangeHistory.getListCost());
				setCellValue(itemRow,++colCount, priceChangeHistory.getMarginPct());
				setCellValue(itemRow,++colCount, priceChangeHistory.getNumofPriceChanges());
				setCellValue(itemRow,++colCount, String.valueOf(priceChangeHistory.getRecentPriceChangeDate()));
				setCellValue(itemRow,++colCount, priceChangeHistory.getNumofCostChanges());
				setCellValue(itemRow,++colCount, String.valueOf(priceChangeHistory.getRecenCostChangeDate()));
				setCellValue(itemRow,++colCount, priceChangeHistory.getCurrentCompPrice());
				setCellValue(itemRow,++colCount, priceChangeHistory.getAnnualRevenue());
				setCellValue(itemRow,++colCount, priceChangeHistory.getRecentSalePrice());
				setCellValue(itemRow,++colCount, priceChangeHistory.getRecentSalePriceOn());
				setCellValue(itemRow,++colCount, priceChangeHistory.getDealCostWhenLastSale());
				setCellValue(itemRow,++colCount, priceChangeHistory.getNumofWeeksOnSale());
				setCellValue(itemRow,++colCount, priceChangeHistory.getNumofWeeksAnalyzed());
				setCellValue(itemRow,++colCount, priceChangeHistory.getCategoryName());
			}
			FileOutputStream fileOut = new FileOutputStream(pgDestFile);  
		    wb.write(fileOut);  
		    wb.close();
		    fileOut.close(); 

		}catch(Exception fe){
			logger.error("File Processing Exception : ", fe);
			throw new GeneralException( fe.getMessage(), fe);
		}

	}
	
	private void setCellValue(HSSFRow row, int colCount, String value) {
		HSSFCell cell;
		cell = row.getCell(colCount);
		if (cell == null)    
		    cell = row.createCell(colCount);
		cell.setCellValue(value);		
	}
	
	private void setCellValue(HSSFRow row, int colCount, double value) {
		HSSFCell cell;
		cell = row.getCell(colCount);
		if (cell == null)    
		    cell = row.createCell(colCount);
		cell.setCellValue(value);	
		
	}
}
