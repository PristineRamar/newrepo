/*
 * Title: Store Price Export for Giant Eagle
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	01/12/2018	John Britto		Initial Version 
 * Version 0.2	01/23/2018	John Britto		NPL & PL Flags logic modified 
 **********************************************************************************
 */

package com.pristine.dataload.offermgmt;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.NotificationDAO;
import com.pristine.dao.offermgmt.StoreFileExportGEDAO;
import com.pristine.dto.offermgmt.LPRuleDTO;
import com.pristine.dto.offermgmt.NLPRuleDTO;
import com.pristine.dto.offermgmt.NotificationDetailDTO;
import com.pristine.dto.offermgmt.NotificationHeaderDTO;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.StoreFileExportDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.BusinessUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class StoreFileExportGE {
	private StoreFileExportGEDAO storeFileExportDAO = null;
	private static Logger logger = Logger.getLogger("StoreFileExport");
	private Connection conn = null;
	private String rootPath;
	private String rootPathBackup;
	private String relativeOutputPath;
	
	public StoreFileExportGE(){
		PropertyConfigurator.configure("log4j-store-export.properties");
		PropertyManager.initialize("recommendation.properties");
		try {
			conn = DBManager.getConnection();
			storeFileExportDAO = new StoreFileExportGEDAO(conn);
			
			 rootPath = PropertyManager.getProperty("DATAUPLOAD.ROOTPATH", "");
			 rootPathBackup = PropertyManager.getProperty("DATABACKUP.ROOTPATH", "");
			 relativeOutputPath = PropertyManager.getProperty("STORE_PRICE_FILE_PATH", "");
			 
		} catch (GeneralException exe) {
			logger.error("Error while connecting to DB:" + exe);
			System.exit(1);
		}
	}
	
  public static void main(String[] args) throws GeneralException {
	StoreFileExportGE storeFileExport = new StoreFileExportGE();

	storeFileExport.processStoreExportData();
  }
	
  private void processStoreExportData(){
	  /* Steps
	   * 1: Get Recommendation run Headers
	   * 2: Get Export data based on list of Run IDs
	   * 3: Calculate the NLP & LP flags
	   * 4: Write the data into file
	   * 5: Update recommendation header with export status
	   * 6: Add Notification entries 
	   * */
	  
	 try
		{
		 //List<StoreFileExportDTO> storeFileExportMap = new ArrayList<StoreFileExportDTO>();
		 ArrayList<StoreFileExportDTO> storeFileExportMap = new ArrayList<StoreFileExportDTO>();
		 
		 // Get the recommendation header
		 List<PRRecommendationRunHeader> recHeaderList = 
	 		storeFileExportDAO.getApprovedRecommendationHeader(conn);
		 
		 //If any approved recommendation exist, 
		 if (recHeaderList != null && recHeaderList.size() > 0){
			 logger.debug("Total approved recommendations: " + recHeaderList.size());
			 
			 // Get export for an store
			 for (int i=0; i < recHeaderList.size(); i++){
				 logger.debug("Get recommended items for Run Id " + recHeaderList.get(i).getRunId());
				 storeFileExportDAO.getRecommendationDataForStoreExport(
						 recHeaderList.get(i).getRunId(), storeFileExportMap);
			 }
		 }
		 else
			 logger.info("There is no approved recommendation");
		 
		 if (storeFileExportMap != null && storeFileExportMap.size() > 0){
			 logger.debug("caclulate Low Price Flag...");
			 caclulateLowPriceFlag(storeFileExportMap);		 
		 }
		 
		 //Write the data into file
		 logger.debug("Total prices to export: " + storeFileExportMap.size());
		 logger.debug("Write data into file");
		 writeStoreData(storeFileExportMap);
		 
		 if (recHeaderList != null && recHeaderList.size() > 0){
			 logger.debug("Update status in DB...");
			 UpdateStatus(recHeaderList);
		 }
		 
		}
		catch(GeneralException ge){
			logger.error("Error in getStoreExportData " + ge.getMessage());
		}catch(Exception e){
			logger.error("Exception in getStoreExportData " + e.getMessage());
		}finally{
			PristineDBUtil.close(conn);
		}
	}
	
	private void writeStoreData(List<StoreFileExportDTO> storePriceList) throws GeneralException {
		PrintWriter pw = null;
		FileWriter fw = null;
		try {
			String FileName = PropertyManager.getProperty("STORE_PRICE_FILE_NAME", "");
			
			String fileName = FileName + ".txt";
			String outputPath = rootPath + "/" + relativeOutputPath + "/" + fileName;
			logger.debug("Output file path: " + outputPath);
			File file = new File(outputPath);
			
			// If file not exist, create new. If exist, append the same
			if (!file.exists())
				fw = new FileWriter(outputPath);
			else
				fw = new FileWriter(outputPath, true);
			
			pw = new PrintWriter(fw, true);
			try {
				List<StoreFileExportDTO> StoreFileExportDTOList = (List<StoreFileExportDTO>) storePriceList;
				for (int j = 0; j < StoreFileExportDTOList.size(); j++) {
					StoreFileExportDTO StoreFileExportDTO = StoreFileExportDTOList.get(j);
					pw.print(StoreFileExportDTO.getRetItemCode()); //RETAILER ITEM CODE
					pw.print("|");
					pw.print("ZONE"); // PRC_LVL_ID								
					pw.print("|");
					pw.print(StoreFileExportDTO.getBannerCode()); //BANNER CODE #
					pw.print("|");
					pw.print(StoreFileExportDTO.getZoneNo()); //ZONE NUMBER
					pw.print("|");
					pw.print(""); //STORE NUMBER
					pw.print("|");
					pw.print(StoreFileExportDTO.getVendorNo());// VENDOR NUMBER
					pw.print("|");
					pw.print(StoreFileExportDTO.getQuantity());// MUNIT COUNT
					pw.print("|");
					pw.print(StoreFileExportDTO.getPrice());// MUNIT COUNT
					pw.print("|");
					pw.print(StoreFileExportDTO.getRegEffectiveDate());	//PRICE EFFECTIVE DATE
					pw.print("|");
					pw.print(StoreFileExportDTO.getNLPFlag());// NEW LOW PRICE FLAG
					pw.print("|");
					pw.print(StoreFileExportDTO.getNLPEndDate());// NEW LOW PRICE FLAG END DATE
					pw.print("|");
					pw.print(StoreFileExportDTO.getLPFlag());// LOW PRICE FLAG
					pw.print("|");
					pw.print(StoreFileExportDTO.getLPEndDate());// LOW PRICE FLAG END DATE
					pw.print("|");
					pw.print(StoreFileExportDTO.getRunId());// RUN ID
					pw.print("|");
					pw.println("");
				} 
				pw.close();
				
				PrestoUtil.zipFile(fileName, rootPath + "/" + relativeOutputPath, FileName + ".zip");
				PrestoUtil.moveFile (outputPath, rootPathBackup + "/" + relativeOutputPath);
			}catch (Exception ex) {
			        logger.error("Outer Exception - JavaException", ex);
			        ex.printStackTrace();
			        throw new GeneralException("Outer Exception - JavaException - " + ex.toString());
			     }
			} catch (Exception ex) {
			    logger.error("JavaException", ex);
			    throw new GeneralException("JavaException - " + ex.toString());
	    }
	}

	private void UpdateStatus(List<PRRecommendationRunHeader> recHeaderList) throws GeneralException{
		NotificationDAO notificationDao = new NotificationDAO();
		NotificationHeaderDTO notificationHeaderObj = new NotificationHeaderDTO();
		List<NotificationDetailDTO> notificationDetailListObj = new ArrayList<NotificationDetailDTO>();
		NotificationDetailDTO notificationDetailObj = new NotificationDetailDTO();

		try{

			// Update the recommendation header status
			logger.debug("Update expored status to recommendation header");
			storeFileExportDAO.updateRecommendationStatus(conn, recHeaderList);
			
			// Update notification header
			for (int i=0; i < recHeaderList.size(); i++){
				logger.debug("Get user list for the processing category & zone");
				ArrayList<String> userList = storeFileExportDAO.getUserIDList(recHeaderList.get(i));
				
				if (userList != null && userList.size() > 0){
					notificationHeaderObj = new NotificationHeaderDTO();
					notificationHeaderObj.setModuleId(PRConstants.REGULAR_PRICE_RECOMMENDATION);
					notificationHeaderObj.setNotificationTypeId(PRConstants.STATUS_EXPORTED);
					notificationHeaderObj.setNotificationKey1(recHeaderList.get(i).getRunId());

					logger.debug("Add notification  header");
					long notificationId = notificationDao.insertNotificationHeaderV2(conn, notificationHeaderObj);
					
					for (int j=0; j < userList.size(); j++){
						notificationDetailObj.setNotificationId(notificationId);
						notificationDetailObj.setUserId(userList.get(j));
						notificationDetailListObj.add(notificationDetailObj);
					}
				}
				else{
					logger.debug("There is no user attached with Run Id:" + recHeaderList.get(i).getRunId());
					logger.debug("No need to add notification data");
				}
			}
			
			// Update notification details
			if (notificationDetailListObj != null && notificationDetailListObj.size() > 0){
				logger.debug("Add notification  detail");
				notificationDao.insertNotificationDetails(conn, notificationDetailListObj);
			}
			
			PristineDBUtil.commitTransaction(conn, "Commit Changes");
		}
		catch (Exception ex) {
	        logger.error("Outer Exception - JavaException", ex);
	        PristineDBUtil.rollbackTransaction(conn, "Rollback Changes");
	        throw new GeneralException("Outer Exception - JavaException - " + ex.toString());
	     }
	}
	
	private void caclulateLowPriceFlag(List<StoreFileExportDTO> storePriceList) throws ParseException{
		
		BusinessUtil businessUtilObj = new BusinessUtil();  
		List<NLPRuleDTO> NLPList = populateNLPCollection();
		List<LPRuleDTO> LPList = populateLPCollection();

		for (int i=0; i < storePriceList.size(); i++){
			String effectiveDate = storePriceList.get(i).getRegEffectiveDate();
			Date nplEndDate = new SimpleDateFormat("yyyy-MM-dd").parse(effectiveDate);  
			Calendar cal = Calendar.getInstance();
			cal.setTime(nplEndDate);
			cal.set(Calendar.DAY_OF_WEEK, Calendar.WEDNESDAY);
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			
			if (isNewLowPrice(NLPList, storePriceList.get(i), businessUtilObj)){
				// Calculate the flag end date
				cal.add(Calendar.DATE, PRConstants.STORE_EXPORT_FLAG_NLP_END_WEEK_COUNT * PRConstants.DAYS_IN_WEEK);
				String flagEndDate = dateFormat.format(cal.getTime());

				// Set Flag
				storePriceList.get(i).setNLPFlag("Y");

				// Set FlagEffective end date
				storePriceList.get(i).setNLPEndDate(flagEndDate);
			}
			else{
				storePriceList.get(i).setNLPFlag("N");
				storePriceList.get(i).setNLPEndDate("");
			}
			
			
			if (storePriceList.get(i).getCompPrice() > 0 ){
				if (isLowPrice(LPList, storePriceList.get(i), businessUtilObj)){
					// Calculate the flag end date
					cal.add(Calendar.DATE, PRConstants.STORE_EXPORT_FLAG_LP_END_WEEK_COUNT * PRConstants.DAYS_IN_WEEK);
					String flagEndDate = dateFormat.format(cal.getTime());
	
					storePriceList.get(i).setLPFlag("Y");
					storePriceList.get(i).setLPEndDate(flagEndDate);
				}
				else{
					storePriceList.get(i).setLPFlag("N");
					storePriceList.get(i).setLPEndDate("");
				}
			}
			else{
				storePriceList.get(i).setLPFlag("N");
				storePriceList.get(i).setLPEndDate("");
			}
			
		}
	}
	
	private boolean isNewLowPrice(List<NLPRuleDTO> NLPList, StoreFileExportDTO storePriceObj, BusinessUtil businessUtilObj){
		boolean isNewLowPrice = false;
		double diff = 0;
		
		NLPRuleDTO NLPObj = new NLPRuleDTO(); 
		
		for (int i=0; i < NLPList.size(); i++){
			NLPObj = new NLPRuleDTO();  
			NLPObj = NLPList.get(i);
		
			double recUnitPrice = businessUtilObj.getUnitPrice(
				storePriceObj.getPrice(), storePriceObj.getQuantity());
			
			double curUnitPrice = businessUtilObj.getUnitPrice(
				storePriceObj.getCurrentPrice(), storePriceObj.getCurrentQuantity());
			
			if (recUnitPrice > 0 && curUnitPrice > 0)
				diff = PRFormatHelper.roundToTwoDecimalDigitAsDouble(curUnitPrice - recUnitPrice);
			
			if (NLPObj.getMinRange() == 0 && NLPObj.getMaxRange() > 0 && curUnitPrice >= NLPObj.getMinRange() && curUnitPrice <= NLPObj.getMaxRange() &&	diff >= NLPObj.getPriceDiff())
				isNewLowPrice = true;			
			else if (NLPObj.getMinRange() > 0 && NLPObj.getMaxRange() > 0 && curUnitPrice >= NLPObj.getMinRange() && curUnitPrice <= NLPObj.getMaxRange() && diff >= NLPObj.getPriceDiff())
				isNewLowPrice = true;
			else if (NLPObj.getMinRange() > 0 && NLPObj.getMaxRange() == 0 && curUnitPrice >= NLPObj.getMinRange() && diff >= NLPObj.getPriceDiff())
				isNewLowPrice = true;
		}
		
		logger.debug("NLP | " + storePriceObj.getRetItemCode() + " | " +
		storePriceObj.getPrice() + " | " + storePriceObj.getCurrentPrice() + " | " +
		diff + " | " + isNewLowPrice);
		
		return isNewLowPrice;
	}
	
	private boolean isLowPrice(List<LPRuleDTO> LPList, 
		StoreFileExportDTO storePriceObj, BusinessUtil businessUtilObj){
		
		boolean isLowPrice = false;

		double recUnitPrice = businessUtilObj.getUnitPrice(
				storePriceObj.getPrice(), storePriceObj.getQuantity());
			
		double priceIndex = businessUtilObj.getSimplePriceIndex(
			storePriceObj.getPrice(), storePriceObj.getQuantity(), 
			storePriceObj.getCompPrice(), storePriceObj.getCompQuantity());
		
		for (int i=0; i < LPList.size(); i++){
			if ((recUnitPrice >= LPList.get(i).getMinRange() && 
					recUnitPrice <= LPList.get(i).getMaxRange()) && 
					Math.round(priceIndex) <= LPList.get(i).getPriceIndex())
				isLowPrice = true;
		}
		
		logger.debug("LP | " + storePriceObj.getRetItemCode() + " | " +
		storePriceObj.getPrice() + " | " + storePriceObj.getCompPrice() + " | " +
		priceIndex + " | " + isLowPrice);
		
		return isLowPrice;
	}
	
	private List<NLPRuleDTO> populateNLPCollection(){
		List<NLPRuleDTO> NLPList = new ArrayList<NLPRuleDTO>();
		
		NLPRuleDTO NLPObj;		
		NLPObj = new NLPRuleDTO();		NLPObj.setMinRange(0);		NLPObj.setMaxRange(2.50);		NLPObj.setPriceDiff(0.10);		NLPList.add(NLPObj);	
		NLPObj = new NLPRuleDTO();		NLPObj.setMinRange(2.51);	NLPObj.setMaxRange(5.00);		NLPObj.setPriceDiff(0.20);		NLPList.add(NLPObj);	
		NLPObj = new NLPRuleDTO();		NLPObj.setMinRange(5.01);	NLPObj.setMaxRange(8.00);		NLPObj.setPriceDiff(0.30);		NLPList.add(NLPObj);			
		NLPObj = new NLPRuleDTO();		NLPObj.setMinRange(8.01);	NLPObj.setMaxRange(10.00);		NLPObj.setPriceDiff(0.40);		NLPList.add(NLPObj);	
		NLPObj = new NLPRuleDTO();		NLPObj.setMinRange(10.01);	NLPObj.setMaxRange(20.00);		NLPObj.setPriceDiff(0.50);		NLPList.add(NLPObj);	
		NLPObj = new NLPRuleDTO();		NLPObj.setMinRange(20.01);	NLPObj.setMaxRange(30.00);		NLPObj.setPriceDiff(1);			NLPList.add(NLPObj);	
		NLPObj = new NLPRuleDTO();		NLPObj.setMinRange(30.01);	NLPObj.setMaxRange(40.00);		NLPObj.setPriceDiff(2);			NLPList.add(NLPObj);	
		NLPObj = new NLPRuleDTO();		NLPObj.setMinRange(40.01);	NLPObj.setMaxRange(60.00);		NLPObj.setPriceDiff(3);			NLPList.add(NLPObj);	
		NLPObj = new NLPRuleDTO();		NLPObj.setMinRange(60.01);	NLPObj.setMaxRange(70.00);		NLPObj.setPriceDiff(4);			NLPList.add(NLPObj);	
		NLPObj = new NLPRuleDTO();		NLPObj.setMinRange(70.01);	NLPObj.setMaxRange(130.00);		NLPObj.setPriceDiff(5);			NLPList.add(NLPObj);	
		NLPObj = new NLPRuleDTO();		NLPObj.setMinRange(130.01);	NLPObj.setMaxRange(0);			NLPObj.setPriceDiff(10);		NLPList.add(NLPObj);	
		
		return NLPList;
	}

	private List<LPRuleDTO> populateLPCollection(){
		List<LPRuleDTO> LPList = new ArrayList<LPRuleDTO>();
		LPRuleDTO LPObj;
		
		LPObj = new LPRuleDTO();		LPObj.setMinRange(0);		LPObj.setMaxRange(0.99);		LPObj.setPriceDiff(110);		LPList.add(LPObj);	
		LPObj = new LPRuleDTO();		LPObj.setMinRange(1);		LPObj.setMaxRange(29.99);		LPObj.setPriceDiff(105);		LPList.add(LPObj);	
		return LPList;
	}
	
	
}


