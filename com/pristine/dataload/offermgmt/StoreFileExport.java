package com.pristine.dataload.offermgmt;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.StoreFileExportDAO;
import com.pristine.dto.offermgmt.StoreFileExportDTO;
import com.pristine.dto.offermgmt.UserDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


public class StoreFileExport {
	private StoreFileExportDAO storeFileExportDAO = null;
	private static Logger logger = Logger.getLogger("StoreFileExport");
	private Connection conn = null;
	private static final String ZONE_ID = "ZONE_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	private int locationLevelId = -1;
	private int locationId = -1;
	private int productLevelId = -1;
	private int productId = -1;
	private boolean isPerishable = false;
	private PrintWriter pw = null;
	private String rootPath;
	private static String relativeOutputPath;
	private FileWriter fw = null;
	
	public StoreFileExport(){
		PropertyConfigurator.configure("log4j-pr-store-export.properties");
		PropertyManager.initialize("recommendation.properties");
		try {
			conn = DBManager.getConnection();
			storeFileExportDAO = new StoreFileExportDAO(conn);
		} catch (GeneralException exe) {
			logger.error("Error while connecting to DB:" + exe);
			System.exit(1);
		}
	}
	
  public static void main(String[] args) throws GeneralException {
	    
		StoreFileExport storeFileExport = new StoreFileExport();
		if(args.length > 0){
			for(String arg : args){
				if(arg.startsWith(ZONE_ID)){
					storeFileExport.setLocationLevelId(Constants.ZONE_LEVEL_ID);
					storeFileExport.setLocationId(Integer.parseInt(arg.substring(ZONE_ID.length())));
				}
				else if(arg.startsWith(PRODUCT_LEVEL_ID)){
					storeFileExport.setProductLevelId(Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length())));
				}
				else if(arg.startsWith(PRODUCT_ID)){
					storeFileExport.setProductId(Integer.parseInt(arg.substring(PRODUCT_ID.length())));
				}
				else if (arg.startsWith(OUTPUT_FOLDER)) {
					relativeOutputPath = arg.substring(OUTPUT_FOLDER.length());
				}
			}
		    
			storeFileExport.getStoreExportData(storeFileExport.getLocationId(), storeFileExport.getProductLevelId(), storeFileExport.getproductId());
		  } 
    	}
	
	
  private void getStoreExportData(int locationId, int productLevelId, int productId){
	 try
		{
		 List<StoreFileExportDTO> storeFileExportMap = new ArrayList<StoreFileExportDTO>();
		 
		 int getWeek = Integer.parseInt(PropertyManager.getProperty("NO_OF_WEEKS_TO_BE_EXPORTED", ""));
		 
		 Date date = new Date();
		 Calendar c = Calendar.getInstance();
		 c.setTime(date);
		 int totalDays = getWeek * 7;
		 c.add(Calendar.DATE, -totalDays);
		 Date startDate = c.getTime();
		 c.add(Calendar.DATE, totalDays);
		 Date endDate = c.getTime();
		 DateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy");
		 String stDate = dateFormat.format(startDate);
		 String edDate = dateFormat.format(endDate);
		 logger.info("startDate - " + stDate);
		 logger.info("endDate - " + edDate);
		 storeFileExportMap = storeFileExportDAO.getStoreExportData(locationId,productLevelId,productId, stDate, edDate);
		 rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		 WriteStoreData(storeFileExportMap, locationId, productLevelId, productId,stDate, edDate);
		}
		catch(GeneralException ge){
			logger.error("Error in getStoreExportData " + ge.getMessage());
		}catch(Exception e){
			logger.error("Exception in getStoreExportData " + e.getMessage());
		}finally{
			PristineDBUtil.close(conn);
		}
	}
	
  //The result will be generated as Text File.
	private void WriteStoreData(List<StoreFileExportDTO> listobj, int locationId, int productLevelId, int productId, String stDate, String endDate) throws GeneralException {
		try {
			DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
			Date date = new Date();
			String currentDate = dateFormat.format(date);
			String FileName = PropertyManager.getProperty("FILE_NAME", "");
			
			String fileName = FileName + "_" + currentDate + ".txt";
			String zipFileName = FileName + "_" + currentDate + ".zip";
			String outputPath = rootPath + "/" + relativeOutputPath + "/" + fileName;
			File file = new File(outputPath);
			if (!file.exists())
				fw = new FileWriter(outputPath);
			else
				fw = new FileWriter(outputPath);
			pw = new PrintWriter(fw, true);
			try {
				List<StoreFileExportDTO> StoreFileExportDTOList = (List<StoreFileExportDTO>) listobj;
				for (int j = 0; j < StoreFileExportDTOList.size(); j++) {
					StoreFileExportDTO StoreFileExportDTO = StoreFileExportDTOList.get(j);
					pw.print(StoreFileExportDTO.getCompStrNo()); //COMP STR #
					pw.print("|");
					pw.print(StoreFileExportDTO.getRetItemCode()); //RETAILER ITEM CODE
					pw.print("|");
					pw.print(StoreFileExportDTO.getQuantity()); //QUANTITY
					pw.print("|");
					pw.print(StoreFileExportDTO.getPrice()); //PRICE
					pw.print("|");
					pw.print(StoreFileExportDTO.getRegEffectiveDate());// REGULAR EFFECTIVE DATE
					pw.println("       "); // spaces
				} 
				
				 //Once the File is exported, the STATUS column in the database should be updated as 'Exported'
				String statusUpdate = PropertyManager.getProperty("STATUS_UPDATE", "");
				if(statusUpdate.equals("YES")){
					storeFileExportDAO.updateRecStatusAsExported(locationId, productId, productLevelId, stDate, endDate);
				}
				 
				//The generated text file should be compressed.
				CompressExportedFile(fileName, zipFileName);
				
 				 //The Exported file should be sent to client through SFTP
				//TransferExportedFileThroughSFTP("StoreFileExport.zip");
				
				//Get UserIDs of ProductID
				ArrayList<Integer> productIds = new ArrayList<Integer>();
				ArrayList<Integer> locationIds = new ArrayList<Integer>();
				ArrayList<UserDTO> userDTO = new ArrayList<UserDTO>();
				if(productId != -1 && locationId != -1)
				{
					//The UserID will be taken based on ProductID
					productIds.add(productId);
					locationIds.add(locationId);
					userDTO = storeFileExportDAO.getUserIDbyProductID(productIds, locationIds);
				}
				else
				{
					//The UserID will be taken based on RunID -> ProductID -> UserID
					locationIds.add(locationId);
					productIds = storeFileExportDAO.getProductIDbyRunId();
					if(!productIds.isEmpty())
					{
						userDTO = storeFileExportDAO.getUserIDbyProductID(productIds, locationIds);
					}
				}
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
	
	//The Exported file should be sent to client through SFTP
	/*private void TransferExportedFileThroughSFTP (String fileName) throws GeneralException{
        String SFTPHOST = PropertyManager.getProperty("SFTPHOST", "");
        int SFTPPORT = 22;
        String SFTPUSER = PropertyManager.getProperty("SFTPUSER", "");
        String SFTPPASS = PropertyManager.getProperty("SFTPPASS", "");
        String SFTPWORKINGDIR = PropertyManager.getProperty("SFTPWORKINGDIR", "");

        com.jcraft.jsch.Session session = null;
        com.jcraft.jsch.Channel channel = null;
        ChannelSftp channelSftp = null;
        logger.info("Preparing the host information for sftp.");
        try {
            JSch jsch = new JSch();
            session = jsch.getSession(SFTPUSER, SFTPHOST, SFTPPORT);
            session.setPassword(SFTPPASS);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            logger.info("Host connected.");
            channel = session.openChannel("sftp");
            channel.connect();
            logger.info("SFTP Channel Opened and Connected.");
            channelSftp = (ChannelSftp) channel;
            channelSftp.put(SFTPWORKINGDIR, "StoreFileExport.zip");
            logger.info("File transfered successfully to host.");
        } catch (Exception ex) {
             logger.error("Exception found while tranfer the response - " + ex.toString());
             ex.printStackTrace();
             throw new GeneralException("Exception found while tranfer the response - " + ex.toString());
        }
        finally{
            channelSftp.exit();
            logger.info("SFTP Channel exited.");
            channel.disconnect();
            logger.info("Channel disconnected.");
            session.disconnect();
            logger.info("Host Session disconnected.");
         }
    }   */
	
	private void CompressExportedFile(String Filename, String zipFileName) throws GeneralException{

    	try{
    	    byte[] buffer = new byte[1024];
    	    rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
    	    String ZipPath = PropertyManager.getProperty("PRICE_ZIP_FOLDER", "");
    	    String TextPath = PropertyManager.getProperty("PRICE_TEXT_FOLDER", "");
    		FileOutputStream fos = new FileOutputStream(rootPath + "\\" + ZipPath + "\\" + zipFileName);
    		ZipOutputStream zos = new ZipOutputStream(fos);
    		ZipEntry ze= new ZipEntry(Filename);
    		zos.putNextEntry(ze);
    		FileInputStream in = new FileInputStream(rootPath + "\\" + TextPath + "\\" + Filename);
    		int length;
    		while ((length = in.read(buffer)) > 0) {
    			zos.write(buffer, 0, length);
    		}
    		in.close();
    		zos.flush();
    		zos.closeEntry();
          	zos.close();
          	fos.flush();
          	fos.close();
          	logger.info("File Compressed.");
         }catch(Exception ex){
    	   ex.printStackTrace();
    	   logger.error("Exception - " + ex.toString());
    	   throw new GeneralException("Exception found while compressing the file - " + ex.toString());
    	}
	}
	
	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}
	public int getlocationLevelId() {
		return locationLevelId;
	}
	
	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}
	public int getLocationId() {
		return locationId;
	}
	
	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}
	public int getProductLevelId() {
		return productLevelId;
	}
	
	public void setProductId(int productId) {
		this.productId = productId;
	}
	public int getproductId() {
		return productId;
	}
	
	public void setIsPerishable(boolean setIsPerishable) {
		this.isPerishable = setIsPerishable;
	}
	public Boolean getisPerishable() {
		return isPerishable;
	}

	 
}


