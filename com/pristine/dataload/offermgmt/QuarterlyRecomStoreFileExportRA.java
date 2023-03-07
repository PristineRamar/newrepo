/*
 * Title: Store Price Export for RiteAid
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	09/21/2020	John Britto		Initial Version 
 **********************************************************************************
 */

package com.pristine.dataload.offermgmt;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.PriceExportDAO;
import com.pristine.dao.offermgmt.StoreFileExportGEDAO;
import com.pristine.dto.offermgmt.NotificationDetailInputDTO;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.StoreFileExportDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.PGPEncryptDecryptService;
import com.pristine.service.offermgmt.NotificationService;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.SFTPFileTransferUtil;
import com.pristine.util.StoreFileExportUtil;
import com.pristine.util.offermgmt.PRConstants;

public class QuarterlyRecomStoreFileExportRA {
	private StoreFileExportGEDAO storeFileExportDAO = null;
	private static Logger logger = Logger.getLogger("StoreFileExport");
	private Connection conn = null;
	private String rootPath;
	private String rootPathBackup;
	private String relativeOutputPath;
	private String fileName;
	private String pgpFolder;
	private String PASSPHRASE;
	private String publicKeyFile;
	private String privateKeyFile;

	public SFTPFileTransferUtil sftpUtil;
	
	DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MMddyyyy_HHmm");
	LocalDateTime now = LocalDateTime.now();

	public QuarterlyRecomStoreFileExportRA() {
		PropertyConfigurator.configure("log4j-store-export.properties");
		PropertyManager.initialize("recommendation.properties");
		try {
			conn = DBManager.getConnection();
			storeFileExportDAO = new StoreFileExportGEDAO(conn);

			rootPath = PropertyManager.getProperty("DATAUPLOAD.ROOTPATH");
			rootPathBackup = PropertyManager.getProperty("DATABACKUP.ROOTPATH");
			relativeOutputPath = PropertyManager.getProperty("STORE_PRICE_FILE_PATH");
			fileName = PropertyManager.getProperty("STORE_PRICE_FILE_NAME");
			pgpFolder = PropertyManager.getProperty("STORE_PRICE_PGP_FOLDER");
			PASSPHRASE = PropertyManager.getProperty("STORE_PRICE_FILE_ENC_PASSPHRASE");
			publicKeyFile =  PropertyManager.getProperty("STORE_PRICE_FILE_PUBLIC_KEY_FILE_PATH");
			privateKeyFile =  PropertyManager.getProperty("STORE_PRICE_FILE_PRIVATE_KEY_FILE_PATH");

		} catch (GeneralException exe) {
			logger.error("Error while connecting to DB:" + exe);
			System.exit(1);
		}
	}

	public static void main(String[] args) throws GeneralException {
		QuarterlyRecomStoreFileExportRA storeFileExport = new QuarterlyRecomStoreFileExportRA();

		storeFileExport.processStoreExportData();
	}

	private void processStoreExportData() {
		/*
		 * Steps 
		 * 1: Get Recommendation run Headers 
		 * 2: Get Export data based on list of Run IDs 
		 * 3: Write the data into file  
		 * 4. Update export status at item level 
		 * 5: Update recommendation header with export status 
		 * 6: Add Notification entries
		 */

		try {
			// List<StoreFileExportDTO> storeFileExportMap = new
			// ArrayList<StoreFileExportDTO>();
			ArrayList<StoreFileExportDTO> storeFileExportList = new ArrayList<StoreFileExportDTO>();

			// 1.Get the recommendation header
			List<PRRecommendationRunHeader> recHeaderList = storeFileExportDAO.getApprovedQuarterlyRecHeader(conn);

			// If any approved recommendation exist, process by run id
			if (recHeaderList != null && recHeaderList.size() > 0) {
				logger.debug("Total approved recommendations: " + recHeaderList.size());

				// Get export data
				for (int i = 0; i < recHeaderList.size(); i++) {
					logger.debug("Get recommended items for Run Id " + recHeaderList.get(i).getRunId());
					ArrayList<StoreFileExportDTO> fileExportList = storeFileExportDAO.getQuarterlyRecDataForStoreExportForRiteAid(recHeaderList.get(i).getRunId());
					storeFileExportList.addAll(fileExportList);
				}
			} else
				logger.info("There is no approved recommendation");
			
			if(storeFileExportList.size() > 0) {
			// Write the data into file
			logger.debug("Total prices to export: " + storeFileExportList.size());
			
			HashMap<String, List<StoreFileExportDTO>> storeExportbyUsedId = (HashMap<String, List<StoreFileExportDTO>>) storeFileExportList.stream()
					.collect(Collectors.groupingBy(StoreFileExportDTO :: getUserId));
			
			for(Map.Entry<String, List<StoreFileExportDTO>> userEntry : storeExportbyUsedId.entrySet()) {
			logger.debug("Write export data approved by: " + userEntry.getKey());
			writeStoreData(userEntry.getValue(), userEntry.getKey());	
			}
			boolean encrypted = false;
			File fileDirectoryPath = new File(rootPath + relativeOutputPath);
		    File fileContents[] = fileDirectoryPath.listFiles();
		    for (File file : fileContents) {
		    	if((file.getName()).contains(".txt")/*.equals(fileName  + dtf.format(now) + ".txt")*/) {
		    		//pgp encryption
		    		encrypted = encryptTheFile(file.getName());
		    	}
		    }		
		    
		    //boolean decrypted = decryptFile();
		    			
		    if(encrypted) {
		    	writeEmptyFile();
		    	//delete zip file only when the file is encrypted
		    	File zipFileDirectoryPath = new File(rootPath + relativeOutputPath);
			    File zipFileContents[] = zipFileDirectoryPath.listFiles();
			    for (File file : zipFileContents) {
			    	// If file transferred then delete files in output directory
			    	PrestoUtil.deleteFile(rootPath + relativeOutputPath+file.getName());
				}
		    	
			    //Transfer encrypted files via SFTP	to client's network		 
		    	sftpUtil = new SFTPFileTransferUtil();
				boolean fileTransferred = sftpUtil.transferExportedFileThroughSFTP(
						rootPath + relativeOutputPath + pgpFolder + "/");

				if (fileTransferred) {
					//delete pgp file only after the files are transferred to client
					File directoryPath = new File(rootPath + relativeOutputPath + pgpFolder + "/");
				    File contents[] = directoryPath.listFiles();
				    for (File file : contents) {
				    	// If file transferred then delete files in output directory
				    	PrestoUtil.deleteFile(rootPath + relativeOutputPath+ pgpFolder + "/" + file.getName());
					}
				}

		    }
		    else {
		    	logger.error("File is not encrypted to PGP");
		    	logger.error("Failed to send the file.");
		    	throw new Exception("File is not encrypted to PGP.. FAILED to send file.");
		    }
			 
			
			if (recHeaderList != null && recHeaderList.size() > 0) {
				logger.debug("Update status in DB...");
				StoreFileExportUtil.updateStatus(recHeaderList,storeFileExportDAO,conn);
			}
		   }

		} catch (GeneralException ge) {
			logger.error("Error in getStoreExportData " + ge.getMessage());
		} catch (Exception e) {
			logger.error("Exception in getStoreExportData " + e.getMessage());
		} finally {
			PristineDBUtil.close(conn);
		}
	}

	private void writeEmptyFile() {

		try {
			String FileName = PropertyManager.getProperty("STORE_PRICE_FILE_NAME");
			String fileName = FileName + ".done";
			String outputPath = rootPath + "/" + relativeOutputPath + pgpFolder + "/" + fileName;

			File file = new File(outputPath);
			if (!file.exists()) {
				file.createNewFile();
				logger.info("writeEmptyFile() - Empty file created");
			}

		} catch (Exception ex) {
			logger.error("Exception - Error creating an empty file - ", ex);
			ex.printStackTrace();
		}

	}

	public ArrayList<String> getZipFiles(String specificPath) throws GeneralException 
	{
    	String fullPath = rootPath;
	    if(specificPath!=null && specificPath.trim().length()>0)
	    {
	        fullPath=fullPath + "/"+specificPath;
	    }
	    ArrayList <String> fileList = new ArrayList <String> ();

	    File dir = new File(fullPath);
	    String[] children = dir.list();
        if (children != null) {
            for (int i=0; i<children.length; i++) {
                // Get filename of file or directory
                String filename = children[i];
                
                //logger.debug(filename);
                if( filename.contains(".zip"))
                {
                     fileList.add(fullPath + "/" + filename);
                     logger.info("getZipFiles() - filename=" + filename);
                }
            }
        }
		return fileList;
	}

	private boolean decryptFile() throws Exception {
		String DE_INPUT = rootPath + relativeOutputPath + pgpFolder + "/" + fileName + dtf.format(now)+ ".zip.pgp";
		String DE_OUTPUT = rootPath+"file.zip";
		String DE_KEY_FILE = privateKeyFile;

		PGPEncryptDecryptService p = new PGPEncryptDecryptService();
		p.setInputFileName(DE_INPUT);
		p.setOutputFileName(DE_OUTPUT);
		p.setPassphrase(PASSPHRASE);
		p.setSecretKeyFileName(DE_KEY_FILE);
		boolean decrypted = p.decrypt();

		return decrypted;
	}

	private boolean encryptTheFile(String fileName) {
		boolean encrypted = false;
		PGPEncryptDecryptService encryptService = new PGPEncryptDecryptService();

		try {
			
			String E_INPUT = rootPath + relativeOutputPath + fileName;
			
			encryptService.setInputFileName(E_INPUT);
			logger.info("encryptTheFile() From: " + E_INPUT);

			String name = fileName.replace(".txt", ".pgp");
			
			String E_OUTPUT = rootPath + relativeOutputPath + pgpFolder + "/" + name;
			encryptService.setOutputFileName(E_OUTPUT);
			logger.info("encryptTheFile() To: " + E_OUTPUT);

			String E_KEY_FILE = publicKeyFile;

			encryptService.setPassphrase(PASSPHRASE);
			encryptService.setPublicKeyFileName(E_KEY_FILE);
		
		} catch (Exception e) {
			logger.info("encryptTheFile() - Error while setting up file path " + e);
		}

		try {
			encrypted = encryptService.encrypt();
		} catch (Exception e) {
			logger.info("encryptTheFile() - Error while encrypting file " + e);
		}

		return encrypted;

	    
	}

	private void writeStoreData(List<StoreFileExportDTO> storePriceList, String userId) throws GeneralException {
		

		
		PrintWriter pw = null;
		FileWriter fw = null;
		try {
			String FileName = PropertyManager.getProperty("STORE_PRICE_FILE_NAME");

			String fileName = FileName +"_"+userId+"_"
					+ ""+dtf.format(now)+ ".txt";
			String outputPath = rootPath + "/" + relativeOutputPath + fileName;
			logger.debug("Output file path: " + outputPath);
			File file = new File(outputPath);

			// If file not exist, create new. If exist, append the same
			if (!file.exists()) {
				fw = new FileWriter(outputPath);

				pw = new PrintWriter(fw, true);
				pw.print(
						"User ID|Price Band|Item Indicator|Item or Original Item or Priced Alike or Like Item Group|Retail UOM Pieces|Retail Effective Date|Retail Price Multiple|Retail Price|Reason Code|Comments|Approval Reason|Timestamp");
				pw.println("");
			}

			else {
				fw = new FileWriter(outputPath, true);
				pw = new PrintWriter(fw, true);
			}

			if (!file.exists())
				pw.print(
						"User ID|Price Band|Item Indicator|Item or Original Item or Priced Alike or Like Item Group|Retail UOM Pieces|Retail Effective Date|Retail Price Multiple|Retail Price|Reason Code|Comments|Approval Reason|Timestamp");

			try {
				List<StoreFileExportDTO> StoreFileExportDTOList = (List<StoreFileExportDTO>) storePriceList;
				for (int j = 0; j < StoreFileExportDTOList.size(); j++) {
					StoreFileExportDTO StoreFileExportDTO = StoreFileExportDTOList.get(j);

					// User ID
					pw.print(StoreFileExportDTO.getUserId());
					pw.print("|");

					// Price Band
					pw.print(StoreFileExportDTO.getZoneNo());
					pw.print("|");

					// Item Indicator
					pw.print(StoreFileExportDTO.getItemIndicator());
					pw.print("|");

					// Item or Original Item or Priced Alike or Like Item Group
					pw.print(StoreFileExportDTO.getRetItemCode());
					pw.print("|");

					// Retail UOM Pieces
					pw.print(StoreFileExportDTO.getUOMData());
					pw.print("|");

					// Retail Effective Date
					pw.print(StoreFileExportDTO.getRegEffectiveDate());
					pw.print("|");

					// Retail Price Multiple
					pw.print(StoreFileExportDTO.getQuantity());
					pw.print("|");

					// Retail Price
					pw.print(StoreFileExportDTO.getPrice());
					pw.print("|");
					
					
					// Reason Code
					if(StoreFileExportDTO.getReasonCode() == null || StoreFileExportDTO.getReasonCode().isEmpty()) {
						pw.print("");					
					}else {
						pw.print(StoreFileExportDTO.getReasonCode());
					}
					pw.print("|");

					// Comments
					if (StoreFileExportDTO.getComment() == null || StoreFileExportDTO.getComment().isEmpty()) {
						pw.print("");
					} else {
						pw.print(StoreFileExportDTO.getComment());
					}
					pw.print("|");
					
					// Approval reason
					if (StoreFileExportDTO.getApprovalReason() == null
							|| StoreFileExportDTO.getApprovalReason().isEmpty()) {
						pw.print("");
					} else {
						pw.print(StoreFileExportDTO.getApprovalReason());
					}
					pw.print("|");
					
					

					// Timestamp
					pw.print(StoreFileExportDTO.getTimeStampData());

					pw.println("");
				}
				pw.close();

				PrestoUtil.zipFile(fileName, rootPath + "/" + relativeOutputPath, FileName+"_"+userId+"_" +dtf.format(now)+ ".zip");
				PrestoUtil.moveFile(rootPath + "/" + relativeOutputPath+FileName+"_"+userId+"_"+dtf.format(now)+ ".zip", rootPathBackup + "/" + relativeOutputPath);
			} catch (Exception ex) {
				logger.error("Outer Exception - JavaException", ex);
				ex.printStackTrace();
				throw new GeneralException("Outer Exception - JavaException - " + ex.toString());
			}
		} catch (Exception ex) {
			logger.error("JavaException", ex);
			throw new GeneralException("JavaException - " + ex.toString());
		}
	}
}