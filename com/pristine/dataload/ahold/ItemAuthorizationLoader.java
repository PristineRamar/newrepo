package com.pristine.dataload.ahold;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.StoreItemMapDAO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.StoreItemMapDTO;
import com.pristine.dto.riteaid.RAItemInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.sun.org.apache.xalan.internal.xsltc.compiler.sym;

public class ItemAuthorizationLoader extends PristineFileParser {
	static Logger logger = Logger.getLogger("ItemAuthorizationLoader");
	int updateInsertCount = 0;
	int recordCount = 0;
	int ignoredCount = 0;
	int statusMismatchCount = 0;
	int updateFailedCount = 0;
	int chainId = 0;
	private int stopCount = -1;
	List<StoreItemMapDTO> notProcessedItems;
	Connection conn = null;
	StoreItemMapDAO storeItemMapDAO = new StoreItemMapDAO();
	RetailPriceDAO retailPriceDAO = new RetailPriceDAO();

	public ItemAuthorizationLoader() {
		super("analysis.properties");
		try {
			// Create DB connection
			conn = DBManager.getConnection();

		} catch (GeneralException ex) {

		}
	}

	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j-ItemAuthorizationLoader.properties");
		String FilePath = PropertyManager.getProperty("DATALOAD.ROOTPATH");
		PropertyManager.initialize("analysis.properties");
		String subFolder = null;
		String listFile = null;

		logger.info("main() - Started");

		for (int ii = 0; ii < args.length; ii++) {

			String arg = args[ii];

			if (arg.startsWith("SUBFOLDER")) {
				subFolder = arg.substring("SUBFOLDER=".length());
			}

			if (arg.startsWith("LIST_FILE")) {
				listFile = arg.substring("LIST_FILE=".length());
			}
			  else
				    listFile = "";
			
		}

		ItemAuthorizationLoader itemAuthorizationLoader = new ItemAuthorizationLoader();
		itemAuthorizationLoader.processItemAuthFile(subFolder,listFile);
	}

	private ArrayList<String> getFileList(String subFolder, String listFile) throws GeneralException {
		ArrayList<String> fileList = new ArrayList<String>();

		String filePath = getRootPath() + "/" + subFolder + "/"  + listFile;
		File file = new File(filePath);
		if (!file.exists()){
    		logger.error("getFileList - File doesn't exist : " + filePath);
			throw new GeneralException("File: " + filePath + " doesn't exist");
		}
		
		BufferedReader br = null;
		try {
			String line;
			FileReader textFile = new FileReader(file);
			boolean allFilesExists = true;
			br = new BufferedReader(textFile);
			while ((line = br.readLine()) != null) {
				if (!line.trim().equals(""))
				{
					String strFile = getRootPath() + "/" + subFolder + "/" + line;
					File file1 = new File(strFile);
					if (file1.exists()) {
						fileList.add(strFile);
					}
					else
					{
						allFilesExists = false;
			    		logger.error("getFileList - File doesn't exist : " + strFile);
					}
				}
			}

			if (!allFilesExists){
				throw new GeneralException("Some files are missing.");
			}
		} catch (Exception e) {
        	throw new GeneralException("Error in getFileList()",e);
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return fileList;
	}
	
	
	void processItemAuthFile(String subFolder,String listFile) {
			boolean commit = true;
			String fieldName[] = new String[5];
			fieldName[0] = "retItemCode";// AUTH_ITEM_NUM
			fieldName[1] = "compStrNo";// AUTH_STORE_NUM
			fieldName[2] = "itemStatus";// AUTH_STATUS
			fieldName[3] = "version";// AUTH_VERSION
			fieldName[4] = "authRec";// AUTH_REC
			logger.info("Item authorization starts");
			ArrayList<String> fileList = new ArrayList<String>(); 
			try {
				if (listFile.trim().isEmpty())
					fileList = getFiles(subFolder);
				else
					fileList = getFileList(subFolder, listFile);
				
			chainId = Integer.parseInt(retailPriceDAO.getChainId(conn));
			for (int j = 0; j < fileList.size(); j++) {
				notProcessedItems = new ArrayList<StoreItemMapDTO>();
				logger.info("processing - " + fileList.get(j));
				long strTime = System.currentTimeMillis();
				
				String line[] = null;
				CsvReader reader = new CsvReader(fileList.get(j));
				int stop = -1;
				reader.setDelimiter('|');
				while(reader.readRecord()){
					line = reader.getValues();
					stop++;
				}
				stop--;
				reader.close();
				String processDate = line[1]; 
				parseDelimitedFile(StoreItemMapDTO.class, fileList.get(j), '|',
						fieldName, stop);
				
				long endTime = System.currentTimeMillis();
				logger.info("Time taken to process file - "
						+ (endTime - strTime) + "ms");
				logger.info("# of Status mismatch records = " + statusMismatchCount);
				logger.info("# of Records ignored = " + ignoredCount);
				logger.info("Saving ignored items...");
				storeItemMapDAO.saveNotProcessedRecords(conn, notProcessedItems, fileList.get(j), processDate);
				logger.info("Total # of Records Precessed = "
						+ updateInsertCount);
			}

			logger.info("Item authorization ends");
			PristineDBUtil.commitTransaction(conn, "batch record update");

		} catch (GeneralException ge) {
			logger.error("Error while processing item authorization files - " + ge.toString());
			commit = false;
			ge.printStackTrace();
		}

		catch (Exception e) {
			logger.error("Error while processing item authorization files - " + e.toString());
			e.printStackTrace();
			commit = false;
		}
		String archivePath = getRootPath() + "/" + subFolder + "/";
		  if( commit ){
				PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
			}
			else{
				PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
			}
		
	}

	@Override
	public void processRecords(List listObject) throws GeneralException {
		ArrayList<StoreItemMapDTO> storeItemMapList = new ArrayList<StoreItemMapDTO>();
		List<StoreItemMapDTO> finalList = new ArrayList<StoreItemMapDTO>();
		Set<String> retItemCodeSet = new HashSet<String>();
		Set<String> strNoSet = new HashSet<String>();

		for (int j = 0; j < listObject.size(); j++) {

			StoreItemMapDTO storeItemMapDTO = (StoreItemMapDTO) listObject
					.get(j);
			String retailerItemCode = Integer.toString(Integer
					.parseInt(storeItemMapDTO.getRetItemCode().split("_")[0]));
		
			storeItemMapDTO.setRetItemCode(retailerItemCode);
			if(!retItemCodeSet.contains(retailerItemCode))
			retItemCodeSet.add(retailerItemCode);
			String storeNumber = storeItemMapDTO.getCompStrNo().substring(1);
			storeItemMapDTO.setCompStrNo(storeNumber);
			if(!strNoSet.contains(storeNumber))
			strNoSet.add(storeNumber);
			storeItemMapList.add(storeItemMapDTO);
		}

		long startTime = System.currentTimeMillis();
		HashMap<String, List<Integer>> itemCodeMap = storeItemMapDAO
				.getItemCodeListForRetItemcodes(conn, retItemCodeSet);
		long endTime = System.currentTimeMillis();
		logger.info("Time taken to retreive itemcode map - "
				+ (endTime - startTime) + "ms");
		
		startTime = System.currentTimeMillis();
		HashMap<String, Integer> compStrNoLevelIdMap = retailPriceDAO
				.getStoreId(conn, chainId, strNoSet);
		endTime = System.currentTimeMillis();
		logger.info("Time taken to retreive storeId map - "
				+ (endTime - startTime) + "ms");

		for (StoreItemMapDTO storeItemMapDTO : storeItemMapList) {
			if (itemCodeMap.containsKey(storeItemMapDTO.getRetItemCode())
					&& compStrNoLevelIdMap.containsKey(storeItemMapDTO
							.getCompStrNo())) {

				List<Integer> itemCodeList = itemCodeMap.get(storeItemMapDTO
						.getRetItemCode());
				for (int itemCode : itemCodeList) {
					StoreItemMapDTO outStoreItemMapDTO = new StoreItemMapDTO();

					boolean canBeAdded = false;
					outStoreItemMapDTO.copy(storeItemMapDTO);
					outStoreItemMapDTO.setItemCode(itemCode);
					outStoreItemMapDTO.setCompStrId(compStrNoLevelIdMap
							.get(storeItemMapDTO.getCompStrNo()));

					if (outStoreItemMapDTO.getItemStatus().equals("")) {
						outStoreItemMapDTO.setIsAuthorized("Y");
						canBeAdded = true;
					}

					else if (outStoreItemMapDTO.getItemStatus().equals("NEW") || outStoreItemMapDTO.getItemStatus().equals("ACT")) {
						outStoreItemMapDTO.setIsAuthorized("Y");
						canBeAdded = true;
					}

					else if (outStoreItemMapDTO.getItemStatus().equals("DEL")) {
						outStoreItemMapDTO.setIsAuthorized("N");
						canBeAdded = true;
					} else {
						statusMismatchCount++;
						notProcessedItems.add(storeItemMapDTO);
					}
					if (canBeAdded) {
						finalList.add(outStoreItemMapDTO);
					}

				}
			} else {
				ignoredCount++;
				notProcessedItems.add(storeItemMapDTO);
			}
		}

		storeItemMapDAO.updateIsAuthorizedFlag(conn, finalList);
		
		updateInsertCount += listObject.size();
		logger.info("processRecords() - # of records processed - "
				+ updateInsertCount);


	}
}
