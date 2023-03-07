/*
 * Title: Store Price Export for FleetFarm
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	07/19/2022		Initial Version 
 **********************************************************************************
 */

package com.pristine.dataload.offermgmt;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.StoreFileExportGEDAO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.StoreFileExportDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.SFTPFileTransferUtil;
import com.pristine.util.StoreFileExportUtil;

public class QuarterlyRecomStoreFileExportFF {
	private StoreFileExportGEDAO storeFileExportDAO = null;
	private static Logger logger = Logger.getLogger("StoreFileExportFF");
	private Connection conn = null;
	private String rootPath;
	private String relativeOutputPath;
	public Map<Integer, List<StoreDTO>> zoneStoreMapping = new HashMap<Integer, List<StoreDTO>>();
	public Map<Integer, List<StoreDTO>> gloabalZoneStoreMapping = new HashMap<Integer, List<StoreDTO>>();
	public Map<Integer, List<RetailPriceDTO>> clearancePriceData = new HashMap<Integer, List<RetailPriceDTO>>();

	public SFTPFileTransferUtil sftpUtil;
	
	//YYYYMMDD_HHMMSS
	DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYYMMdd_HHMMSS");
	LocalDateTime now = LocalDateTime.now();

	public QuarterlyRecomStoreFileExportFF() {
		PropertyConfigurator.configure("log4j-store-export.properties");
		PropertyManager.initialize("recommendation.properties");
		try {
			conn = DBManager.getConnection();
			storeFileExportDAO = new StoreFileExportGEDAO(conn);

			rootPath = PropertyManager.getProperty("DATAUPLOAD.ROOTPATH");
			relativeOutputPath = PropertyManager.getProperty("STORE_PRICE_FILE_PATH_FF");

		} catch (GeneralException exe) {
			logger.error("Error while connecting to DB:" + exe);
			System.exit(1);
		}
	}

	public static void main(String[] args) throws GeneralException {
		QuarterlyRecomStoreFileExportFF storeFileExport = new QuarterlyRecomStoreFileExportFF();
		storeFileExport.cacheExistingData();
		storeFileExport.processStoreExportData();
	}

	public void cacheExistingData() {
		zoneStoreMapping = storeFileExportDAO.getStoreZoneMapping();
		gloabalZoneStoreMapping = storeFileExportDAO.getGlobalStoreZoneMapping();
		
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
			ArrayList<StoreFileExportDTO> zoneLevelFileExportList = new ArrayList<StoreFileExportDTO>();
			List<StoreFileExportDTO> storeFileExportList = new ArrayList<StoreFileExportDTO>();

			// 1.Get the recommendation header
			List<PRRecommendationRunHeader> recHeaderList = storeFileExportDAO.getApprovedQuarterlyRecHeader(conn);

			// If any approved recommendation exist, process by run id
			if (recHeaderList != null && recHeaderList.size() > 0) {
				logger.debug("Total approved recommendations: " + recHeaderList.size());

				// Get export data
				for (int i = 0; i < recHeaderList.size(); i++) {
					ArrayList<StoreFileExportDTO> fileExportList = storeFileExportDAO.getQuarterlyRecDataForStoreExportForFF(recHeaderList.get(i).getRunId());
					zoneLevelFileExportList.addAll(fileExportList);
				}
				List<Integer> itemCodeList = zoneLevelFileExportList.stream()
						.collect(Collectors.groupingBy(StoreFileExportDTO::getProductId)).keySet().stream()
						.collect(Collectors.toList());
				clearancePriceData = storeFileExportDAO.cacheExistingClearancePriceDataFromDB(itemCodeList);
				List<StoreFileExportDTO> storeLevelFileExportList = convertZoneLevelToStoreLvelList(zoneLevelFileExportList);
				storeFileExportList = excludeClearanceStores(storeLevelFileExportList);
				
			} else
				logger.info("There is no approved recommendation");
			
			if (storeFileExportList.size() > 0) {
				// Write the data into file
				logger.debug("Total prices to export: " + storeFileExportList.size());

				HashMap<String, List<StoreFileExportDTO>> storeExportbyUsedId = (HashMap<String, List<StoreFileExportDTO>>) storeFileExportList
						.stream().collect(Collectors.groupingBy(StoreFileExportDTO::getUserId));

				for (Map.Entry<String, List<StoreFileExportDTO>> userEntry : storeExportbyUsedId.entrySet()) {
					logger.debug("Write export data approved by: " + userEntry.getKey());
					writeStoreData(userEntry.getValue(), userEntry.getKey());
				}
				if (recHeaderList != null && recHeaderList.size() > 0) {
					logger.debug("Update status in DB...");
					StoreFileExportUtil.updateStatus(recHeaderList,storeFileExportDAO,conn);
					StoreFileExportUtil.callAuditTrail(recHeaderList);
				}
			}

		} catch (GeneralException ge) {
			logger.error("Error in getStoreExportData " + ge.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception in getStoreExportData " + e.getMessage());
		} finally {
			PristineDBUtil.close(conn);
		}
	}

	public List<StoreFileExportDTO> excludeClearanceStores(List<StoreFileExportDTO> storeLevelFileExportList) {
		logger.info("Excluding the items in clearance begins....");
		List<StoreFileExportDTO> clearanceList = new ArrayList<StoreFileExportDTO>();
		List<Integer> storeIDs = clearancePriceData.entrySet().parallelStream().map(Map.Entry::getKey)
				.collect(Collectors.toList());
		storeIDs.sort(Comparator.naturalOrder());
		List<StoreFileExportDTO> filteredList = storeLevelFileExportList.parallelStream()
				.filter(s -> Collections.binarySearch(storeIDs, s.getStrId(),
						Comparator.comparing(Integer::intValue)) >= 0)
				.collect(Collectors.toList());
		filteredList.stream().forEach(oneStoreFileExportDTO -> {
			List<RetailPriceDTO> retailPriceDTOs = clearancePriceData.get(oneStoreFileExportDTO.getStrId());
			retailPriceDTOs = retailPriceDTOs.stream().filter(onepriceDTO -> onepriceDTO.getItemcode()
					.equals(String.valueOf(oneStoreFileExportDTO.getProductId()))).collect(Collectors.toList());
			List<RetailPriceDTO> filteredRetailPriceDTOs = retailPriceDTOs.stream()
					.filter(onepriceDTO -> checkIfNewEffectiveDateFallsExistingDate(onepriceDTO,
							oneStoreFileExportDTO.getRegEffectiveDate()) == true)
					.collect(Collectors.toList());
			if (filteredRetailPriceDTOs.size() > 0) {
				clearanceList.add(oneStoreFileExportDTO);
			}
		});
		List<StoreFileExportDTO> clearanceExcludedList = storeLevelFileExportList.stream().filter(s->!clearanceList.contains(s)).collect(Collectors.toList());
		logger.debug("Clearance Excluded records..."+clearanceExcludedList.size());
		return clearanceExcludedList;
	}
	public boolean checkIfNewEffectiveDateFallsExistingDate(RetailPriceDTO retailPriceDTO,String effectiveDateStr){
		
		try {
			Date startDate = DateUtil.toDate(retailPriceDTO.getSaleStartDate());
			Date endDate = null;
			if (retailPriceDTO.getSaleEndDate() != null) {
				endDate = DateUtil.toDate(retailPriceDTO.getSaleEndDate());
			}
			Date effectiveDate = DateUtil.toDate(effectiveDateStr,"yyyy-MM-dd");
			if((startDate.before(effectiveDate) || startDate.equals(effectiveDate)) && (endDate ==null ||endDate.after(effectiveDate) ||endDate.equals(effectiveDate) )){
				return true;
			}
		} catch (GeneralException e) {
			e.printStackTrace();
			logger.error("Exception in date format");
		}
		
		return false;
	}
	public List<StoreFileExportDTO> convertZoneLevelToStoreLvelList(List<StoreFileExportDTO> zoneFileExportList) {
		logger.info("Converting zone level data to store level data starts...");
		List<StoreFileExportDTO> storeLevelFileExportList = new ArrayList<StoreFileExportDTO>();
		zoneFileExportList.forEach(oneStoreFile -> {
			List<StoreDTO> storeList = new ArrayList<StoreDTO>();
			if (zoneStoreMapping.containsKey(oneStoreFile.getLocationId())) {
				storeList = zoneStoreMapping.get(oneStoreFile.getLocationId());

			} else if (gloabalZoneStoreMapping.containsKey(oneStoreFile.getLocationId())) {
				storeList = gloabalZoneStoreMapping.get(oneStoreFile.getLocationId());
			}
			List<StoreFileExportDTO> storeLevelFileExport = getDataAtStoreLevel(oneStoreFile,storeList);
			storeLevelFileExportList.addAll(storeLevelFileExport);
		});	
		logger.info("Converting Zone to store level ends..."+storeLevelFileExportList.size());
		return storeLevelFileExportList;
	}

	private List<StoreFileExportDTO> getDataAtStoreLevel(StoreFileExportDTO oneStoreFile, List<StoreDTO> storeList) {
		List<StoreFileExportDTO> storeLevelFileExportList = new ArrayList<StoreFileExportDTO>();
		storeList.forEach(oneStore -> {
			try {
				StoreFileExportDTO storeFileExportDTO = (StoreFileExportDTO) oneStoreFile.clone();
				storeFileExportDTO.setCompStrNo(oneStore.getStrNum());
				storeFileExportDTO.setStrId(oneStore.getStrId());
				storeLevelFileExportList.add(storeFileExportDTO);
			} catch (CloneNotSupportedException e) {
				logger.error(e.getMessage());
			}
		});
		return storeLevelFileExportList;
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
	
	public void writeStoreData(List<StoreFileExportDTO> storePriceList, String userId) throws GeneralException {
		PrintWriter pw = null;
		FileWriter fw = null;
		try {
			String storePriceFileName = PropertyManager.getProperty("STORE_PRICE_FILE_NAME_FF");

			String fileName = storePriceFileName +"_"
					+ ""+dtf.format(now)+ ".txt";
			String outputPath = rootPath + "/" + relativeOutputPath+"/" + fileName;
			logger.debug("Output file path: " + outputPath);
			File file = new File(outputPath);

			// If file not exist, create new. If exist, append the same
			if (!file.exists()) {
				fw = new FileWriter(outputPath);
				pw = new PrintWriter(fw, true);
				pw.print(
						"Store Number|Zone Number|Zone Name|Dept Id|Class Id|Item Code|Retail|Retail Effective Date");
				pw.println("");
			}

			else {
				fw = new FileWriter(outputPath, true);
				pw = new PrintWriter(fw, true);
				
			}

			if (!file.exists())
				pw.print(
						"Store Number|Zone Number|Zone Name|Dept Id|Class Id|Item Code|Retail|Retail Effective Date");
			try {
				List<StoreFileExportDTO> storeFileExportDTOList = (List<StoreFileExportDTO>) storePriceList;
				for (int j = 0; j <storeFileExportDTOList.size(); j++) {
					StoreFileExportDTO storeFileExportDTO = storeFileExportDTOList.get(j);

					// User ID
					pw.print(storeFileExportDTO.getCompStrNo());
					pw.print("|");

					// Price Band Need to confirm
					pw.print(storeFileExportDTO.getZoneNo());
					pw.print("|");
					//Zone name
					pw.print(storeFileExportDTO.getZoneName());
					pw.print("|");

					// Dept Id
					pw.print(storeFileExportDTO.getDeptId());
					pw.print("|");
					
					//Class Id
					pw.print(storeFileExportDTO.getCatCode());
					pw.print("|");

					// Item code
					pw.print(storeFileExportDTO.getRetItemCode());
					pw.print("|");

					// Retail Price 
					pw.print(storeFileExportDTO.getPrice());
					pw.print("|");

					// Retail Effective date
					if (!StringUtils.isEmpty(storeFileExportDTO.getRegEffectiveDate())) {
						Date retailEffDate = DateUtil.toDate(storeFileExportDTO.getRegEffectiveDate(), "yyyy-MM-dd");
						SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
						pw.print(formatter.format(retailEffDate));
						pw.print("|");
					}
					pw.println("");
				}
				pw.close();
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
