package com.pristine.dataload.gianteagle;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dto.fileformatter.gianteagle.ZoneDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ZoneSetup extends PristineFileParser {

	static Logger logger = Logger.getLogger("ZoneSetup");
	private List<ZoneDTO> zoneSetUpList;
	int stopCount = -1;
	Connection conn = null;

	public ZoneSetup() {
		super("analysis.properties");
		try {
			conn = DBManager.getConnection();

		} catch (GeneralException ex) {

		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-ZoneSetup.properties");
		PropertyManager.initialize("analysis.properties");
		String subFolder = null;

		for (int ii = 0; ii < args.length; ii++) {
			String arg = args[ii];
			if (arg.startsWith("INPUT_FOLDER")) {
				subFolder = arg.substring("INPUT_FOLDER=".length());
			}
		}

		ZoneSetup zoneSetup = new ZoneSetup();
		zoneSetup.processZoneSetupFile(subFolder);
	}

	public void processRecords(List listobj) throws GeneralException {
		for (int jj = 0; jj < listobj.size(); jj++) {
			ZoneDTO zoneDTO = (ZoneDTO) listobj.get(jj);
			// Set actual zone number with BnrCd, Zone number, Price group
			// code.If SplrNo is null then don't assign that value
			if (zoneDTO.getSplrNo() == "") {
				zoneDTO.setActualZoneNum(zoneDTO.getBnrCd() + "-" + zoneDTO.getZnNo() + "-" + zoneDTO.getPrcGrpCd());
			} else {
				zoneDTO.setActualZoneNum(zoneDTO.getBnrCd() + "-" + zoneDTO.getZnNo() + "-" + zoneDTO.getPrcGrpCd()
						+ "-" + zoneDTO.getSplrNo());
			}
			if (zoneDTO.getMktAreaDscr() == null || zoneDTO.getMktAreaDscr() == "") {
				zoneDTO.setMktAreaDscr("UNDEFINED");
			}
			zoneSetUpList.add(zoneDTO);
		}

	}

	private void processZoneSetupFile(String subFolder) {
		try {
			ArrayList<String> zipFileList = getZipFiles(subFolder);
			int curZipFileCount = -1;
			boolean processZipFile = false;
			String zipFilePath = getRootPath() + "/" + subFolder;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;

				try {
					if (processZipFile) {
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);
					}
					fileList = getFiles(subFolder);
					zoneSetUpList = new ArrayList<ZoneDTO>();
					String fieldNames[] = new String[7];
					setHeaderPresent(true);
					fieldNames[0] = "znNo";
					fieldNames[1] = "mktAreaDscr";
					fieldNames[2] = "splrNo";
					fieldNames[3] = "prcGrpCd";
					fieldNames[4] = "bnrCd";
					fieldNames[5] = "prcGrpDscr";
					fieldNames[6] = "sysCd";
					logger.info("Zone Setup starts");
					
					for (int j = 0; j < fileList.size(); j++) {
						// clearVariables();
						logger.info("processing - " + fileList.get(j));
						parseDelimitedFile(ZoneDTO.class, fileList.get(j), '|', fieldNames, stopCount);
						loadRecords();
					}
					PristineDBUtil.commitTransaction(conn, "batch record update");
					logger.info("Zone Setup Completed");
				} catch (GeneralException ge) {
					ge.printStackTrace();
					logger.error(ge.toString(), ge);
					logger.error("Error while processing Zone Setup File - " + ge);
					PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
					commit = false;
				} catch (Exception e) {
					logger.error("Error while processing Zone Setup File - " + e);
					commit = false;
				}
				if (processZipFile) {
					PrestoUtil.deleteFiles(fileList);
					fileList.clear();
					fileList.add(zipFileList.get(curZipFileCount));
				}
				String archivePath = getRootPath() + "/" + subFolder + "/";

				if (commit) {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
				} else {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				}
				curZipFileCount++;
				processZipFile = true;
			} while (curZipFileCount < zipFileList.size());
		} catch (GeneralException ex) {
			logger.error("Outer Exception -  GeneralException", ex);
		} catch (Exception ex) {
			logger.error("Outer Exception - JavaException", ex);
		} finally {
			PristineDBUtil.close(getOracleConnection());
		}
	}

	public void loadRecords() throws GeneralException {

		RetailPriceZoneDAO retailPriceZoneDAO = new RetailPriceZoneDAO();
		List<ZoneDTO> insertZoneNumList = new ArrayList<ZoneDTO>();
		List<ZoneDTO> updateZoneNumList = new ArrayList<ZoneDTO>();
		// Get all zone numbers from retail price zone as a list
		retailPriceZoneDAO.updateActiveIndicatorFlag(conn);
		HashSet<String> zoneNumberList = retailPriceZoneDAO.getZoneNumFromRetailPriceZone(conn);
		// Check zone numbers available in existing list else add them in the
		// list
		for (ZoneDTO zoneDTO : zoneSetUpList) {
			if (!zoneNumberList.contains(zoneDTO.getActualZoneNum())) {
				logger.debug("Inside the loop to insert values");
				ZoneDTO zoneDTOs = new ZoneDTO();
				zoneDTOs.setPrcGrpCd(zoneDTO.getPrcGrpCd());
				//Set zone type based on prc grp code
				if(zoneDTO.getPrcGrpCd() == Integer.parseInt(Constants.GE_PRC_GRP_CD_DSD)
						|| zoneDTO.getPrcGrpCd() == Integer.parseInt(Constants.GE_PRC_GRP_CD_DSD1)){
					zoneDTOs.setZoneType(Constants.ZONE_TYPE_V);	
				}else{
					zoneDTOs.setZoneType(Constants.ZONE_TYPE_W);
				}
				zoneDTOs.setActualZoneNum(zoneDTO.getActualZoneNum());
				zoneDTOs.setMktAreaDscr(zoneDTO.getMktAreaDscr());
				insertZoneNumList.add(zoneDTOs);
			}else if (zoneNumberList.contains(zoneDTO.getActualZoneNum())){
				//Set zone type based on prc grp code
				if(zoneDTO.getPrcGrpCd() == Integer.parseInt(Constants.GE_PRC_GRP_CD_DSD)
						|| zoneDTO.getPrcGrpCd() == Integer.parseInt(Constants.GE_PRC_GRP_CD_DSD1)){
					zoneDTO.setZoneType(Constants.ZONE_TYPE_V);	
				}else{
					zoneDTO.setZoneType(Constants.ZONE_TYPE_W);
				}
				updateZoneNumList.add(zoneDTO);
			}
		}
		// Insert the list of new zone numbers into retail price zone
		int insertedRecordCount = retailPriceZoneDAO.insertintoRetialPriceZone(conn, insertZoneNumList);
		//UPdate Zone number active indicator for the existing zones
		int updatedRecordCount = retailPriceZoneDAO.updateRPZActiveIndicator(conn, updateZoneNumList);
		logger.info("Number of Existing ZoneNumber: " + zoneNumberList.size());
		logger.info("Number of New zones inserted: " + insertedRecordCount);
		logger.info("Number of zones Active Indicator updated: " + updatedRecordCount);

	}
}
