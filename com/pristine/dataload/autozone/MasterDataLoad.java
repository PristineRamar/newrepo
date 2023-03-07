package com.pristine.dataload.autozone;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.MasterDataInsertDao;
import com.pristine.dto.masterDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.CsvReaderUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings("rawtypes")
public class MasterDataLoad extends PristineFileParser {

	private static Logger logger = Logger.getLogger("MasterDataLoad");

	static String filepath = null;
	public static char separator;
	private static Connection conn = null;

	public static void main(String[] args) throws Exception {

		MasterDataLoad mDataLoad = new MasterDataLoad();
		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j.properties");

		separator = (PropertyManager.getProperty("SEPARATOR", ",")).charAt(0);

		for (String arg : args) {
			if (arg.startsWith("FILE_PATH=")) {
				filepath = arg.substring("FILE_PATH=".length());
			}
		}
		mDataLoad.processPriceFile(filepath);

		logger.info("----File Insert Completed-----");

	}

	@Override
	public void processRecords(List listobj) throws GeneralException {
		// TODO Auto-generated method stub

	}

	@SuppressWarnings("unchecked")
	private void processPriceFile(String relativePath) {
		ArrayList<String> fileList = null;

		// get zip files
		ArrayList<String> zipFileList = null;

		String zipFilePath = getRootPath() + "/" + relativePath;

		try {
			zipFileList = getZipFiles(relativePath);
		} catch (GeneralException ge) {
			logger.error("Error in setting up objects", ge);
			return;
		}

		int curZipFileCount = -1;
		boolean processZipFile = false;

		do {
			try {

				if (processZipFile)
					PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);

				fileList = getFiles(relativePath);
				for (int i = 0; i < fileList.size(); i++) {
					String file = fileList.get(i);
					logger.info("provcessing file : " + file);
					List<masterDataDTO> fileData = readDeltaFile(file);
					writeDataInDb(fileData);

				}
			} catch (GeneralException ex) {
				logger.error("GeneralException", ex);

			} catch (Exception ex) {
				logger.error("JavaException", ex);

			}

			if (processZipFile) {
				PrestoUtil.deleteFiles(fileList);
				fileList.clear();
				fileList.add(zipFileList.get(curZipFileCount));
			}

			curZipFileCount++;
			processZipFile = true;
		} while (curZipFileCount < zipFileList.size());

		return;

	}

	private void writeDataInDb(List<masterDataDTO> finalList) throws SQLException, GeneralException {
		MasterDataInsertDao masterDataDao = new MasterDataInsertDao();
		conn = getOracleConnection();

		try {
			masterDataDao.writeItemsToDB(conn, finalList);
			PristineDBUtil.commitTransaction(conn, "Records inserted");

		} catch (Exception ex) {
			logger.error("data rolledback " + ex.getMessage());
			PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
		}

	}

	public List<masterDataDTO> readDeltaFile(String file) throws Exception, GeneralException {

		CsvReader csvReader = CsvReaderUtil.readFilecheck(file, separator);
		String line[];
		int counter = 0;

		List<masterDataDTO> fileData = new ArrayList<>();

		while (csvReader.readRecord()) {
			if (counter != 0) {

				line = csvReader.getValues();
				try {

					masterDataDTO masterDataDTO = new masterDataDTO();
					masterDataDTO.setStore(line[0]);
					masterDataDTO.setZone(line[1]);
					masterDataDTO.setZoneName(line[2]);
					masterDataDTO.setCountryCode(line[3]);
					masterDataDTO.setPredicted(line[4]);
					masterDataDTO.setPrimaryDc(line[5]);
					masterDataDTO.setItemCode(Integer.parseInt(line[6]));
					masterDataDTO.setRecommendationUnit(line[7]);
					masterDataDTO.setPartNumber(line[8]);
					masterDataDTO.setHpsfFlag(line[9]);
					masterDataDTO.setRetaileffdate(line[10]);
					masterDataDTO.setDiyRetail(line[11]);
					masterDataDTO.setCoreRetail(line[12]);
					masterDataDTO.setVdpRetail(line[13]);
					masterDataDTO.setLevel(line[14]);
					masterDataDTO.setTotalPriceChange(line[15]);
					masterDataDTO.setApprover(line[16]);
					masterDataDTO.setApproverName(line[17]);
					masterDataDTO.setCeFlag(line[18]);
					masterDataDTO.setStoreLockExpiryFlag(line[19]);
					masterDataDTO.setFileName(new File(file).getName());
					fileData.add(masterDataDTO);

				}

				catch (Exception ex) {
					logger.error("Ignored record for itemcode:" + line[6] + ex);
					continue;
				}
			}

			counter++;
		}

		logger.info("# records read from file : " + new File(file).getName() + " : " + fileData.size());
		return fileData;
	}

}
