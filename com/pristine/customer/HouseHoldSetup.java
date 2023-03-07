package com.pristine.customer;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.customer.CustomerDAO;
import com.pristine.dto.customer.HouseHoldDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class HouseHoldSetup extends PristineFileParser{

		static Logger logger = Logger.getLogger("ZoneSetup");
		CustomerDAO customerDAO = new CustomerDAO();
		private List<HouseHoldDTO> updateList;
		int stopCount = -1;
		long countRecords = 0;
		Connection conn = null;

		public HouseHoldSetup() {
			super("analysis.properties");
			try {
				conn = DBManager.getConnection();

			} catch (GeneralException ex) {

			}
		}

		public static void main(String[] args) {
			PropertyConfigurator.configure("log4j-houseHoldSetup.properties");
			PropertyManager.initialize("analysis.properties");
			String subFolder = null;

			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];
				if (arg.startsWith("INPUT_FOLDER")) {
					subFolder = arg.substring("INPUT_FOLDER=".length());
				}
			}

			HouseHoldSetup houseHoldSetup = new HouseHoldSetup();
			houseHoldSetup.processHouseHoldSetupFile(subFolder);
		}

		public void processRecords(List listobj) throws GeneralException {
			long startTime = System.currentTimeMillis();
			for (int jj = 0; jj < listobj.size(); jj++) {
				countRecords++;
				HouseHoldDTO houseHoldDTO = (HouseHoldDTO) listobj.get(jj);
				//Setup the data either to update or needs to be inserted...
				for(int i = houseHoldDTO.getConsumerID().length();i < 12;i++ ){
					houseHoldDTO.setConsumerID("0"+houseHoldDTO.getConsumerID());
				}
				updateList.add(houseHoldDTO);
			}
			loadRecords();
			updateList.clear();
			PristineDBUtil.commitTransaction(conn, "batch record update");
			logger.info("Number of records Processed: "+countRecords);
		}

		private void processHouseHoldSetupFile(String subFolder) {
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
						//To get list of loyalty card number from Customer_loyalty_info table...
						updateList = new ArrayList<HouseHoldDTO>();
						String fieldNames[] = new String[4];
						setHeaderPresent(true);
						fieldNames[0] = "retailAbbreviation";
						fieldNames[1] = "groupID";
						fieldNames[2] = "consumerID";
						fieldNames[3] = "consumerIDType";
						logger.info("House hold Setup starts");
						
						for (int j = 0; j < fileList.size(); j++) {
							// clearVariables();
							logger.info("processing - " + fileList.get(j));
							parseDelimitedFile(HouseHoldDTO.class, fileList.get(j), ',', fieldNames, stopCount);
						}
						
						PristineDBUtil.commitTransaction(conn, "batch record update");
						logger.info("House hold Setup Completed");
					} catch (GeneralException ge) {
						ge.printStackTrace();
						logger.error(ge.toString(), ge);
						logger.error("Error while processing houseHold Setup File - " + ge);
						PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
						commit = false;
					} catch (Exception e) {
						logger.error("Error while processing houseHold Setup File - " + e);
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
			customerDAO.updateCustomerLoyaltyInfo(conn, updateList);
//			if(insertList != null){
//				int insertCount = customerDAO.insertintoCustomerLoyatyInfo(conn, insertList);
//				logger.info("Number of Records Inserted: "+insertCount);
//			}
		}
	}
