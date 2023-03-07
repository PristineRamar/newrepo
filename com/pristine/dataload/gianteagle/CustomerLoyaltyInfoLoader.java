package com.pristine.dataload.gianteagle;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.DBManager;
import com.pristine.dao.GEHouseholdDAO;
import com.pristine.dto.dataload.gianteagle.GEHouseholdDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class CustomerLoyaltyInfoLoader {

	private static Logger logger = Logger.getLogger("CustomerLoyaltyInfoLoader");
	private static String FILE_PATH = "FILE_PATH=";
	private static String relativeInputPath;
	String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
	static boolean flag = false;
	Connection conn = null;
	GEHouseholdDAO geHHDAO = new GEHouseholdDAO();

	public CustomerLoyaltyInfoLoader() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException var1_1) {
			logger.error("Error when creating connection - " + var1_1);
		}

	}

	public static void main(String args[]) throws Exception, GeneralException {

		PropertyConfigurator.configure("log4j-CustomerLoyaltyInfoLoader.properties");
		PropertyManager.initialize("analysis.properties");

		CustomerLoyaltyInfoLoader customerInfo = new CustomerLoyaltyInfoLoader();

		for (String arg : args) {
			if (arg.startsWith(FILE_PATH)) {
				relativeInputPath = arg.substring(FILE_PATH.length());
			}

		}

		customerInfo.readFile();

	}

	private void updateDB(List<GEHouseholdDTO> tempProcessedList) throws GeneralException, SQLException {

		int status = geHHDAO.updateHouseHoldInfo(conn, tempProcessedList);

		if (status > 0) {
			try {
				PristineDBUtil.commitTransaction(conn, "batch record update");

			} catch (Exception ex) {

				logger.error("processFile()-Exception while processing file : ");

				PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
			}
		}

	}

	public void readFile() throws Exception, GeneralException {

		File myDirectory = new File(rootPath + "/" + relativeInputPath);
		String[] fileList = myDirectory.list();
		List<GEHouseholdDTO> customerInfo = null;

		for (String fileName : fileList) {

			logger.info("Reading  file" + fileName);
			String file = myDirectory + "/" + fileName;
			customerInfo = readInputFile(file);

		}

		logger.info("readFile()-# of items read:" + customerInfo.size());

		List<GEHouseholdDTO> temp = new ArrayList<>();
		int counter = 0;

		for (GEHouseholdDTO cust : customerInfo) {

			temp.add(cust);
			counter++;

			if (counter == 1000) {
				logger.info("processing # of records" + counter);
				List<GEHouseholdDTO> tempProcessedList = Processdata(temp);
				updateDB(tempProcessedList);
				temp = new ArrayList<>();
				tempProcessedList = new ArrayList<GEHouseholdDTO>();
				counter = 0;

			}

		}

		if (counter > 0) {
			logger.info("processing # of records" + counter);
			List<GEHouseholdDTO> tempProcessedList = Processdata(temp);
			updateDB(tempProcessedList);

		}

	}

	private List<GEHouseholdDTO> Processdata(List<GEHouseholdDTO> customerInfo) throws CloneNotSupportedException {

		List<GEHouseholdDTO> ProcessedList = new ArrayList<GEHouseholdDTO>();

		for (GEHouseholdDTO custInfo : customerInfo) {
			GEHouseholdDTO hhdata = (GEHouseholdDTO) custInfo.clone();
			if (custInfo.getBlockcode().contains("-")) {
				String[] hhdata1 = custInfo.getBlockcode().split("-");
				try {
					if (hhdata1.length > 1) {
						hhdata.setBlockcode(hhdata1[0]);
						hhdata.setZipcode(hhdata1[1]);
					}

					ProcessedList.add(hhdata);

				} catch (ArrayIndexOutOfBoundsException e) {
					// logger.info(message);

				}
			} else {
				hhdata.setZipcode("");
				ProcessedList.add(hhdata);

			}
		}
		return ProcessedList;
	}

	private List<GEHouseholdDTO> readInputFile(String file) throws Exception {

		List<GEHouseholdDTO> householdList = new ArrayList<>();
		CsvReader csvReader = readFilecheck(file, ',');
		String line[];
		int counter = 0;
		while (csvReader.readRecord()) {
			if (counter != 0) {

				line = csvReader.getValues();

				GEHouseholdDTO houseHoldDTO = new GEHouseholdDTO();

				try {
					houseHoldDTO.setHouseholdno(Integer.parseInt(line[0]));
					houseHoldDTO.setBlockcode(line[1]);
					householdList.add(houseHoldDTO);

				}

				catch (Exception ex) {
					logger.info("Ignored record" + line[0] + "as its not a valid number");
					continue;
				}

			}
			counter++;

		}

		/*
		 * for(GEHouseholdDTO a:householdList) { if(a.getHouseholdno()=="" ||
		 * a.getBlockcode()=="") { logger.info(a.getHouseholdno());
		 * logger.info(a.getBlockcode()); } }
		 */
		csvReader.close();
		return householdList;
	}

	private CsvReader readFilecheck(String fileName, char delimiter) throws Exception {

		CsvReader reader = null;
		try {
			reader = new CsvReader(new FileReader(fileName));
			if (delimiter != '0') {
				reader.setDelimiter(delimiter);
			}
		} catch (Exception e) {
			throw new Exception("File read error ", e);
		}
		return reader;

	}

}