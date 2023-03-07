package com.pristine.dataload.gianteagle;

import java.io.*;
import java.sql.Connection;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.util.*;

public class CompUPCMappingV2 {
	private static Logger logger = Logger.getLogger("CompUPCMappingV2");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String COMP_NAME = "COMP_NAME=";
	private String rootPath;
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	private static String relativeInputPath;
	private static String compName;
	private Connection conn = null;
	List<CompetitiveDataDTO> compDataList;
	CompetitiveDataDAO competitiveDataDAO;
	
	private CompUPCMappingV2() {
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException gex) {
			logger.error("Error when creating connection - " + gex);
		}
	}
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-comp-upc-mapping.properties");
		PropertyManager.initialize("analysis.properties");
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
			}
			if (arg.startsWith(COMP_NAME)) {
				compName = arg.substring(COMP_NAME.length());
			}
		}
		CompUPCMappingV2 fileFormatter = new CompUPCMappingV2();
		try {
			fileFormatter.processFile();
		} catch (IOException | GeneralException e) {
			e.printStackTrace();
		}
	}
	
	private void processFile() throws GeneralException, IOException {
		competitiveDataDAO =new CompetitiveDataDAO(conn);
		ExcelFileParser parser = new ExcelFileParser();
		String inputPath = rootPath + "/" + relativeInputPath;
		parser.setFirstRowToProcess(2); // skip header row
		TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
		mapFieldNames(fieldNames);
		ArrayList<String> fileList = parser.getFiles(relativeInputPath);
		String fileName = null;
		try {
			for (String processigFile : fileList) {
				logger.info("Processing file - " + processigFile);
				fileName = processigFile;
				compDataList = parser.parseExcelFile(CompetitiveDataDTO.class, fileName, 0, fieldNames);
				processCompData();
				compDataList.clear();
				PrestoUtil.moveFile(fileName, inputPath + "/" + Constants.COMPLETED_FOLDER);
				PristineDBUtil.commitTransaction(conn, "batch record update");
			}
		} catch (GeneralException ge) {
			logger.error("Error when parsing or processing input file", ge);
			PrestoUtil.moveFile(fileName, inputPath + "/" + Constants.BAD_FOLDER);
			PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
		} finally {
			PristineDBUtil.close(conn);
		}
	}
	
	private void processCompData() throws GeneralException {
		
		if(compDataList!=null && compDataList.size()>0){
			HashMap<String, List<CompetitiveDataDTO>> locationAndCompData = new HashMap<>();
			for (CompetitiveDataDTO competitiveDataDTO : compDataList) {
				if (competitiveDataDTO.retailerItemCode != null && !competitiveDataDTO.retailerItemCode.isEmpty()) {
					
					
					competitiveDataDTO.retailerItemCode = Long.toString((long) (Double.valueOf(competitiveDataDTO.retailerItemCode).longValue()));

					competitiveDataDTO.compSKU = Long.toString((long) (Double.valueOf(competitiveDataDTO.compSKU).longValue()));
					
					List<CompetitiveDataDTO> competitiveDataDTOs = new ArrayList<>();
					if(locationAndCompData.containsKey(compName)) {
						competitiveDataDTOs = locationAndCompData.get(compName);
					}
					competitiveDataDTOs.add(competitiveDataDTO);
					locationAndCompData.put(compName, competitiveDataDTOs);
					
				}
			}
			HashMap<String, Integer> compStrAndChainInfo = new HashMap<>();
			competitiveDataDAO.getCompChainId(compStrAndChainInfo);
			
			ItemDAO itemDAO = new ItemDAO();
			// HashMap<"Retailer Item code", List<ItemDTO>>
			HashMap<String, List<ItemDTO>> retItemCodeAndItem = itemDAO.getRetItemCodeAndItem(conn);
			// Load data
			competitiveDataDAO.insertCompUPCMapping(conn, locationAndCompData, compStrAndChainInfo, retItemCodeAndItem);
		}
	}
	
	private void mapFieldNames(TreeMap<Integer, String> fieldNames) {
		fieldNames.put(0, "retailerItemCode");
		fieldNames.put(1, "itemName");
		fieldNames.put(2, "size");
		fieldNames.put(3, "uom");
		fieldNames.put(4, "deptName");
		fieldNames.put(5, "deptName");
		fieldNames.put(6, "categoryName");
		fieldNames.put(7, "subCategoryName");
		fieldNames.put(8, "compSKU");
	}
}
