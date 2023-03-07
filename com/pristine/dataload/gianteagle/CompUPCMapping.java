package com.pristine.dataload.gianteagle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class CompUPCMapping {
	private static Logger logger = Logger.getLogger("CompUPCMapping");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private String rootPath;
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	private static String relativeInputPath;
	private Connection conn = null;
	List<CompetitiveDataDTO> compDataList;
	CompetitiveDataDAO competitiveDataDAO;
	
	private CompUPCMapping() {
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
		}
		CompUPCMapping fileFormatter = new CompUPCMapping();
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
				String tempFileName = processigFile.replace(inputPath, "").toUpperCase().replace(".XLSX", "")
						.replace("/", "").trim();
				logger.info("Processing file - " + processigFile);
				fileName = processigFile;
				compDataList = parser.parseExcelFile(CompetitiveDataDTO.class, fileName, 1, fieldNames);
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
		HashMap<String, List<CompetitiveDataDTO>> compDetailBasedOnStoreLocation = new HashMap<>();
		if(compDataList!=null && compDataList.size()>0){
			
			for (CompetitiveDataDTO competitiveDataDTO : compDataList) {
				if (competitiveDataDTO.upc != null && !competitiveDataDTO.upc.isEmpty()) {
					HashMap<String, HashMap<String, List<CompetitiveDataDTO>>> compDetailsBasedOnCompName = new HashMap<>();
					
					competitiveDataDTO.upc = Long.toString((long) (Double.valueOf(competitiveDataDTO.upc).longValue()));
					
					if(competitiveDataDTO.retailerItemCode != null && !competitiveDataDTO.retailerItemCode.isEmpty()){
						competitiveDataDTO.retailerItemCode = Long.toString((long) (Double.valueOf(competitiveDataDTO.retailerItemCode).longValue()));
					}
					// Assign UPC as per Presto Standard
					String UPCWithNoCheckDigit;
					String upc = String.valueOf(competitiveDataDTO.upc);
					if (upc.length() > 11)
						UPCWithNoCheckDigit = upc.substring(0, 11);
					else
						UPCWithNoCheckDigit = upc;
					upc = PrestoUtil.castUPC(UPCWithNoCheckDigit, false);
					competitiveDataDTO.upc = upc;

					if (competitiveDataDTO.podWM != null && competitiveDataDTO.podWM != "" && !competitiveDataDTO.podWM.isEmpty()) {
						CompetitiveDataDTO compData = new CompetitiveDataDTO();
						compData.copy(competitiveDataDTO);
						compData.storeName = "podWM";
						compData.compSKU = Long.toString((long) (Double.valueOf(competitiveDataDTO.podWM).longValue()));
						getCompDetailsBasedOnCompName("WALMART", compData.compSKU, compData, compDetailsBasedOnCompName);
//						groupBasedOnCompStore(compData, compDetailBasedOnStoreLocation, "podWM");
					}

					if (competitiveDataDTO.codWM != null && competitiveDataDTO.codWM != "" && !competitiveDataDTO.codWM.isEmpty()) {
						CompetitiveDataDTO compData = new CompetitiveDataDTO();
						compData.copy(competitiveDataDTO);
						compData.storeName = "codWM";
						compData.compSKU = Long.toString((long) (Double.valueOf(competitiveDataDTO.codWM).longValue()));
						getCompDetailsBasedOnCompName("WALMART", compData.compSKU, compData, compDetailsBasedOnCompName);
//						groupBasedOnCompStore(compData, compDetailBasedOnStoreLocation, "codWM");
					}

					if (competitiveDataDTO.colWM != null && competitiveDataDTO.colWM != "" && !competitiveDataDTO.colWM.isEmpty()) {
						CompetitiveDataDTO compData = new CompetitiveDataDTO();
						compData.copy(competitiveDataDTO);
						compData.storeName = "colWM";
						compData.compSKU = Long.toString((long) (Double.valueOf(competitiveDataDTO.colWM).longValue()));
						getCompDetailsBasedOnCompName("WALMART", compData.compSKU, compData, compDetailsBasedOnCompName);
//						groupBasedOnCompStore(compData, compDetailBasedOnStoreLocation, "colWM");
					}

					if (competitiveDataDTO.colKR != null && competitiveDataDTO.colKR != "" && !competitiveDataDTO.colKR.isEmpty()) {
						CompetitiveDataDTO compData = new CompetitiveDataDTO();
						compData.copy(competitiveDataDTO);
						compData.storeName = "colKR";
						compData.compSKU = Long.toString((long) (Double.valueOf(competitiveDataDTO.colKR).longValue()));
						getCompDetailsBasedOnCompName("KROGER", compData.compSKU, compData, compDetailsBasedOnCompName);
//						groupBasedOnCompStore(compData, compDetailBasedOnStoreLocation, "colKR");
					}

					if (competitiveDataDTO.colMJ != null && competitiveDataDTO.colMJ != "" && !competitiveDataDTO.colMJ.isEmpty()) {
						CompetitiveDataDTO compData = new CompetitiveDataDTO();
						compData.copy(competitiveDataDTO);
						compData.storeName = "colMJ";
						compData.compSKU = Long.toString((long) (Double.valueOf(competitiveDataDTO.colMJ).longValue()));
						getCompDetailsBasedOnCompName("MEIJER", compData.compSKU, compData, compDetailsBasedOnCompName);
//						groupBasedOnCompStore(compData, compDetailBasedOnStoreLocation, "colMJ");
					}
					getChainLevelDataBasedOnCompName(compDetailsBasedOnCompName, compDetailBasedOnStoreLocation);
				}
			}
			
			HashMap<String, Integer> compStrAndChainInfo = competitiveDataDAO.getCompStoreId();
			competitiveDataDAO.getCompChainId(compStrAndChainInfo);
			
			// Load data
			competitiveDataDAO.insertCompUPCMapping(conn, compDetailBasedOnStoreLocation, compStrAndChainInfo);
		}
	}
	
	private void getChainLevelDataBasedOnCompName(HashMap<String, HashMap<String, List<CompetitiveDataDTO>>> compDetailsBasedOnCompName,
			HashMap<String, List<CompetitiveDataDTO>> compDetailBasedOnStoreLocation){
		
		HashMap<String, List<CompetitiveDataDTO>> compNameWithLatestCompDetails = new HashMap<>();
		
		compDetailsBasedOnCompName.forEach((compName, compDetails)->{
			compDetails.forEach((key,values)->{
				int sizeOfCompDetailsList = values.size();
				
				values.forEach(compItem->{
					compItem.setTotalScore(sizeOfCompDetailsList);
					
					List<CompetitiveDataDTO> tempList = new ArrayList<>();
					if(compNameWithLatestCompDetails.containsKey(compName)){
						tempList= compNameWithLatestCompDetails.get(compName);
					}
					tempList.add(compItem);
					compNameWithLatestCompDetails.put(compName, tempList);
				});
			});
		});
		
		compNameWithLatestCompDetails.forEach((key, values)->{
			
			values.sort(Comparator.comparing(CompetitiveDataDTO::getTotalScore).reversed());
			
			int totalScore = 0;
			for(CompetitiveDataDTO compData: values){
				if(compData.getTotalScore()>0 && totalScore == 0){
					totalScore = compData.getTotalScore();
					groupBasedOnCompStore(compData, compDetailBasedOnStoreLocation, key);
				}else if(totalScore > 0 && compData.getTotalScore() < totalScore){
					groupBasedOnCompStore(compData, compDetailBasedOnStoreLocation, compData.storeName);
				}
			}
		});
	}
	
	private void getCompDetailsBasedOnCompName(String compName, String compSKUorUPC,
			CompetitiveDataDTO competitiveDataDTO, HashMap<String, HashMap<String, List<CompetitiveDataDTO>>> compDetailsBasedOnCompName) {
		
		HashMap<String, List<CompetitiveDataDTO>> compDetailsBasedOnSKUorUPC = new HashMap<>();
		List<CompetitiveDataDTO> competitiveDataDTOs = new ArrayList<>();
		if(compDetailsBasedOnCompName.containsKey(compName)){
			compDetailsBasedOnSKUorUPC = compDetailsBasedOnCompName.get(compName);
			if(compDetailsBasedOnSKUorUPC.containsKey(compSKUorUPC)){
				competitiveDataDTOs = compDetailsBasedOnSKUorUPC.get(compSKUorUPC);
			}
		}
		
		competitiveDataDTOs.add(competitiveDataDTO);
		compDetailsBasedOnSKUorUPC.put(compSKUorUPC, competitiveDataDTOs);
		compDetailsBasedOnCompName.put(compName, compDetailsBasedOnSKUorUPC);
	}
	
	private void groupBasedOnCompStore(CompetitiveDataDTO competitiveDataDTO,HashMap<String, List<CompetitiveDataDTO>> compDetailBasedOnStoreLocation,
			String compAndStoreName) {
		List<CompetitiveDataDTO> podWMList = new ArrayList<>();
		if (compDetailBasedOnStoreLocation.get(compAndStoreName) != null) {
			podWMList = compDetailBasedOnStoreLocation.get(compAndStoreName);
		}
		podWMList.add(competitiveDataDTO);
		compDetailBasedOnStoreLocation.put(compAndStoreName, podWMList);
	}
	
	private void mapFieldNames(TreeMap<Integer, String> fieldNames) {
		fieldNames.put(0, "upc");
		fieldNames.put(1, "retailerItemCode");
		fieldNames.put(2, "deptName");
		fieldNames.put(3, "categoryName");
		fieldNames.put(4, "subCategoryName");
		fieldNames.put(5, "itemName");
		fieldNames.put(6, "size");
		fieldNames.put(7, "uom");
		fieldNames.put(8, "podWM");
		fieldNames.put(9, "codWM");
		fieldNames.put(10, "colWM");
		fieldNames.put(11, "colKR");
		fieldNames.put(12, "colMJ");
	}
}
