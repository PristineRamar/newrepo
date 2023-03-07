package com.pristine.fileformatter.autozone;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.DBManager;
import com.pristine.dao.Autozone.StoreDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;

@SuppressWarnings({ "unused", "rawtypes" })
public class MCPFileGenerator {

	int cntr = 0;

	public static String input;

	static String rootPath = "";
	public static String output;
	FileWriter fw;
	PrintWriter pw;
	private Connection conn = null;
	static Logger logger = Logger.getLogger("MCPFileGenerator");
	HashMap<String, List<CompetitiveDataDTO>> dataMap = null;
	static HashMap<String, CompetitiveDataDTO> storeMap = new HashMap<String, CompetitiveDataDTO>();
	static List<CompetitiveDataDTO> storeList = new ArrayList<CompetitiveDataDTO>();
	HashMap<String, List<CompetitiveDataDTO>> sortedmap = new HashMap<String, List<CompetitiveDataDTO>>();
	float regPrice;
	static String checkLocation="";

	HashSet<String> distinctStores = new HashSet<String>();
	HashSet<String> regPriceNotFound = new HashSet<String>();
	HashSet<String> storeNotFound = new HashSet<String>();
	StoreDAO storeDAO = new StoreDAO();
	List<CompetitiveDataDTO> mcpList =null;
	HashMap<String, CompetitiveDataDTO> alaskaStores = new HashMap<>();
	HashMap<String, CompetitiveDataDTO> purtoRicoStores = new HashMap<>();

	public MCPFileGenerator() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException var1_1) {
			logger.error("Error when creating connection - " + var1_1);
		}
	}

	public static void main(String args[]) throws Exception, GeneralException {

		PropertyConfigurator.configure("log4j.properties");
		PropertyManager.initialize("analysis.properties");
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		checkLocation=PropertyManager.getProperty("LOCN_SPECIFIC_MCP");
		
		MCPFileGenerator mcpGenerator = new MCPFileGenerator();
		for (String arg : args) {

			if (arg.startsWith("FILE_NAME=")) {
				input = arg.substring("FILE_NAME=".length());
			}

			if (arg.startsWith("OUTPUT_PATH=")) {
				output = arg.substring("OUTPUT_PATH=".length());
			}
		}
		File myDirectory = new File(rootPath + "/" + input);

		mcpGenerator.PopulateStoreMap();
		mcpGenerator.populateFile(myDirectory);

	}

	private void PopulateStoreMap() throws GeneralException {

		
		  storeList = storeDAO.getCompStr(conn);
		  
		  storeList.forEach(store -> {
		  
		  storeMap.put(store.getCompStrNo(), store);
		  
		  });
		  
		  logger.info("PopulateStoreMap()- storeMap size" + storeMap.size());
		 
		if (checkLocation.equalsIgnoreCase("TRUE")) {
			
			alaskaStores=storeDAO.getAlaskaHawaiStores(conn);
			purtoRicoStores=storeDAO.getPurtoRicoStores(conn);
			
			logger.info("PopulateStoreMap()- alaskaStores size" + alaskaStores.size());
			logger.info("PopulateStoreMap()- purtoRicoStores size" + purtoRicoStores.size());
		}
	}

	
	private void populateFile(File myDirectory) throws Exception, GeneralException {

		String[] fileList = myDirectory.list();
		String fName="";
		logger.info("fileList size " + fileList.length);
		for (String fileName : fileList) {
			
			dataMap = new HashMap<String, List<CompetitiveDataDTO>>();
			logger.info("Reading  file" + fileName);
			fName=fileName;
			String file = myDirectory + "/" + fileName;
			readProdFile(file);
			GenerateMCP();
			WriteCategoriesToCsvFile(fName);
			
			logger.info("#regPricenotfound size for: " + regPriceNotFound.size());
			regPriceNotFound.forEach(item -> {

				logger.info(item);
			});
			regPriceNotFound.clear();
			logger.info("*******");

			logger.info("#Stores not Found:" + storeNotFound.size());
			for (String storeNo : storeNotFound) {
				logger.info(storeNo);
			}
			storeNotFound.clear();
			logger.info("*******");
		
		}

	
		// insertnewCompStore();

		

	}

	private void insertnewCompStore() throws GeneralException, SQLException {

		List<String> insertList = new ArrayList<String>();

		for (String mapElement : distinctStores) {
			{
				if (storeMap.containsKey(mapElement.split(";")[0])) {

				} else {
					logger.info("Inserting for:" + mapElement.split(";")[0]);
					insertList.add(mapElement);

				}

			}
			if (insertList.size() > 0) {
				storeDAO.setUpStores(conn, insertList);

			}
			logger.info("#stores  inserted" + insertList.size());
		}
	}

	private void GenerateMCP() {

		mcpList = new ArrayList<CompetitiveDataDTO>();
		
		dataMap.forEach((key, value) -> {

			List<CompetitiveDataDTO> compdata = value;
			HashMap<String, List<CompetitiveDataDTO>> itemMap = new HashMap<String, List<CompetitiveDataDTO>>();
			compdata.forEach(item -> {

				String itemKey = item.getRetailerItemCode() + ";" + item.getCheckDate();
				List<CompetitiveDataDTO> temp = new ArrayList<>();
				if (itemMap.containsKey(itemKey)) {
					temp.addAll(itemMap.get(itemKey));
				}
				temp.add(item);
				itemMap.put(itemKey, temp);

			});

			itemMap.forEach((k, val) -> {

				List<CompetitiveDataDTO> itemdata = val;

				HashMap<Float, Integer> countMap = new HashMap<Float, Integer>();
				HashMap<String, Float> minMap = new HashMap<String, Float>();

				HashMap<String, HashMap<String, List<CompetitiveDataDTO>>> compStrsorted = new HashMap<String, HashMap<String, List<CompetitiveDataDTO>>>();
				HashMap<String, Float> compStrMap = new HashMap<String, Float>();

				for (CompetitiveDataDTO item : itemdata) {

					if (compStrsorted.containsKey(item.getRetailerItemCode() + ";" + item.getCheckDate())) {
						HashMap<String, List<CompetitiveDataDTO>> compStr = compStrsorted
								.get(item.getRetailerItemCode() + ";" + item.getCheckDate());

						List<CompetitiveDataDTO> temp = null;

						if (compStr.containsKey(item.getCompStrNo() + ";" + item.getRetailerItemCode()))

						{
							temp = new ArrayList<>();
							temp = (compStr.get(item.getCompStrNo() + ";" + item.getRetailerItemCode()));
							temp.add(item);

						} else {
							temp = new ArrayList<>();
							temp.add(item);

						}
						compStr.put(item.getCompStrNo() + ";" + item.getRetailerItemCode(), temp);
						compStrsorted.put(item.getRetailerItemCode() + ";" + item.getCheckDate(), compStr);

					} else {
						HashMap<String, List<CompetitiveDataDTO>> compStrnew = new HashMap<String, List<CompetitiveDataDTO>>();
						List<CompetitiveDataDTO> tem = new ArrayList<>();
						tem.add(item);
						compStrnew.put(item.getCompStrNo() + ";" + item.getRetailerItemCode(), tem);

						compStrsorted.put(item.getRetailerItemCode() + ";" + item.getCheckDate(), compStrnew);
					}

				}

				int maxCount = 0;
				float minAmout = 0;
				regPrice = 0;
				int ct = 0;

				for (Map.Entry<String, HashMap<String, List<CompetitiveDataDTO>>> StrPrice : compStrsorted.entrySet()) {

					HashMap<String, List<CompetitiveDataDTO>> compStrnew = StrPrice.getValue();

					for (Map.Entry<String, List<CompetitiveDataDTO>> strWiseprc : compStrnew.entrySet()) {

						List<CompetitiveDataDTO> Strprices = strWiseprc.getValue();

						CompetitiveDataDTO min = Strprices.stream()
								.min(Comparator.comparing(CompetitiveDataDTO::getRegularPrice))
								.orElseThrow(NoSuchElementException::new);
						;
						minMap.put(strWiseprc.getKey(), min.getRegularPrice());
					}

				}

				for (Map.Entry<String, Float> mapElement : minMap.entrySet()) {

					Float va1 = mapElement.getValue();

					if (countMap.containsKey(va1)) {
						int count = countMap.get(va1);
						countMap.put(va1, count + 1);

					} else {
						countMap.put(va1, 1);
					}

				}

				for (Map.Entry mapElement : countMap.entrySet()) {

					String va1 = mapElement.getValue().toString();

					if (ct == 0) {
						maxCount = Integer.parseInt(va1);
						minAmout = (float) mapElement.getKey();
						ct = 1;
					} else if (Integer.parseInt(va1) > maxCount) {
						maxCount = Integer.parseInt(va1);
						minAmout = (float) mapElement.getKey();

					} else if (Integer.parseInt(va1) == maxCount) {
						minAmout = Math.min(minAmout, (float) mapElement.getKey());

					}

				}
				regPrice = minAmout;

				for (CompetitiveDataDTO iData : itemdata) {

					if (iData.getRegularPrice() == regPrice) {
						String compStr = iData.getShortName() + "-MCP";
						iData.setCompStrNo(compStr);
						mcpList.add(iData);
						if (!distinctStores.contains(compStr)) {
							distinctStores.add(compStr + ";" + iData.getCompChainId() + ";" + iData.getCompChainName()
									+ ";" + iData.getAddressLine1() + ";" + iData.getCity() + ";" + iData.getState()
									+ ";" + iData.getZip());
						}
						break;
					}
				}

			});

		});

	}

	public void readProdFile(String file) throws Exception, GeneralException {

		CsvReader csvReader = readFilecheck(file, '|');
		String line[];
		int counter = 0;

		while (csvReader.readRecord()) {
			if (counter != 0) {

				line = csvReader.getValues();
				try {

					if (line[0] != "") {
						CompetitiveDataDTO compDataDTO = new CompetitiveDataDTO();

						compDataDTO.setCompStrNo(line[0]);

						if (line[0].equals("101-0000")) {

							String line1 = line[3].toLowerCase();
							if (!line1.replaceAll("\\s+", "").contains("wholesalercloseout")) {

								compDataDTO.setCheckDate(line[1]);
								compDataDTO.setRetailerItemCode(line[2]);
								compDataDTO.setItemName(line[3]);
								if (!line[5].equals(Constants.EMPTY)) {
									compDataDTO.setRegularPack(Integer.parseInt(line[4]));
									compDataDTO.setRegularPrice(Float.parseFloat(line[5]));
									compDataDTO.setSize(line[6]);
									compDataDTO.setUpc(line[7]);
									compDataDTO.setOutSideRangeInd(line[8]);
									compDataDTO.setComment(line[9]);

									if (!line[11].isEmpty()) {
										if (Float.parseFloat(line[11]) > Float.parseFloat(line[5])) {
											compDataDTO.setSaleMPack(0);
											compDataDTO.setSalePrice(Float.parseFloat("0"));
											compDataDTO.setSaleDate("");
										}
										compDataDTO.setSaleMPack(Integer.parseInt(line[10]));
										compDataDTO.setSalePrice(Float.parseFloat(line[11]));
										compDataDTO.setSaleDate(line[12]);
									} else {
										compDataDTO.setSaleMPack(0);
										compDataDTO.setSalePrice((float) 0);
										compDataDTO.setSaleDate("");
									}

									compDataDTO.setPartNumber(line[13]);

									if (storeMap.containsKey(compDataDTO.getCompStrNo())) {
										CompetitiveDataDTO compData = storeMap.get(compDataDTO.getCompStrNo());
										compDataDTO.setCompChainId(compData.getCompChainId());
										compDataDTO.setCompChainName(compData.getCompChainName());
										compDataDTO.setShortName(compData.getShortName());
										compDataDTO.setAddressLine1(compData.getAddressLine1());
										compDataDTO.setZip(compData.getZip());
										compDataDTO.setCity(compData.getCity());
										compDataDTO.setState(compData.getState());

									} else {
										storeNotFound.add(compDataDTO.getCompStrNo());
										logger.debug("Comp ChainId not found for " + compDataDTO.getRetailerItemCode());
									}

									List<CompetitiveDataDTO> temp = new ArrayList<>();
									if (compDataDTO.getCompChainId() != null) {
										if (dataMap.containsKey(compDataDTO.getCompChainId())) {
											temp.addAll(dataMap.get(compDataDTO.getCompChainId()));
										}
										temp.add(compDataDTO);
										dataMap.put(compDataDTO.getCompChainId(), temp);
									} else {
										logger.debug("IgnoredRecord since compChainId not found for " + line[0]);
									}

								}

								else {
									regPriceNotFound.add(line[2]);
								}

							}
						} else {
							compDataDTO.setCheckDate(line[1]);
							compDataDTO.setRetailerItemCode(line[2]);
							compDataDTO.setItemName(line[3]);
							if (!line[5].equals(Constants.EMPTY)) {
								compDataDTO.setRegularPack(Integer.parseInt(line[4]));
								compDataDTO.setRegularPrice(Float.parseFloat(line[5]));
								compDataDTO.setSize(line[6]);
								compDataDTO.setUpc(line[7]);
								compDataDTO.setOutSideRangeInd(line[8]);
								compDataDTO.setComment(line[9]);

								if (!line[11].isEmpty()) {
									if (Float.parseFloat(line[11]) > Float.parseFloat(line[5])) {
										compDataDTO.setSaleMPack(0);
										compDataDTO.setSalePrice(Float.parseFloat("0"));
										compDataDTO.setSaleDate("");
									}
									compDataDTO.setSaleMPack(Integer.parseInt(line[10]));
									compDataDTO.setSalePrice(Float.parseFloat(line[11]));
									compDataDTO.setSaleDate(line[12]);
								} else {
									compDataDTO.setSaleMPack(0);
									compDataDTO.setSalePrice((float) 0);
									compDataDTO.setSaleDate("");
								}

								compDataDTO.setPartNumber(line[13]);
								
								if(checkLocation.equalsIgnoreCase("TRUE")  && (alaskaStores.containsKey(compDataDTO.getCompStrNo()) 
									|| purtoRicoStores.containsKey(compDataDTO.getCompStrNo())))
										{
									if (alaskaStores.containsKey(compDataDTO.getCompStrNo())) {
										CompetitiveDataDTO compData = alaskaStores.get(compDataDTO.getCompStrNo());
								
										compDataDTO.setShortName(PropertyManager.getProperty("ALASKA_HAWAII"));
										compDataDTO.setCompChainName(compData.getCompChainName());
										compDataDTO.setCompChainId(compData.getCompChainId() +"-" + compDataDTO.getShortName());
										compDataDTO.setAddressLine1(compData.getAddressLine1());
										compDataDTO.setZip(compData.getZip());
										compDataDTO.setCity(compData.getCity());
										compDataDTO.setState(compData.getState());
									} else if (purtoRicoStores.containsKey(compDataDTO.getCompStrNo())) {
										CompetitiveDataDTO compData = purtoRicoStores.get(compDataDTO.getCompStrNo());
										compDataDTO.setCompChainName(compData.getCompChainName());
										compDataDTO.setShortName(PropertyManager.getProperty("PURTO_RICO"));
										compDataDTO.setCompChainId(compData.getCompChainId() +"-" + compDataDTO.getShortName());
										compDataDTO.setAddressLine1(compData.getAddressLine1());
										compDataDTO.setZip(compData.getZip());
										compDataDTO.setCity(compData.getCity());
										compDataDTO.setState(compData.getState());
									}
								}else
									if (storeMap.containsKey(compDataDTO.getCompStrNo())) {
									CompetitiveDataDTO compData = storeMap.get(compDataDTO.getCompStrNo());
									compDataDTO.setCompChainId(compData.getCompChainId());
									compDataDTO.setCompChainName(compData.getCompChainName());
									compDataDTO.setShortName(compData.getShortName());
									compDataDTO.setAddressLine1(compData.getAddressLine1());
									compDataDTO.setZip(compData.getZip());
									compDataDTO.setCity(compData.getCity());
									compDataDTO.setState(compData.getState());

								} else {
									storeNotFound.add(compDataDTO.getCompStrNo());
									logger.debug("Store not found for: " + compDataDTO.getRetailerItemCode());
								}

								List<CompetitiveDataDTO> temp = new ArrayList<>();
								if (compDataDTO.getCompChainId() != null) {
									if (dataMap.containsKey(compDataDTO.getCompChainId())) {
										temp.addAll(dataMap.get(compDataDTO.getCompChainId()));
									}
									temp.add(compDataDTO);
									dataMap.put(compDataDTO.getCompChainId(), temp);
								} else {
									logger.debug("IgnoredRecord since compChainId not found for " + line[0]);
								}

							}

							else {
								regPriceNotFound.add(line[2]);
							}

						}

					}
				}

				catch (Exception ex) {
					logger.error("Ignored record" + line[0] + ";" + line[2] + ";" + line[1]);
					logger.error("Error is :" + ex);
					continue;
				}
			}

			counter++;
		}

		logger.info("#records in dataMap " + dataMap.size());

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

	public void WriteCategoriesToCsvFile(String fName) throws IOException 
	{
		
		logger.info("WriteCategoriesToCsvFile:Writing started");
		String fileType = PropertyManager.getProperty("FILE_TYPE", ".csv");
		String csvOutputPath = rootPath + "/" + output + "/" + "MCP" + "_" + fName + fileType;

		fw = new FileWriter(csvOutputPath);
		pw = new PrintWriter(fw);
		String separtor = "|";
		Header(separtor);

		for (CompetitiveDataDTO item : mcpList) {

			pw.print(item.getCompStrNo());
			pw.print(separtor);
			pw.print(item.getCheckDate());
			pw.print(separtor);
			pw.print(item.getRetailerItemCode());
			pw.print(separtor);
			pw.print(item.getItemName());
			pw.print(separtor);
			pw.print(item.getRegularPack());
			pw.print(separtor);
			pw.print(item.getRegularPrice());
			pw.print(separtor);
			pw.print(item.getSize());
			pw.print(separtor);
			pw.print(item.getUpc());
			pw.print(separtor);
			pw.print(item.getOutSideRangeInd());
			pw.print(separtor);
			pw.print(item.getComment());
			pw.print(separtor);
			pw.print(item.getSaleMPack());
			pw.print(separtor);
			pw.print(item.getSalePrice());
			pw.print(separtor);
			pw.print(item.getSaleDate());
			pw.print(separtor);
			pw.print(item.getPartNumber());
			pw.println();

		}

		pw.flush();
		fw.flush();
		logger.info("WriteCategoriesToCsvFile:Writing Complete");
	}

	private void Header(String separtor) {

		pw.print("competitor_store");
		pw.print(separtor);
		pw.print("price_check_date");
		pw.print(separtor);
		pw.print("item_code");
		pw.print(separtor);
		pw.print("item_description");
		pw.print(separtor);
		pw.print("regular_quantity");
		pw.print(separtor);
		pw.print("regular_retail");
		pw.print(separtor);
		pw.print("item_size");
		pw.print(separtor);
		pw.print("upc");
		pw.print(separtor);
		pw.print("outside_indicator");
		pw.print(separtor);
		pw.print("additional_info");
		pw.print(separtor);
		pw.print("sale_quantity");
		pw.print(separtor);
		pw.print("sale_retail");
		pw.print(separtor);
		pw.print("sale_date");
		pw.print(separtor);
		pw.print("part_number");
		pw.println();

	}
	

}
