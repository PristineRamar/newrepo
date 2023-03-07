package com.pristine.webscrape;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONObject;

import com.pristine.parsinginterface.PristineFileParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dto.KrogerStoreDetailsDTO;
import com.pristine.dto.KrogerStoreDetailsDTO.StoreInformation;
import com.pristine.dto.KrogerStoreDetailsDTO.StoreInformation.KrogerAddress;
import com.pristine.dto.KrogerStoreDetailsDTO.StoreInformation.LatLong;
import com.pristine.dto.SaveALotStoreDTO;
import com.pristine.dto.SaveALotStoreDTO.Fields;
import com.pristine.dto.WalGreensStoreDetailDTO;
import com.pristine.dto.WalGreensStoreDetailDTO.Store;
import com.pristine.dto.WalGreensStoreDetailDTO.Store.Phone;
import com.pristine.dto.WalGreensStoreDetailDTO.Store.WalGreensAddress;
import com.pristine.dto.WalmartStoreAddressFinderDTO;
import com.pristine.dto.WalmartStoreAddressFinderDTO.Address;
import com.pristine.dto.WalmartStoreAddressFinderDTO.GeoPoint;
import com.pristine.dto.WalmartStoreAddressFinderDTO.StoreType;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

public class StoreAddressFinder extends PristineFileParser {
	private static Logger logger = Logger.getLogger("StoreAddressFinder");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	private final static String FORMAT_MODE = "FORMAT";
	private final static String MODE = "MODE=";
	private final static String WALMART_STORE = "WALMART";
	private final static String KROGER_STORE = "KROGER";
	private final static String SAVE_A_LOT = "SAVEALOT";
	private final static String WAL_GREENS = "WALGREENS";
	private String mode = "WALMART";
	private int recordCount = 0;
	private int skippedCount = 0;
	String rootPath = null;
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private static String relativeInputPath, relativeOutputPath = FORMAT_MODE;

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-Store_address_finder.properties");
		PropertyManager.initialize("analysis.properties");
		StoreAddressFinder storeAddressFinder = new StoreAddressFinder();
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_FOLDER)) {
				relativeOutputPath = arg.substring(OUTPUT_FOLDER.length());
			}
			if (arg.startsWith(MODE)) {
				storeAddressFinder.mode = arg.substring(MODE.length());
			}
		}
		try {
			storeAddressFinder.processFile();
		} catch (GeneralException e) {
			e.printStackTrace();
		}
	}

	private String readFile(String fileName) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				line = br.readLine();
			}
			return sb.toString();
		} finally {
			br.close();
		}
	}

	private void processFile() throws GeneralException {
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		try {
			parseFile();
		} catch (IOException | GeneralException e) {
			logger.error("processFile()", e);
			throw new GeneralException("Error -- processFile()", e);
		}
		logger.info("Formatting Item file Completed... ");
	}

	/**
	 * Get the Path of Input file.
	 * 
	 * @param fieldNames
	 * @throws GeneralException
	 * @throws IOException
	 */
	private void parseFile() throws GeneralException, IOException {

		try {
			// getzip files
			ArrayList<String> zipFileList = getZipFiles(relativeInputPath);
			// Start with -1 so that if any regular files are present, they are
			// processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;

			String zipFilePath = getRootPath() + "/" + relativeInputPath;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;

				try {
					if (processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);

					fileList = getFiles(relativeInputPath);

					for (int i = 0; i < fileList.size(); i++) {

						long fileProcessStartTime = System.currentTimeMillis();

						recordCount = 0;
						skippedCount = 0;

						String fileName = fileList.get(i);
						logger.info("File Name - " + fileName);
						String outputFileName[] = fileName.split("/");
						logger.info("Output File name - " + outputFileName[outputFileName.length - 1]);

						String outputPath = rootPath + "/" + relativeOutputPath + "/"
								+ outputFileName[outputFileName.length - 1];

						File file = new File(outputPath);
						if (!file.exists())
							fw = new FileWriter(outputPath);
						else
							fw = new FileWriter(outputPath, true);

						pw = new PrintWriter(fw);
						String jSonString = readFile(fileName);
						if(mode.equals(WALMART_STORE)){
							List<WalmartStoreAddressFinderDTO> walmertStoreDetailsList = walmartJSonDeserialization(jSonString);
							writeFileForWalmart(walmertStoreDetailsList);
						}
						else if(mode.equals(KROGER_STORE)){
							List<KrogerStoreDetailsDTO> krogerStoreDetailsList = krogerJSonDeserialization(jSonString);
							writeFileForKroger(krogerStoreDetailsList);
						}
						else if(mode.equals(SAVE_A_LOT)){
							List<SaveALotStoreDTO> saveALotStoreDetailsList = saveALotJSonDeserialization(jSonString);
							writeFileForSaveALot(saveALotStoreDetailsList);
						}
						else if(mode.equals(WAL_GREENS)){
							List<WalGreensStoreDetailDTO> walGreensStoreDetailsList = walGreensJSonDeserialization(jSonString);
							writeFileForWalGreens(walGreensStoreDetailsList);
						}
						pw.flush();
						fw.flush();
						pw.close();
						fw.close();

						long fileProcessEndTime = System.currentTimeMillis();

						logger.info("Time taken to process the file - " + (fileProcessEndTime - fileProcessStartTime)
								+ "ms");
					}
				} catch (GeneralException | Exception ex) {
					logger.error("parseFile()", ex);
					throw new GeneralException("Error -- parseFile()", ex);
				} finally {

				}

				if (processZipFile) {
					PrestoUtil.deleteFiles(fileList);
					fileList.clear();
					fileList.add(zipFileList.get(curZipFileCount));
				}
				String archivePath = getRootPath() + "/" + relativeInputPath + "/";

				if (commit) {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
				} else {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				}
				curZipFileCount++;
				processZipFile = true;
			} while (curZipFileCount < zipFileList.size());

		} catch (GeneralException ex) {
			logger.error("parseFile()", ex);
			throw new GeneralException("Error -- parseFile()", ex);
		} catch (Exception ex) {
			logger.error("parseFile()", ex);
			throw new GeneralException("Exception -- parseFile()", ex);
		}
	}

	/**
	 * To process Walmart Store details
	 * @param jSonString
	 * @return
	 * @throws GeneralException
	 */
	public List<WalmartStoreAddressFinderDTO> walmartJSonDeserialization(String jSonString) throws GeneralException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		List<WalmartStoreAddressFinderDTO> storeDetailsList = new ArrayList<WalmartStoreAddressFinderDTO>();
		try {
			List<WalmartStoreAddressFinderDTO> jSonValues = mapper.readValue(jSonString,
					new TypeReference<List<WalmartStoreAddressFinderDTO>>(){});
			for (WalmartStoreAddressFinderDTO storeAddressFinderDTO : jSonValues) {
				WalmartStoreAddressFinderDTO storeList = new WalmartStoreAddressFinderDTO();
				// get id
				storeList.setId(storeAddressFinderDTO.id);
				// Get name from Store type inner class
				StoreType storeType = storeAddressFinderDTO.storeType;
				storeList.setName(storeType.name);
				// Get Address from inner class
				Address address = storeAddressFinderDTO.address;
				storeList.setAddress1(address.address1);
				storeList.setAddress2(address.city);
				storeList.setState(address.state);
				storeList.setPostCode(address.postalCode);
				// Get Phone number
				storeList.setPhone(storeAddressFinderDTO.phone);
				GeoPoint geoPoint = storeAddressFinderDTO.geoPoint;
				storeList.setLatitude(geoPoint.latitude);
				storeList.setLongitude(geoPoint.longitude);
				storeList.setOpenDate(storeAddressFinderDTO.openDate);
				storeDetailsList.add(storeList);
			}

		} catch (Exception exception) {
			exception.printStackTrace();
			logger.error("Exception in jsonParser()" + exception.toString(), exception);
			throw new GeneralException("Error -- jsonParser()", exception);
		}
		return storeDetailsList;
	}

	private void writeFileForWalmart(List<WalmartStoreAddressFinderDTO> storeDetailsList) {

		pw.print("ID, NAME, ADDRESS1, ADDRESS2, STATE, POSTCODE, PHONE, LATITUDE, LONGITUDE, OPENDATE");
		pw.println();
		for (WalmartStoreAddressFinderDTO storeDetails : storeDetailsList) {
			recordCount++;
			pw.print(storeDetails.getId()); // Id
			pw.print(",");
			pw.print(storeDetails.getName()); // NAME
			pw.print(",");
			pw.print(storeDetails.getAddress1()); // ADDRESS1
			pw.print(",");
			pw.print(storeDetails.getAddress2()); // ADDRESS2
			pw.print(",");
			pw.print(storeDetails.getState()); // STATE
			pw.print(",");
			pw.print(storeDetails.getPostCode()); // POSTCODE
			pw.print(",");
			pw.print(storeDetails.getPhone()); // PHONE
			pw.print(",");
			pw.print(storeDetails.getLatitude()); // LATITUDE
			pw.print(",");
			pw.print(storeDetails.getLongitude()); // LONGITUDE
			pw.print(",");
			pw.print(storeDetails.getOpenDate()); // OPENDATE
			pw.println(" "); // spaces
		}
	}
	
	/**
	 * To process Kroger Store details
	 * @param jSonString
	 * @return
	 * @throws GeneralException
	 */
	public List<KrogerStoreDetailsDTO> krogerJSonDeserialization(String jSonString) throws GeneralException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		List<KrogerStoreDetailsDTO> storeDetailsList = new ArrayList<KrogerStoreDetailsDTO>();
		try {
			List<KrogerStoreDetailsDTO> jSonValues = mapper.readValue(jSonString,
					new TypeReference<List<KrogerStoreDetailsDTO>>() {
					});
			for (KrogerStoreDetailsDTO krogerStoreDetailsDTO : jSonValues) {
				KrogerStoreDetailsDTO storeList = new KrogerStoreDetailsDTO();
				// get id
				StoreInformation storeInfo = krogerStoreDetailsDTO.storeInformation;
				storeList.setId(storeInfo.storeNumber);
				// Get name
				storeList.setName(storeInfo.legalName);
				//Get Store type
				storeList.setStoreType(storeInfo.storeType);
				// Get Brand
				storeList.setBrand(storeInfo.brand);
				// Get Phone number
				storeList.setPhone(storeInfo.phoneNumber);
				// Get Address from inner class
				KrogerAddress address = storeInfo.address;
				storeList.setAddress1(address.addressLineOne);
				storeList.setAddress2(address.city);
				storeList.setState(address.state);
				storeList.setPostCode(address.zipCode);

				LatLong latLong = storeInfo.latLong;
				storeList.setLatitude(latLong.latitude);
				storeList.setLongitude(latLong.longitude);
				storeDetailsList.add(storeList);
			}

		} catch (Exception exception) {
			exception.printStackTrace();
			logger.error("Exception in jsonParser()" + exception.toString(), exception);
			throw new GeneralException("Error -- jsonParser()", exception);
		}
		return storeDetailsList;
	}

	private void writeFileForKroger(List<KrogerStoreDetailsDTO> storeDetailsList) {

		pw.print("ID, NAME,STORE_TYPE, BRAND, ADDRESS1, ADDRESS2, STATE, POSTCODE, PHONE, LATITUDE, LONGITUDE, OPENDATE");
		pw.println();
		for (KrogerStoreDetailsDTO storeDetails : storeDetailsList) {
			recordCount++;
			pw.print(storeDetails.getId()); // Id
			pw.print(",");
			pw.print(storeDetails.getName()); // NAME
			pw.print(",");
			pw.print(storeDetails.getStoreType()); // STORE_TYPE
			pw.print(",");
			pw.print(storeDetails.getBrand()); // BRAND
			pw.print(",");
			pw.print(storeDetails.getAddress1()); // ADDRESS1
			pw.print(",");
			pw.print(storeDetails.getAddress2()); // ADDRESS2
			pw.print(",");
			pw.print(storeDetails.getState()); // STATE
			pw.print(",");
			pw.print(storeDetails.getPostCode()); // POSTCODE
			pw.print(",");
			pw.print(storeDetails.getPhone()); // PHONE
			pw.print(",");
			pw.print(storeDetails.getLatitude()); // LATITUDE
			pw.print(",");
			pw.print(storeDetails.getLongitude()); // LONGITUDE
			pw.print(",");
			pw.print(storeDetails.getOpenDate()); // OPENDATE
			pw.println(" "); // spaces
		}
	}
	
	public List<SaveALotStoreDTO> saveALotJSonDeserialization(String jSonString) throws GeneralException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		List<SaveALotStoreDTO> storeDetailsList = new ArrayList<SaveALotStoreDTO>();
		try {
			List<SaveALotStoreDTO> jSonValues = mapper.readValue(jSonString,
					new TypeReference<List<SaveALotStoreDTO>>() {
					});
			for (SaveALotStoreDTO saveALotStoreDetailsDTO : jSonValues) {
				Fields storeDetails = saveALotStoreDetailsDTO.Fields;
				for(String locationId: storeDetails.LocationId){
					storeDetails.setLocationId(locationId);
				}
				for(String storeNumber: storeDetails.StoreNumber){
					storeDetails.setStoreNumber(storeNumber);
				}
				for(String city: storeDetails.City){
					storeDetails.setCity(city);
				}
				for(String zip: storeDetails.Zip){
					storeDetails.setZip(zip);
				}
				for(String state: storeDetails.State){
					storeDetails.setState(state);
				}
				for(String latlng: storeDetails.Latlng){
					storeDetails.setLatlng(latlng);
				}
				for(String phone: storeDetails.Phone){
					storeDetails.setPhone(phone);
				}
				for(String locationName: storeDetails.LocationName){
					storeDetails.setLocationName(locationName);
				}
				for(String address: storeDetails.Address1){
					storeDetails.setAddress1(address);
				}
				
				
				//To Split latitude and Longitude values from latlng...
				String[] geoPoint = storeDetails.getLatlng().split(",");
				saveALotStoreDetailsDTO.setLatitude(geoPoint[0].trim());
				saveALotStoreDetailsDTO.setLongitude(geoPoint[1].trim());
				storeDetailsList.add(saveALotStoreDetailsDTO);
			}

		} catch (Exception exception) {
			exception.printStackTrace();
			logger.error("Exception in jsonParser()" + exception.toString(), exception);
			throw new GeneralException("Error -- jsonParser()", exception);
		}
		return storeDetailsList;
	}

	private void writeFileForSaveALot(List<SaveALotStoreDTO> storeDetailsList) {

		pw.print("ID, STORE_NUMBER,NAME, ADDRESS1, ADDRESS2, STATE, POSTCODE, PHONE, LATITUDE, LONGITUDE, OPENDATE");
		pw.println();
		for (SaveALotStoreDTO saveALotStoreDTO : storeDetailsList) {
			Fields storeDetails = saveALotStoreDTO.Fields;
			pw.print(storeDetails.getLocationId()); // Id
			pw.print(",");
			pw.print(storeDetails.getStoreNumber()); // STORE_NUMBER
			pw.print(",");
			pw.print(storeDetails.getLocationName()); // NAME
			pw.print(",");
			pw.print(storeDetails.getAddress1()); // ADDRESS1
			pw.print(",");
			pw.print(storeDetails.getCity()); // ADDRESS2
			pw.print(",");
			pw.print(storeDetails.getState()); // STATE
			pw.print(",");
			pw.print(storeDetails.getZip()); // POSTCODE
			pw.print(",");
			pw.print(storeDetails.getPhone()); // PHONE
			pw.print(",");
			pw.print(saveALotStoreDTO.getLatitude()); // LATITUDE
			pw.print(",");
			pw.print(saveALotStoreDTO.getLongitude()); // LONGITUDE
			pw.print(",");
			pw.print(saveALotStoreDTO.getOpenDate()); // OPENDATE
			pw.println(" "); // spaces
		}
	}
	
	public List<WalGreensStoreDetailDTO> walGreensJSonDeserialization(String jSonString) throws GeneralException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		List<WalGreensStoreDetailDTO> storeDetailsList = new ArrayList<WalGreensStoreDetailDTO>();
		try {
			List<WalGreensStoreDetailDTO> jSonValues = mapper.readValue(jSonString,
					new TypeReference<List<WalGreensStoreDetailDTO>>() {});
			
//			JSONObject json = new JSONObject(jSonString);
//			JSONArray phoneDetail = json.getJSONArray("phone");
//			for(int i =0; i <phoneDetail.length();i++){
//				JSONObject phoneNumber = phoneDetail.getJSONObject(i);
//				logger.info("Phone number:"+phoneNumber);
//			}
			for (WalGreensStoreDetailDTO walGreensStoreDetailDTO : jSonValues) {
				storeDetailsList.add(walGreensStoreDetailDTO);
			}

		} catch (Exception exception) {
			exception.printStackTrace();
			logger.error("Exception in jsonParser()" + exception.toString(), exception);
			throw new GeneralException("Error -- jsonParser()", exception);
		}
		return storeDetailsList;
	}
	
	private void writeFileForWalGreens(List<WalGreensStoreDetailDTO> storeDetailsList) {

		pw.print("ID, STORE_NUMBER,NAME, ADDRESS1, ADDRESS2, STATE, POSTCODE, PHONE, LATITUDE, LONGITUDE");
		pw.println();
		for (WalGreensStoreDetailDTO walGreensStoreDetailDTO : storeDetailsList) {
			Store storeDetails = walGreensStoreDetailDTO.store;
			WalGreensAddress address = storeDetails.address;
			Phone phoneDetail = storeDetails.phone;
			pw.print(storeDetails.storeNumber); // Id
			pw.print(",");
			pw.print(storeDetails.storeType); // STORE_TYPE
			pw.print(",");
			pw.print(storeDetails.storeBrand); // NAME
			pw.print(",");
			pw.print(address.street); // ADDRESS1
			pw.print(",");
			pw.print(address.city); // ADDRESS2
			pw.print(",");
			pw.print(address.state); // STATE
			pw.print(",");
			pw.print(address.zip); // POSTCODE
			pw.print(",");
			pw.print(phoneDetail.areaCode+""+phoneDetail.number); // PHONE
			pw.print(",");
			pw.print(walGreensStoreDetailDTO.latitude); // LATITUDE
			pw.print(",");
			pw.print(walGreensStoreDetailDTO.longitude); // LONGITUDE
			pw.println(" "); // spaces
		}
	}
	@Override
	public void processRecords(List listobj) throws GeneralException {
		// TODO Auto-generated method stub

	}
}
