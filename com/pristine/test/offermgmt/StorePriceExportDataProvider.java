package com.pristine.test.offermgmt;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.pristine.dto.fileformatter.gianteagle.ZoneDTO;
import com.pristine.dto.offermgmt.PriceExportDTO;

public class StorePriceExportDataProvider {
	
	private <T> T jsonFileToObject(String jsonFilePath, TypeReference<T> typeReference){
		ObjectMapper objectMapper = new ObjectMapper();
		T mappedObject = null;
		try {
			File jsonFile = new File(jsonFilePath);
			mappedObject = objectMapper.readValue(jsonFile, typeReference);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return mappedObject;
	}
	
	/**
	 * This method reads a file containing test data formatted as a JSON 
	 * and converts it to a list of PriceExportDTO objects
	 * @param jsonDataFilePath Specifies the qualified file path to the test data file.
	 * @return List of PriceExportDTO objects.
	 */
	public List<PriceExportDTO> generateExportQueue(String jsonDataFilePath) {
		List<PriceExportDTO> itemsInQueue;
		itemsInQueue = (List<PriceExportDTO>)jsonFileToObject(jsonDataFilePath,new TypeReference<List<PriceExportDTO>>() {});
		return itemsInQueue;
	}
	
	/**
	 * This method reads a file containing test data formatted as a JSON 
	 * and converts it to a list of ZoneDTO objects
	 * @param jsonDataFilePath Specifies the qualified file path to the test data file.
	 * @return List of ZoneDTO objects.
	 */
	public List<ZoneDTO> generateGlobalZoneData(String jsonDataFilePath) {
		List<ZoneDTO> globalZoneData = null;
		globalZoneData = (List<ZoneDTO>)jsonFileToObject(jsonDataFilePath,new TypeReference<List<ZoneDTO>>() {});
		return globalZoneData;
	}
	
	public HashMap<String, List<PriceExportDTO>> generateEmergencyAndClearanceItems(String jsonDataFilePath) {
		HashMap<String, List<PriceExportDTO>> emergencyAndClearanceItemsData = new HashMap<>();
//		TODO Populate emergencyAndClearanceItemsData from a json file.
		return emergencyAndClearanceItemsData;
	}
	
	public HashMap<String, List<Long>> generateZoneTypeToRunIdsMap(String jsonDataFilePath) {
		HashMap<String, List<Long>> zoneTypeToRunIdsMap = new HashMap<>();
		zoneTypeToRunIdsMap = (HashMap<String, List<Long>>)jsonFileToObject(jsonDataFilePath,
				new TypeReference<HashMap<String, List<Long>>>() {});
		return zoneTypeToRunIdsMap;
	}

	/**
	 * Modify as needed before using.
	 * Temporary method used to write test data to JSON files.
	 * Useful when the raw test data needs to be sampled and formatted into persisting static test data
	 * @param objectMapper ObjectMapper instance used to map data objects to JSON strings and vice versa.
	 * @param itemsInQueue Data object that needs to be written out.
	 * @throws IOException
	 * @throws JsonGenerationException
	 * @throws JsonMappingException
	 */
	private void writeTestDataToJSONFile(ObjectMapper objectMapper, 
			List<PriceExportDTO> itemsInQueue) throws IOException, JsonGenerationException, JsonMappingException {
		// group by zone
		HashMap<String, List<PriceExportDTO>> queueItemsByZoneNos = (HashMap<String, List<PriceExportDTO>>) itemsInQueue.stream()
				.collect(Collectors.groupingBy(PriceExportDTO::getPriceZoneNo));
		List<PriceExportDTO> z1000Entries = queueItemsByZoneNos.get("1000");

		
		List<PriceExportDTO> cloneList = new ArrayList<PriceExportDTO>();
		PriceExportDTO tempEntry;
		SimpleDateFormat sdFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		Date z1000ApprovedDate;
		Calendar cal = Calendar.getInstance();

//		Create clones of the first 16 Z1000 entries 
//		Change them to Z4 entries that were approved on the day before the original Z1000 entries were approved on.
//		Add these clones to the tempList
		List<PriceExportDTO> z4EntriesEarlier = z1000Entries.subList(0, 16);
		for(PriceExportDTO entry: z4EntriesEarlier) {
			try {
				tempEntry = (PriceExportDTO)entry.clone();
				tempEntry.setPriceZoneId(2);
				tempEntry.setPriceZoneNo("4");
				tempEntry.setZoneName("ADV-ORLY");
				z1000ApprovedDate = sdFormat.parse(entry.getApprovedOn());
				cal.setTime(z1000ApprovedDate);
				cal.add(Calendar.DATE, -1);
				tempEntry.setApprovedOn(sdFormat.format(cal.getTime()));
				cloneList.add(tempEntry);
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
//		Create clones of the second 16 Z1000 entries 
//		Change them to Z4 entries that were approved on the day after the original Z1000 entries were approved on.
//		Add these clones to the tempList
		List<PriceExportDTO> z4EntriesLatter = z1000Entries.subList(16, 32);
		for(PriceExportDTO entry: z4EntriesLatter) {
			try {
				tempEntry = (PriceExportDTO)entry.clone();
				tempEntry.setPriceZoneId(2);
				tempEntry.setPriceZoneNo("4");
				tempEntry.setZoneName("ADV-ORLY");
				z1000ApprovedDate = sdFormat.parse(entry.getApprovedOn());
				cal.setTime(z1000ApprovedDate);
				cal.add(Calendar.DATE, 1);
				tempEntry.setApprovedOn(sdFormat.format(cal.getTime()));
				cloneList.add(tempEntry);
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

//		Create clones of the fifth 16 Z1000 entries 
//		Change them to Z16 entries that were approved on the day before the original Z1000 entries were approved on.
//		Add these clones to the tempList
		List<PriceExportDTO> z16EntriesEarlier = z1000Entries.subList(64, 80);
		for(PriceExportDTO entry: z16EntriesEarlier) {
			try {
				tempEntry = (PriceExportDTO)entry.clone();
				tempEntry.setPriceZoneId(3);
				tempEntry.setPriceZoneNo("16");
				tempEntry.setZoneName("ADV-ORLY-NAPA");
				z1000ApprovedDate = sdFormat.parse(entry.getApprovedOn());
				cal.setTime(z1000ApprovedDate);
				cal.add(Calendar.DATE, -1);
				tempEntry.setApprovedOn(sdFormat.format(cal.getTime()));
				cloneList.add(tempEntry);
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
//		Create clones of the sixth 16 Z1000 entries 
//		Change them to Z16 entries that were approved on the day after the original Z1000 entries were approved on.
//		Add these clones to the tempList
		List<PriceExportDTO> z16EntriesLatter = z1000Entries.subList(80, 96);
		for(PriceExportDTO entry: z16EntriesLatter) {
			try {
				tempEntry = (PriceExportDTO)entry.clone();
				tempEntry.setPriceZoneId(3);
				tempEntry.setPriceZoneNo("16");
				tempEntry.setZoneName("ADV-ORLY-NAPA");
				z1000ApprovedDate = sdFormat.parse(entry.getApprovedOn());
				cal.setTime(z1000ApprovedDate);
				cal.add(Calendar.DATE, 1);
				tempEntry.setApprovedOn(sdFormat.format(cal.getTime()));
				cloneList.add(tempEntry);
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
//		Add the newly created Z4 and Z16 entries to the itemQueue.
		itemsInQueue.addAll(cloneList);
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		objectMapper.writeValue(new File("com/pristine/test/offermgmt/sampleQueueExtd.json"), itemsInQueue);
	}
	
}
