package com.pristine.feedvalidator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleCostDTO;
import com.pristine.exception.GeneralException;
import com.pristine.feedvalidator.lookup.PropertyNameLookup;
import com.pristine.feedvalidator.lookup.ValidationTypesLookup;
import com.pristine.feedvalidator.service.FeedValidatorService;
import com.pristine.util.PropertyManager;

public class CostFileValidator<T> extends CoreFeedValidator{
	private static Logger logger = Logger.getLogger("GECostFileValidator");
	FileValidator fileValidator = null;
	HashMap<String, GiantEagleCostDTO> errorMsgAndItems = new HashMap<>();
	String stringType = "STRING";
	String doubleType = "DOUBLE";
	String dateType = "DATE";
	String numberType = "NUMBER";
	String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
	private FileWriter fw = null;
	private PrintWriter pw = null;
	HashMap<String, List<ItemDTO>> upcMap = new HashMap<>();
	List<GiantEagleCostDTO> errorRecords =null;
	List<GiantEagleCostDTO> validRecords = null;
	HashMap<String, RetailCalendarDTO> calendarDetails = new HashMap<>();
	@SuppressWarnings("rawtypes")
	private void initializeCache() throws Exception {
		errorRecords = new ArrayList<>();
		validRecords = new ArrayList<>();
		fileValidator = new FileValidator();
		FileValidator.upcMap = upcMap;
		FileValidator.calendarDetails = calendarDetails;
		
		if(FileValidator.upcMap.size()==0 || FileValidator.calendarDetails.size()==0){
			logger.error("Required Maps haven't initialized properly. Check upcMap and calendarDetails.");
			throw new Exception("Required Maps details haven't assigned properly. Check Maps like upcMap and calendarDetails maps..");
		}
	}
	
	public CostFileValidator(HashMap<String, List<ItemDTO>> upcMap, HashMap<String, RetailCalendarDTO> calendarDetails){
		this.upcMap = upcMap;
		this.calendarDetails = calendarDetails;
	}
	
	public CostFileValidator() {
		super();
	}
	
	/**
	 * To get Error records
	 * @param costList
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public List getErrorRecordsAlone(List costList) throws Exception{
		validateFeed(costList);
		return (errorRecords.size()>0) ? errorRecords: null;
	}
	
	/**
	 * To get valid cost records
	 * @param costList
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public List getNonErrorRecords(List costList) throws Exception{
		validateFeed(costList);
		return (validRecords.size()>0) ? validRecords: null;
	}
	
	private ValidatorConfiguration setValidatorConfiguration(String propertyName, String inputType, String isMandatory, String validatePrestoData,
			String expectedValues, String weekType){
		ValidatorConfiguration validatorConfiguration = new ValidatorConfiguration();
		validatorConfiguration.setPropertyName(PropertyNameLookup.get(propertyName));
		List<ValidationTypesLookup> validationList = new ArrayList<>();
		if(inputType!=null){
			setValidationTypes(validationList, inputType);
		}
		if(isMandatory!=null){
			setValidationTypes(validationList, isMandatory);
		}
		if(validatePrestoData!=null){
			setValidationTypes(validationList, validatePrestoData);
		}
		if(expectedValues!=null){
			setValidationTypes(validationList, expectedValues);
		}
		if(weekType!=null){
			setValidationTypes(validationList, weekType);
		}
		validatorConfiguration.setValidationList(validationList);
		return validatorConfiguration;
	}
	
	private void setValidationTypes(List<ValidationTypesLookup> validationList, String inputType){
		validationList.add(ValidationTypesLookup.get(inputType));
	}
	
	

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void validateFeed(List inputList) throws Exception {
		List<GiantEagleCostDTO> costList = new ArrayList<GiantEagleCostDTO>();
		costList.addAll((List<GiantEagleCostDTO>) inputList);
		initializeCache();
		int recordCount = 0;
		for(GiantEagleCostDTO giantEagleCostDTO: costList){
			recordCount++;
			List<ValidatorConfiguration> costConfigList = new ArrayList<>();
			costConfigList.add(setValidatorConfiguration("UPC", stringType, "MANDATORY", "VALIDATE_PRESTO_DATA", null, "WEEK_CALENDAR"));
			costConfigList.add(setValidatorConfiguration("WHITEM_NO", stringType, null, null, null, "WEEK_CALENDAR"));
			costConfigList.add(setValidatorConfiguration("STRT_DTE", dateType, "MANDATORY", "VALIDATE_PRESTO_DATA", null, "WEEK_CALENDAR"));
			costConfigList.add(setValidatorConfiguration("SPLR_NO", numberType, null, null, null, "WEEK_CALENDAR"));
			costConfigList.add(setValidatorConfiguration("CST_STAT_CD", stringType, "MANDATORY", null, "COST_STATUS_CODE", "WEEK_CALENDAR"));
			
			boolean storeCstMandatory = (giantEagleCostDTO.getSPLR_NO() != null && !giantEagleCostDTO.getSPLR_NO().isEmpty()) ? true : false;
			costConfigList.add(setValidatorConfiguration("BS_CST_AKA_STORE_CST", doubleType, (storeCstMandatory)?"MANDATORY":null, null, 
					null, "WEEK_CALENDAR"));
			
			boolean whseCstMandatory = (giantEagleCostDTO.getSPLR_NO() != null && !giantEagleCostDTO.getSPLR_NO().isEmpty()
					&& giantEagleCostDTO.getBS_CST_AKA_STORE_CST() > 0) ? false : true;
			costConfigList.add(setValidatorConfiguration("DLVD_CST_AKA_WHSE_CST", doubleType, (whseCstMandatory)?"MANDATORY":null, null, 
					null, "WEEK_CALENDAR"));
			
			costConfigList.add(setValidatorConfiguration("LONG_TERM_REFLECT_FG", stringType, "MANDATORY", null, 
					"YES_OR_NO_FLAG", "WEEK_CALENDAR"));
			
			costConfigList.add(setValidatorConfiguration("BNR_CD", stringType, "MANDATORY", null, "BNR_CODE", "WEEK_CALENDAR"));
			
			String errorMessage = fileValidator.validateRow(costConfigList, giantEagleCostDTO);
			
			if(errorMessage!= null && !errorMessage.trim().isEmpty()){
				giantEagleCostDTO.setErrorMessage(errorMessage.replace(",", ""));
				errorRecords.add(giantEagleCostDTO);
			}else{
				validRecords.add(giantEagleCostDTO);
			}
		}
		
		logger.info("Total number of records validated in Cost file validator: "+recordCount);
	}
	
	public boolean isWHSEItemHasOnlyOneZoneAtBnrLevel(List<GiantEagleCostDTO> inputList) throws GeneralException{
		
		HashMap<String, Set<String>> zoneNumBasedOnUPCAndBnrCd = new HashMap<>();
		boolean itemHasOnlyOneZone = true;
		
		inputList.stream()
			.filter(s->s.getSPLR_NO()==null || s.getSPLR_NO().trim().isEmpty())
			.forEach(costRec->{
				String key = costRec.getUPC()+"-"+costRec.getBNR_CD();
				Set<String> zoneNumSet = new HashSet<>();
				if(zoneNumBasedOnUPCAndBnrCd.containsKey(key)){
					zoneNumSet = zoneNumBasedOnUPCAndBnrCd.get(key);
				}
				zoneNumSet.add(costRec.getCST_ZONE_NO());
				zoneNumBasedOnUPCAndBnrCd.put(key, zoneNumSet);
			});
		
		
		// Loop each key set and check any one of the item has more than one cost zone number
		for(Map.Entry<String, Set<String>> entry: zoneNumBasedOnUPCAndBnrCd.entrySet()){
			
			Set<String> value = entry.getValue();
			if(value!=null && value.size()>1){
				logger.error("Item has more than one cost zone in the given feed. Please check item(UPC-Banner code combination): "+entry.getKey()+
						" Cost zone numbers: "+value.stream().collect(Collectors.joining(",")));
				itemHasOnlyOneZone = false;
			}
		}
		return itemHasOnlyOneZone;
	}
}
