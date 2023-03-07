package com.pristine.feedvalidator;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.feedvalidator.lookup.FeedValidatorErrorTypeLookup;
import com.pristine.feedvalidator.lookup.PropertyNameLookup;
import com.pristine.feedvalidator.lookup.ValidationTypesLookup;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;

public class FileValidator<T> {
	private static Logger logger = Logger.getLogger("FileValidator");
	static HashMap<String, List<ItemDTO>> upcMap = new HashMap<>();
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
	static HashMap<String, RetailCalendarDTO> calendarDetails = new HashMap<>();
	
	public String validateRow(List<ValidatorConfiguration> configList, T inputDTO){
		StringBuilder sb = new StringBuilder();
		HashMap<String, Set<Integer>> columnsAndErrorCodes = new HashMap<>();
		configList.forEach(config->{
			try {
				String columnName  = config.getPropertyName().getPropertyNameValue();
				Field field = inputDTO.getClass().getDeclaredField(columnName);
				String inputValue = String.valueOf(runGetter(field, inputDTO));
				String inputType = findInputTypeValue(config.getValidationList());
				boolean isMandatory = checkObjIsMandatory(config.getValidationList());
				boolean validatePrestoData = validateWithPrestoData(config.getValidationList());
				String expectedValues = getExpectedValues(config.getValidationList());
				String weekType = getCalendarWeekType(config.getValidationList());
				
				@SuppressWarnings({ "unchecked", "rawtypes" })
				Set<Integer> errorCodes = new HashSet();
				isValidRecord(inputDTO, columnName, inputValue, inputType, isMandatory, validatePrestoData, 
						expectedValues, weekType, sb, errorCodes);
				columnsAndErrorCodes.put(columnName, errorCodes);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Error in validateObject()",e);
			}
		});
		return sb.toString();
	}
	
	
	private String findInputTypeValue(List<ValidationTypesLookup> validationTypes){
		String inputType = null;
		for(ValidationTypesLookup validationType: validationTypes){
			if(validationType.equals(ValidationTypesLookup.stringType)){
				inputType = ValidationTypesLookup.stringType.getValidationType();
			}else if(validationType.equals(ValidationTypesLookup.dateType)){
				inputType = ValidationTypesLookup.dateType.getValidationType();
			}else if(validationType.equals(ValidationTypesLookup.doubleType)){
				inputType = ValidationTypesLookup.doubleType.getValidationType();
			}else if(validationType.equals(ValidationTypesLookup.numberType)){
				inputType = ValidationTypesLookup.numberType.getValidationType();
			}
		}
		return inputType;
	}
	
	
	
	private boolean checkObjIsMandatory(List<ValidationTypesLookup> validationTypes){
		boolean isMandatory = false;
		for(ValidationTypesLookup validationType: validationTypes){
			if(validationType.equals(ValidationTypesLookup.isMandatory)){
				isMandatory = true;
			}
		}
		return isMandatory;
	}
	
	
	private boolean validateWithPrestoData(List<ValidationTypesLookup> validationTypes){
		boolean validateWithPresto = false;
		for(ValidationTypesLookup validationType: validationTypes){
			if(validationType.equals(ValidationTypesLookup.validatePrestoData)){
				validateWithPresto = true;
			}
		}
		return validateWithPresto;
	}
	
	
	private String getCalendarWeekType(List<ValidationTypesLookup> validationTypes){
		String weekType = null;
		for(ValidationTypesLookup validationType: validationTypes){
			if(validationType.equals(ValidationTypesLookup.calWeekType)){
				weekType = Constants.CALENDAR_WEEK;
			}
		}
		return weekType;
	}
	
	private String getExpectedValues(List<ValidationTypesLookup> validationTypes){
		String expectedValue = null;
		for(ValidationTypesLookup validationType: validationTypes){
			if(validationType.equals(ValidationTypesLookup.costStatusCode)){
				expectedValue = "C,F";
			}else if(validationType.equals(ValidationTypesLookup.allowanceStatusCode)){
				expectedValue = "N,C,F";
			}else if(validationType.equals(ValidationTypesLookup.bannerCode)){
				expectedValue = "GE,MI";
			}else if(validationType.equals(ValidationTypesLookup.yesOrNoFlag)){
				expectedValue = "Y,N";
			}
		}
		return expectedValue;
	}
	
	
	public Object runGetter(Field field, T inputValues) {
		for (Method method : inputValues.getClass().getMethods()) {
			if ((method.getName().startsWith("get")) && (method.getName().length() == (field.getName().length() + 3))) {
				if (method.getName().toLowerCase().endsWith(field.getName().toLowerCase())) {
					try {
						return method.invoke(inputValues);
					} catch (IllegalAccessException e) {
						logger.error("Could not determine method: " + method.getName());
					} catch (InvocationTargetException e) {
						logger.error("Could not determine method: " + method.getName());
					}

				}
			}
		}
		return null;
	}
	
	public boolean isValidRecord(T inputDTO, String columnName, String inputValue, String inputType, boolean isMandatory,
			boolean validatePrestoData, String expectedValues,
			String weekType, StringBuilder sb, Set<Integer> errorCodes) {
		boolean isValidRecord = true;

		if (ValidationTypesLookup.stringType.getValidationType().equals(inputType)) {
			isValidRecord = checkStringValue(inputDTO, columnName, inputValue, isMandatory, sb, errorCodes);
		} else if (ValidationTypesLookup.doubleType.getValidationType().equals(inputType)) {
			isValidRecord = checkDoubleValue(inputDTO, columnName, inputValue, isMandatory, sb, errorCodes);
		} else if (ValidationTypesLookup.dateType.getValidationType().equals(inputType)) {
			isValidRecord = checkDateValue(inputDTO, columnName, inputValue, isMandatory, sb, errorCodes);
		} else if (ValidationTypesLookup.numberType.getValidationType().equals(inputType)) {
			isValidRecord = checkNumberValue(inputDTO, columnName, inputValue, isMandatory, sb, errorCodes);
		}

		// To check given input values are matching with Presto
		boolean isPrestoDataMatching = true;
		if ("DATE".equals(inputType)) {
			isPrestoDataMatching = validatePrestoData(inputDTO, columnName, inputValue, validatePrestoData, sb, weekType, "DATE", errorCodes);
		} else {
			isPrestoDataMatching = validatePrestoData(inputDTO, columnName, inputValue, validatePrestoData, sb, weekType, columnName, errorCodes);
		}

		if (isValidRecord) {
			isValidRecord = isPrestoDataMatching;
		}
		// Given input value must match with any one of expected values
		boolean valuesMatching = isExpectedValuesMatching(inputDTO, columnName, inputValue, expectedValues, sb, errorCodes);
		if (isValidRecord) {
			isValidRecord = valuesMatching;
		}
		return isValidRecord;
	}

	/**
	 * To validate String values
	 * 
	 * @param inputDTO
	 * @param columnName
	 * @param inputValues
	 * @param isMandatory
	 * @param errorMsgAndItems
	 * @param sb
	 */
	private boolean checkStringValue(T inputDTO, String columnName, String inputValue, boolean isMandatory,
			StringBuilder sb, Set<Integer> errorCodes) {
		boolean isValidRecord  =true;
		// To check the given value is mandatory and value is empty
		if (isMandatory && (inputValue == null || inputValue.trim().isEmpty())) {
			isValidRecord = false;
			String errMsg = "The Column " + columnName + " is mandatory ,";
			sb.append(errMsg);
			errorCodes.add(FeedValidatorErrorTypeLookup.mandatoryStringErrorCode.getErrorCode());
		}
		return isValidRecord;
	}

	private boolean checkDoubleValue(T inputDTO, String columnName, String inputValue, boolean isMandatory,
			StringBuilder sb, Set<Integer> errorCodes) {
		boolean isValidRecord  =true;
		// To check the given value is mandatory and value is empty
		if (isMandatory && ((inputValue == null || inputValue.trim().isEmpty())
				|| (inputValue != null && !inputValue.trim().isEmpty() && isValueParsableAsDouble(inputValue) && Double.parseDouble(inputValue)== 0))) {
			isValidRecord = false;
			String errMsg = "The Column " + columnName + " is mandatory ,";
			sb.append(errMsg);
			
			errorCodes.add(FeedValidatorErrorTypeLookup.mandatoryDoubleErrorCode.getErrorCode());
		}
		
		if(inputValue != null && !inputValue.trim().isEmpty() && !isValueParsableAsDouble(inputValue)){
			isValidRecord = false;
			String errMsg = "The Column " + columnName + " must have only numberic values ,";
			sb.append(errMsg);
			errorCodes.add(FeedValidatorErrorTypeLookup.doubleParsingErrorCode.getErrorCode());
		}
		return isValidRecord;
	}

	private boolean checkDateValue(T inputDTO, String columnName, String inputValue, boolean isMandatory,
			StringBuilder sb, Set<Integer> errorCodes) {
		boolean isValidRecord  =true;
		// To check the given value is mandatory and value is empty
		if (isMandatory && ((inputValue == null || inputValue.trim().isEmpty()))) {
			isValidRecord = false;
			String errMsg = "The Column " + columnName + " is mandatory ,";
			sb.append(errMsg);
			
			errorCodes.add(FeedValidatorErrorTypeLookup.mandatoryDateErrorCode.getErrorCode());
		}

		if (inputValue != null && !inputValue.trim().isEmpty() && !isDateParsable(inputValue, formatter)) {
			isValidRecord = false;
			String errMsg = "The Column " + columnName + " is having date in unexpected format ,";
			sb.append(errMsg);
			errorCodes.add(FeedValidatorErrorTypeLookup.dateParsingErrorCode.getErrorCode());
		}
		return isValidRecord;
	}

	private boolean checkNumberValue(T inputDTO, String columnName, String inputValue, boolean isMandatory,
			StringBuilder sb, Set<Integer> errorCodes) {
		boolean isValidRecord  =true;
		// To check the given value is mandatory and value is empty
		if (isMandatory && ((inputValue == null || inputValue.trim().isEmpty()))) {
			isValidRecord = false;
			String errMsg = "The Column " + columnName + " is mandatory ,";
			sb.append(errMsg);
			errorCodes.add(FeedValidatorErrorTypeLookup.mandatoryNumberErrorCode.getErrorCode());
		}

		if (inputValue != null && !inputValue.trim().isEmpty()) {
			try {
				Integer.parseInt(inputValue);
			} catch (Exception e) {
				isValidRecord = false;
				String errMsg = "The Column " + columnName + " must have only numberic values ,";
				sb.append(errMsg);
				errorCodes.add(FeedValidatorErrorTypeLookup.numberParsingErrorCode.getErrorCode());
			}
		}
		return isValidRecord;
	}

	private boolean isExpectedValuesMatching(T inputDTO, String columnName, String inputValue, String expectedValues,
			StringBuilder sb, Set<Integer> errorCodes) {
		boolean isValidRecord  =true;
		if (expectedValues != null && !expectedValues.trim().isEmpty()) {

			List<String> expectedList = new ArrayList<>();
			String[] expectedValueArray = expectedValues.split(",");
			for (int i = 0; i < expectedValueArray.length; i++) {
				expectedList.add(expectedValueArray[i].toUpperCase().trim());
			}

			if (!expectedList.contains(inputValue.toUpperCase().trim())) {
				isValidRecord = false;
				String errMsg = "The Column " + columnName + " has unexpected value other than " + expectedValues + ",";
				sb.append(errMsg);
				
				errorCodes.add(FeedValidatorErrorTypeLookup.expectedValuesNotMatchingErrorCode.getErrorCode());
			}
		}
		return isValidRecord;
	}

	/**
	 * To check the data is available in Presto. Based on the column name the appropriate values will be verified.
	 * 
	 * @param inputDTO
	 * @param columnName
	 * @param inputValue
	 * @param validatePrestoData
	 * @param errorMsgAndItems
	 * @param sb
	 */
	private boolean validatePrestoData(T inputDTO, String columnName, String inputValue, boolean validatePrestoData,
			StringBuilder sb, String weekType, String validatorName, Set<Integer> errorCodes) {
		boolean isValidRecord  =true;
		if (validatePrestoData) {
			if(PropertyNameLookup.UPC.getPropertyNameValue().equals(validatorName)){
				if (!upcMap.containsKey(PrestoUtil.castUPC(inputValue, false))) {
					isValidRecord = false;
					String errMsg = "The " + columnName + " value is not available in Presto ,";
					sb.append(errMsg);
					errorCodes.add(FeedValidatorErrorTypeLookup.dataNotInPrestoErrorCode.getErrorCode());
				}
			}
			if ("DATE".equals(validatorName.toUpperCase())) {
				if (!calendarDetails.containsKey(inputValue)) {
					isValidRecord = false;
					String errMsg = "The " + columnName + " value is not available in Presto ,";
					sb.append(errMsg);
					errorCodes.add(FeedValidatorErrorTypeLookup.dataNotInPrestoErrorCode.getErrorCode());
				}
			}
		}
		return isValidRecord;
	}

	public boolean isZoneAvailableInPresto(String bnrCode, String cstZoneNo, String splrNo, HashMap<String, Integer> retailPriceZone, 
			List<ItemDTO> itemList){
		boolean isZoneAvailable = false;
		
		for(ItemDTO itemDTO: itemList){
			
			String zoneNum = bnrCode+"-"+cstZoneNo+"-"+itemDTO.getPrcGrpCd();
			if(splrNo!=null && !splrNo.trim().isEmpty()){
				zoneNum = zoneNum+"-"+splrNo;
			}
			
			if(retailPriceZone.containsKey(zoneNum) && !isZoneAvailable){
				isZoneAvailable = true;
			}
		}
		
		return isZoneAvailable;
	}
	private boolean isDateParsable(String inputDate, DateTimeFormatter formatter) {
		boolean isDateParsable = true;

		try {
			LocalDate.parse(inputDate, formatter);
		} catch (Exception e) {
			isDateParsable = false;
		}

		return isDateParsable;
	}

	public RetailCalendarDTO getWeekStartDateCalDTO(Connection conn, String inputDate, String weekType) {
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calendarDTO = null;
		if (calendarDetails.get(inputDate) != null) {
			calendarDTO = calendarDetails.get(inputDate);
		}
		if (calendarDTO == null) {
			try {
				calendarDTO = retailCalendarDAO.getCalendarId(conn, inputDate, weekType);
				calendarDetails.put(inputDate, calendarDTO);
			} catch (GeneralException e) {
				e.printStackTrace();
			}

		}
		return calendarDTO;
	}
	
	private boolean isValueParsableAsDouble(String inputValue){
		boolean isParsable = true;
		try{
			Double.parseDouble(inputValue);
		} catch (Exception e) {
			isParsable = false;
		}
		return isParsable;
	}
	
}
