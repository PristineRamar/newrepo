package com.pristine.service;

import java.util.List;

import org.apache.log4j.Logger;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dataload.service.DataInputForAPI;
import com.pristine.dto.Ad;
import com.pristine.dto.Cost;
import com.pristine.dto.Display;
import com.pristine.dto.Price;
import com.pristine.dto.Promotion;
import com.pristine.service.offermgmt.data.WebServiceApacheHttpClient;
import com.pristine.service.offermgmt.data.WebServiceClient;
import com.pristine.util.PropertyManager;

public class RestAPIConnectionService {

	static Logger logger = Logger.getLogger("RestAPIConnection");

	/**
	 * 
	 * @param costInput
	 * @return
	 * @throws Exception
	 */

	@SuppressWarnings("unchecked")
	public List<Cost> getCostFromAPI(DataInputForAPI costInput) throws Exception {
		List<Cost> costList = null;
		ObjectMapper mapper = new ObjectMapper();
		String input = mapper.writeValueAsString(costInput);
		String costUrl = PropertyManager.getProperty("COST_URL");
		logger.info(" getCostFromAPI()- Requesting cost URL is:  " + costUrl);
		logger.info(" getCostFromAPI()- input for CostAPI:  " + input);

		WebServiceClient webServiceClient = new WebServiceApacheHttpClient();
		String response = webServiceClient.postRequest(costUrl, input);
		if (response != null) {
			mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
			mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			costList = (List<Cost>) mapper.readValue(response, new TypeReference<List<Cost>>() {
			});
		}
		return costList;
	}
	

	/**
	 * 
	 * @param priceInput
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public List<Price> getPriceFromAPI(DataInputForAPI priceInput) throws Exception {
		List<Price> priceList = null;
		ObjectMapper mapper = new ObjectMapper();
		String input = mapper.writeValueAsString(priceInput);

		String priceUrl = PropertyManager.getProperty("PRICE_URL");
		logger.info(" getPriceFromAPI()-  Requesting price URL is:  " + priceUrl);
		logger.info(" getPriceFromAPI()-  input for PriceAPI:  " + input);
		WebServiceClient webServiceClient = new WebServiceApacheHttpClient();
		String response = webServiceClient.postRequest(priceUrl, input);
		if (response != null) {
			mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
			mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			priceList = (List<Price>) mapper.readValue(response, new TypeReference<List<Price>>() {
			});
		}
		return priceList;
	}
	
	/**
	 * 
	 * @param promoInput
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public List<Promotion> getPromotionFromAPI(DataInputForAPI promoInput) throws Exception {
		List<Promotion> promotionList = null;
		if (promoInput.isMovingItemsPresent()) {
			ObjectMapper mapper = new ObjectMapper();
			String input = mapper.writeValueAsString(promoInput);

			String promoURL = PropertyManager.getProperty("PROMOTION_URL");
			logger.info(" getPromotionFromAPI()-  Requesting promotion URL is:  " + promoURL);
			logger.info(" getPromotionFromAPI()-  input for promotionAPI:  " + input);
			WebServiceClient webServiceClient = new WebServiceApacheHttpClient();
			String response = webServiceClient.postRequest(promoURL, input);
			if (response != null) {
				mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
				mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
				mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
				promotionList = (List<Promotion>) mapper.readValue(response, new TypeReference<List<Promotion>>() {
				});
			}
		} else {
			logger.warn("getPromotionFromAPI()- No moving items Present for fetching promotions");
		}
		return promotionList;
	}
	
	
	@SuppressWarnings("unchecked")
	public List<Ad> getADDataFromAPI(DataInputForAPI adInput) throws Exception {
		List<Ad> AdList = null;
		ObjectMapper mapper = new ObjectMapper();
		String input = mapper.writeValueAsString(adInput);

		String adURL = PropertyManager.getProperty("AD_URL");
		logger.info(" getADDataFromAPI()-  Requesting AD URL is:  " + adURL);
		logger.info(" getADDataFromAPI()-  input for AD data:  " + input);
		WebServiceClient webServiceClient = new WebServiceApacheHttpClient();
		String response = webServiceClient.postRequest(adURL, input);
		if (response != null) {
			mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
			mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			AdList = (List<Ad>) mapper.readValue(response, new TypeReference<List<Ad>>() {
			});
		}
		
		return AdList;
	}
	
	@SuppressWarnings("unchecked")
	public List<Display> getDisplayDataFromAPI(DataInputForAPI displayInput) throws Exception {
		List<Display> displayList = null;
		ObjectMapper mapper = new ObjectMapper();
		String input = mapper.writeValueAsString(displayInput);

		String displayURL = PropertyManager.getProperty("DISPLAY_URL");
		logger.info(" getDisplayDataFromAPI()-  Requesting DISPLAY_URL is:  " + displayURL);
		logger.info(" getDisplayDataFromAPI()-  input for DISPLAY data:  " + input);
		WebServiceClient webServiceClient = new WebServiceApacheHttpClient();
		String response = webServiceClient.postRequest(displayURL, input);
		if (response != null) {
			mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
			mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			displayList = (List<Display>) mapper.readValue(response, new TypeReference<List<Display>>() {
			});
		}
		return displayList;
	}

	
}
