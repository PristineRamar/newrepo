package com.pristine.service.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.apache.log4j.Logger;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.service.offermgmt.recommendationrule.PastOverridenRetailRule;
import com.pristine.service.offermgmt.recommendationrule.RetainCurrentRetailRule;
import com.pristine.util.PropertyManager;

public class OverrideService {

	private static Logger logger = Logger.getLogger("OverrideService");
	
	/**
	 * 
	 * @param itemList
	 * @throws GeneralException 
	 * @throws Exception 
	 */
	public void setupPastOverrideDetails(List<PRItemDTO> itemList,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) throws Exception, GeneralException {
		for (PRItemDTO itemDTO : itemList) {
			if (itemDTO.getOverriddenRegularPrice() != null) {
				if ((itemDTO.isLir() || (!itemDTO.isLir() && itemDTO.getRetLirId() <= 0))) {
					retainOverrides(itemDTO, itemList);
				}
			}
		}
	}
	
	
	public void retainOverrides(PRItemDTO itemDTO, List<PRItemDTO> itemList) throws Exception, GeneralException {

		MultiplePrice actualRecPrice = new MultiplePrice(itemDTO.getRecommendedRegPrice().multiple,
				itemDTO.getRecommendedRegPrice().price);

		PricingEngineService pricingEngineService = new PricingEngineService();
		List<PRItemDTO> ligMembersOrNonLig = pricingEngineService.getLigMembersOrNonLigItem(itemDTO, itemList);

		// lig members
		for (PRItemDTO ligMemberOrLig : ligMembersOrNonLig) {
			ligMemberOrLig.setRecommendedRegPrice(itemDTO.getOverriddenRegularPrice());
		}

		List<String> additionalDetails = new ArrayList<String>();
		additionalDetails.add(itemDTO.getPastOverrideDate());
		additionalDetails
				.add(actualRecPrice.multiple > 1 ? actualRecPrice.toString() : String.valueOf(actualRecPrice.price));
		new PricingEngineService().writeAdditionalExplainLog(itemDTO, itemList,
				ExplainRetailNoteTypeLookup.USER_OVERRIDE_RETAINED, additionalDetails);
	}
	
	/**
	 * 
	 * @param itemDataMap
	 * @param overrideItemDataMap
	 */
	private void updatePastOverridePrice(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<ItemKey, PRItemDTO> overrideItemDataMap) {
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if(overrideItemDataMap.get(itemKey) != null) {
				logger.debug("updatePastOverridePrice() - Past override found for " + itemKey.toString());
				
				PRItemDTO overrideItemDTO = overrideItemDataMap.get(itemKey); 
				itemDTO.setOverriddenRegularPrice(overrideItemDTO.getOverriddenRegularPrice());
				itemDTO.setPastOverrideDate(overrideItemDTO.getPastOverrideDate());
				logger.debug("updatePastOverridePrice() - Past override price is " + overrideItemDTO.getOverriddenRegularPrice().toString());
			}
		});
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param weekStartDate
	 * @param itemDataMap
	 * @param recommendationRuleMap
	 * @throws GeneralException
	 * @throws NumberFormatException
	 * @throws Exception
	 */
	public void lookupPastOverrides(Connection conn, int locationLevelId, int locationId, int productLevelId,
			int productId, String weekStartDate, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap)
			throws GeneralException, NumberFormatException, Exception {

		Optional<PRItemDTO> firstEle = itemDataMap.values().stream().findFirst();
		if (firstEle.isPresent()) {

			PRItemDTO firstItem = firstEle.get();

			RetainCurrentRetailRule pastOverrideRule = new PastOverridenRetailRule(firstItem, recommendationRuleMap);

			if (pastOverrideRule.isRuleEnabled()) {

				logger.debug("lookupPastOverrides() - Past override rule is enabled");

				int weeksBehindForOverride = Integer
						.parseInt(PropertyManager.getProperty("NO_OF_WEEKS_BEHIND_FOR_OVERRIDE_ITEMS", "8"));

				logger.debug("lookupPastOverrides() - Getting past override details...");

				HashMap<ItemKey, PRItemDTO> overrideItemDataMap = new PricingEngineDAO()
						.getPastRecommendationOverrideDetails(conn, locationLevelId, locationId, productLevelId,
								productId, weekStartDate, weeksBehindForOverride);

				logger.debug("lookupPastOverrides() - Getting past override details is completed");

				updatePastOverridePrice(itemDataMap, overrideItemDataMap);

			}
		}
	}
}
