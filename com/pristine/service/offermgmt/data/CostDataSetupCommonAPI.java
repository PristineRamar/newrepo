package com.pristine.service.offermgmt.data;

import java.sql.Connection;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dataload.service.DataInputForAPI;
import com.pristine.dto.Cost;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.RestAPIConnectionService;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.mwr.basedata.BaseData;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class CostDataSetupCommonAPI {

	private static Logger logger = Logger.getLogger("CostDataSetupCommonAPI");

	private String fiscalOrAdCalendar = (PropertyManager.getProperty("CALENDAR_TYPE", "B"));
	DecimalFormat df = new DecimalFormat("######.##");

	boolean chkFutureCost=Boolean.parseBoolean(PropertyManager.getProperty("FETCH_FUTURE_COST", "FALSE"));
	
	RestAPIConnectionService restAPIService = new RestAPIConnectionService();
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);

	public void setupData(Connection conn, RecommendationInputDTO recommendationInputDTO,
			HashMap<ItemKey, PRItemDTO> itemDataMap,BaseData baseData) throws Exception, GeneralException {

		List<Cost> costDataOfAllItems = restAPIService
				.getCostFromAPI(getCostDataInputForListCost(conn, recommendationInputDTO, false,false,null));

		List<Cost> previousCostOfAllItems = restAPIService
				.getCostFromAPI(getCostDataInputForListCost(conn, recommendationInputDTO, true,false,null));
		
		// Set cost for all the items
		List<Integer> costFoundItems = setCostForAllItems(costDataOfAllItems, previousCostOfAllItems, itemDataMap,
				recommendationInputDTO);
		
		
		if (chkFutureCost) {
			List<Cost> futureCost = restAPIService
					.getCostFromAPI(getCostDataInputForListCost(conn, recommendationInputDTO, false, true,costFoundItems));

			if (futureCost != null) {
				Map<Integer, List<Cost>> costBycalendarId = futureCost.stream()
						.collect(Collectors.groupingBy(Cost::getCalendarId));
				baseData.setFutureCost(costBycalendarId);
				Map<String, List<Cost>> costByStartDate = futureCost.stream()
						.collect(Collectors.groupingBy(Cost::getStartDate));
				String nextWeekstartDate = DateUtil.dateToString(
						DateUtil.incrementDate(DateUtil.toDate(recommendationInputDTO.getStartWeek()), 7),
						Constants.APP_DATE_FORMAT);
				checkandSetFutureCost(itemDataMap, costByStartDate, nextWeekstartDate);
			}
		}
	
	}

	private List<Integer> setCostForAllItems(List<Cost> costList, List<Cost> previousCostOfAllItems,
			HashMap<ItemKey, PRItemDTO> itemDataMap, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {

		double costChangeThreshold = MultiWeekRecConfigSettings.getMwrCostChangeThreshold();
		Set<Integer>costNotFoundSet=new HashSet<>();

		Map<Integer, List<Cost>> costByItem = costList.stream().collect(Collectors.groupingBy(Cost::getItemCode));
		List<Integer> costFoundItems= new ArrayList<Integer>();

		Map<Integer, List<Cost>> previousCostMap = previousCostOfAllItems.stream()
				.collect(Collectors.groupingBy(Cost::getItemCode));

		for (Map.Entry<ItemKey, PRItemDTO> item : itemDataMap.entrySet()) {

			if (!item.getValue().isLir()) {
				if (costByItem.containsKey(item.getValue().getItemCode())) {
					List<Cost> costOfItem = costByItem.get(item.getValue().getItemCode());
					// get the max calendar Id and set the price
					Cost latestCostObj = costOfItem.stream().max(Comparator.comparing(Cost::getCalendarId)).get();
					item.getValue().setListCost(latestCostObj.getCost());
					item.getValue().setListCostEffDate(latestCostObj.getStartDate());
					item.getValue().setNipoBaseCost(latestCostObj.getNipoBaseCost());
					item.getValue().setCwacCoreCost(latestCostObj.getCwagCoreCost());
					item.getValue().setNipoCoreCost(latestCostObj.getNipoCoreCost());
					
					costFoundItems.add((item.getValue().getItemCode()));

					if (previousCostMap != null && previousCostMap.containsKey(item.getValue().getItemCode())) {

						List<Cost> prevCost = previousCostMap.get(item.getValue().getItemCode());

						if (prevCost != null && prevCost.size() > 0) {
							Cost previousCostOfitem = prevCost.stream().max(Comparator.comparing(Cost::getCalendarId))
									.get();
							Double previousCost = previousCostOfitem.getCost();

							double costChange = new Double(
									df.format(Math.abs(item.getValue().getListCost() - previousCost)));

							if (previousCost != item.getValue().getListCost() && costChange > costChangeThreshold) {

								try {
									setCostchange(item.getValue(), previousCost);
								} catch (GeneralException e) {

									logger.error("setCostForAllItems()- Exception in setCostchange : " + e);
								}
							} else
								item.getValue().setPreListCost(previousCost);

						} else {

							item.getValue().setPreListCost(item.getValue().getListCost());
						}

					}
				} else {
					costNotFoundSet.add(item.getValue().getItemCode());
					//logger.info("setCostForAllItems()- Cost not found for item: {} " + item.getValue().getItemCode());
				}
			}
		}

		logger.info("setCostForAllItems()- cost set for # items" + costFoundItems.size());
		
		if (costNotFoundSet.size() > 0) {
			logger.info("# items for which cost is not found: " + costNotFoundSet.size());
			logger.info(" itemsCodes: " + costNotFoundSet.toString());
		}
		costNotFoundSet.clear();
return costFoundItems;
	}

	public void setCostchange(PRItemDTO itemDTO, Double preListCost) throws GeneralException {
		boolean isConsiderAsCostChange = false;
		boolean isMarkAsReview = Boolean
				.parseBoolean(PropertyManager.getProperty("MARK_AS_REVIEW_IF_COST_CHANGE", "FALSE"));
		boolean isPreviousCostSet = false;

		if (itemDTO.getCurRegPriceEffDate() == null || itemDTO.getCurRegPriceEffDate() == null) {
			isConsiderAsCostChange = true;
		} else {
			Date curRegPriceEffDate = DateUtil.toDate(itemDTO.getCurRegPriceEffDate());

			// if reg effective date is equal or after cost effective date
			if (curRegPriceEffDate.equals(DateUtil.toDate(itemDTO.getListCostEffDate()))
					|| curRegPriceEffDate.after(DateUtil.toDate(itemDTO.getListCostEffDate()))) {
				isConsiderAsCostChange = false;
			} else if (curRegPriceEffDate.before(DateUtil.toDate(itemDTO.getListCostEffDate()))) {
				isConsiderAsCostChange = true;
			}

		}

		if (isConsiderAsCostChange) {
			itemDTO.setPreListCost(preListCost);

			{
				// set mark for review, when there is cost change
				if (itemDTO.getListCost() < itemDTO.getPreListCost()) {
					itemDTO.setCostChgIndicator(-1);
				} else if (itemDTO.getListCost() > itemDTO.getPreListCost()) {
					itemDTO.setCostChgIndicator(+1);
				}

				if (itemDTO.getCostChgIndicator() != 0 && isMarkAsReview) {
					itemDTO.setIsMarkedForReview(String.valueOf(Constants.YES));
				}

			}
			isPreviousCostSet = true;

		}

		if (!isPreviousCostSet) {
			itemDTO.setPreListCost(itemDTO.getListCost());
		}

		// logger.debug("setCostchange- " + itemDTO.getItemCode() + " is prevcost set :
		// " + isPreviousCostSet);

	}

	private DataInputForAPI getCostDataInputForListCost(Connection conn, RecommendationInputDTO recommendationInputDto,
			boolean previousCost,boolean futureCost, List<Integer> costFoundItems) throws GeneralException {
		DataInputForAPI costDataInput = new DataInputForAPI();
		RetailCalendarDTO calDTO = new RetailCalendarDAO().getCalendarId(conn, recommendationInputDto.getBaseWeek(),
				Constants.CALENDAR_WEEK);
		if (costFoundItems != null && costFoundItems.size() > 0) {
			costDataInput.setItemCodes(costFoundItems);
		} else {
			costDataInput.setProductLevelId(recommendationInputDto.getProductLevelId());
			costDataInput.setProductId(recommendationInputDto.getProductId());
		}
		costDataInput.setLocationLevelId(recommendationInputDto.getLocationLevelId());
		costDataInput.setLocationId(recommendationInputDto.getLocationId());
		costDataInput.setCalType(PRConstants.CALENDAR_WEEK);

		if (previousCost) {
			String previousWeekdate = DateUtil.dateToString(
					DateUtil.getPreviousWeekStartDate(DateUtil.toDate(recommendationInputDto.getBaseWeek()), 2),
					Constants.APP_DATE_FORMAT);

			String wkEnd = DateUtil.getWeekEndDate(DateUtil.toDate(previousWeekdate), Constants.APP_DATE_FORMAT);

			costDataInput.setStartDate(previousWeekdate);
			costDataInput.setEndDate(wkEnd);

		} else if (futureCost) {
			String nextWeekstartDate = DateUtil.dateToString(
					DateUtil.incrementDate(DateUtil.toDate(recommendationInputDto.getStartWeek()), 7),
					Constants.APP_DATE_FORMAT);
			costDataInput.setStartDate(nextWeekstartDate);
			costDataInput.setFetchFutureCost(true);
		}

		else {
			costDataInput.setStartDate(calDTO.getStartDate());
			costDataInput.setEndDate(calDTO.getEndDate());
		}

		costDataInput.setFiscalOrAdCalendar(fiscalOrAdCalendar);
		costDataInput.setCostTypeId(Constants.REGULAR_COST_ID);
		return costDataInput;
	}
	
	private void checkandSetFutureCost(HashMap<ItemKey, PRItemDTO> itemDataMap, Map<String, List<Cost>> costByStartDate,
			String nextWeekstartDate) {
		for (Map.Entry<String, List<Cost>> costByDate : costByStartDate.entrySet()) {

			if (LocalDate.parse(costByDate.getKey(), formatter).isAfter(LocalDate.parse(nextWeekstartDate, formatter))
					|| LocalDate.parse(costByDate.getKey(), formatter)
							.isEqual(LocalDate.parse(nextWeekstartDate, formatter))) {
				Map<Integer, List<Cost>> costByItem = costByDate.getValue().stream()
						.collect(Collectors.groupingBy(Cost::getItemCode));
				itemDataMap.forEach((item, itemDTO) -> {
					if (!itemDTO.isLir()) {
						if (costByItem.containsKey(itemDTO.getItemCode())) {
							List<Cost> priceOfItem = costByItem.get(itemDTO.getItemCode());
							Cost cost = priceOfItem.stream().findFirst().orElse(null);
							if (cost != null && cost.getCost() > 0 && itemDTO.getListCost() != null
									&& itemDTO.getListCost() > 0) {
								if (cost.getCost() != itemDTO.getListCost()) {
									itemDTO.setFutureListCost(cost.getCost());
									itemDTO.setFutureCostEffDate(cost.getStartDate());
								}
							}
						}
					}
				});

			}

		}
		
	}
	
	

}
