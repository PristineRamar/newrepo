package com.pristine.service.offermgmt.data;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dataload.service.DataInputForAPI;
import com.pristine.dto.Display;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.RestAPIConnectionService;
import com.pristine.service.offermgmt.DisplayTypeLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.mwr.basedata.BaseData;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class DisplayDataSetupCommonAPI {

	@SuppressWarnings("unused")
	private static Logger logger = Logger.getLogger("DisplayDataSetupCommonAPI");

	private String fiscalOrAdCalendar = (PropertyManager.getProperty("CALENDAR_TYPE", "B"));

	RestAPIConnectionService restAPIService = new RestAPIConnectionService();

	public void setupData(RecommendationInputDTO recommendationInputDTO, HashMap<ItemKey, PRItemDTO> itemDataMap,
			BaseData baseData)
			throws Exception, GeneralException {

		int noOfsaleAdDisplayWeeks = MultiWeekRecConfigSettings.getMwrNoOfSaleAdDisplayWeeks();

		List<Display> displayForAllItems = restAPIService
				.getDisplayDataFromAPI(getDisplayInput(recommendationInputDTO, noOfsaleAdDisplayWeeks));

		setDisplayDataForAllItems(displayForAllItems, itemDataMap, recommendationInputDTO.getBaseWeek(),
				recommendationInputDTO.getStartWeek(), noOfsaleAdDisplayWeeks);

		setBaseData(baseData, displayForAllItems);
		
	}

	private void setDisplayDataForAllItems(List<Display> displayForAllItems, HashMap<ItemKey, PRItemDTO> itemDataMap,
			String currentWeek, String recWeekStartDate, int noOfsaleAdDisplayWeeks) {
		Map<Integer, List<Display>> displayByItem = displayForAllItems.stream()
				.collect(Collectors.groupingBy(Display::getItemCode));

		itemDataMap.forEach((item, itemDTO) -> {
			if (!itemDTO.isLir()) {

				if (displayByItem.containsKey(itemDTO.getItemCode())) {

					fillDisplayDetails(displayByItem.get(itemDTO.getItemCode()), itemDTO, recWeekStartDate);
				}
			}
		});

	}

	private void fillDisplayDetails(List<Display> displayDetails, PRItemDTO itemDTO, String recWeekStartDate) {

		String weekStartDate = null;

		if (!itemDTO.isFuturePromotion() && itemDTO.isOnGoingPromotion()) {
			weekStartDate = recWeekStartDate;
		}
		// Get latest future week display details
		if (weekStartDate != null) {
			for (Display displayInfoDTO : displayDetails) {
				if (displayInfoDTO.getCalendarStartDate().equals(weekStartDate)) {
					// pick first occurrence
					if (itemDTO.getFutWeekDisplayInfo().getDisplayTypeLookup() == null) {
						PRItemDisplayInfoDTO displayDTO = new PRItemDisplayInfoDTO();
						covertToPRItemDisplayInfoDTO(displayInfoDTO, displayDTO);
						itemDTO.setRecWeekDisplayInfo(displayDTO);
					}
				}
			}
		}
	}

	public void covertToPRItemDisplayInfoDTO(Display displayInfo, PRItemDisplayInfoDTO displayInfoDTO) {
		displayInfoDTO.setDisplayTypeLookup(DisplayTypeLookup.get(displayInfo.getDisplayTypeId()));
		displayInfoDTO.setDisplayWeekStartDate(displayInfo.getCalendarEndDate());

	}

	private DataInputForAPI getDisplayInput(RecommendationInputDTO recommendationInputDto, int noOfsaleAdDisplayWeeks) {
		DataInputForAPI displayInput = new DataInputForAPI();
		int noOfDays = (noOfsaleAdDisplayWeeks * 7) - 1;
		LocalDate endDate = DateUtil
				.stringToLocalDate(recommendationInputDto.getQuarterStartDate(), Constants.APP_DATE_FORMAT)
				.plusDays(noOfDays);
		displayInput.setProductLevelId(recommendationInputDto.getProductLevelId());
		displayInput.setProductId(recommendationInputDto.getProductId());
		displayInput.setLocationLevelId(recommendationInputDto.getLocationLevelId());
		displayInput.setLocationId(recommendationInputDto.getLocationId());
		displayInput.setCalType(PRConstants.CALENDAR_WEEK);
		displayInput.setStartDate(recommendationInputDto.getQuarterStartDate());
		displayInput.setEndDate(DateUtil.localDateToString(endDate, Constants.APP_DATE_FORMAT));
		displayInput.setFiscalOrAdCalendar(fiscalOrAdCalendar);
		displayInput.setFetchStorePrice(0);
		return displayInput;
	}

	private void setBaseData(BaseData baseData, List<Display> displayList) {
		HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails = new HashMap<>();

		Map<Integer, List<Display>> displayByItem = displayList.stream()
				.collect(Collectors.groupingBy(Display::getItemCode));

		displayByItem.forEach((itemCode, displayItemList) -> {
			List<PRItemDisplayInfoDTO> saleList = new ArrayList<>();
			displayItemList.forEach(displayInfoDTO -> {
				PRItemDisplayInfoDTO displayDTO = new PRItemDisplayInfoDTO();
				covertToPRItemDisplayInfoDTO(displayInfoDTO, displayDTO);
				saleList.add(displayDTO);
			});
			displayDetails.put(itemCode, saleList);
		});

		baseData.setDisplayDetails(displayDetails);
	}
}
