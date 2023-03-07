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
import com.pristine.dto.Ad;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
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

public class AdDataSetupCommonAPI {

	@SuppressWarnings("unused")
	private static Logger logger = Logger.getLogger("AdDataSetupCommonAPI");

	private String fiscalOrAdCalendar = (PropertyManager.getProperty("CALENDAR_TYPE", "B"));

	RestAPIConnectionService restAPIService = new RestAPIConnectionService();

	public void setupData(RecommendationInputDTO recommendationInputDTO, HashMap<ItemKey, PRItemDTO> itemDataMap,
			BaseData baseData)
			throws Exception, GeneralException {

		int noOfsaleAdDisplayWeeks = MultiWeekRecConfigSettings.getMwrNoOfSaleAdDisplayWeeks();

		List<Ad> adDataForAllItems = restAPIService
				.getADDataFromAPI(getAdDataInput(recommendationInputDTO, noOfsaleAdDisplayWeeks));

		setAdForAllItems(adDataForAllItems, itemDataMap, recommendationInputDTO.getBaseWeek(),
				recommendationInputDTO.getStartWeek(), noOfsaleAdDisplayWeeks);
		
		setBaseData(baseData, adDataForAllItems);

	}

	private void setAdForAllItems(List<Ad> adDataForAllItems, HashMap<ItemKey, PRItemDTO> itemDataMap,
			String currentWeek, String recWeekStartDate, int noOfsaleAdDisplayWeeks) {
		Map<Integer, List<Ad>> adByItem = adDataForAllItems.stream().collect(Collectors.groupingBy(Ad::getItemCode));

		itemDataMap.forEach((item, itemDTO) -> {
			if (!itemDTO.isLir()) {

				if (adByItem.containsKey(itemDTO.getItemCode())) {
					setAdDetail(adByItem.get(itemDTO.getItemCode()), itemDTO, recWeekStartDate);
				}
			}
		});

	}

	private void setAdDetail(List<Ad> adPromos, PRItemDTO itemDTO, String recWeekStartDate) {
		String weekStartDate = null;

		if (!itemDTO.isFuturePromotion() && itemDTO.isOnGoingPromotion()) {
			weekStartDate = recWeekStartDate;
		}

		if (weekStartDate != null && !weekStartDate.trim().isEmpty()) {
			for (Ad adInfoDTO : adPromos) {
				if (adInfoDTO.getCalendarStartDate().equals(weekStartDate)) {
					// pick first occurrence
					if (itemDTO.getRecWeekAdInfo().getAdPageNo() == 0) {
						PRItemAdInfoDTO adDTO = new PRItemAdInfoDTO();
						covertToPRItemAdInfoDTO(adInfoDTO, adDTO);
						itemDTO.setRecWeekAdInfo(adDTO);
						break;
					}
				}
			}
		}

	}

	public void covertToPRItemAdInfoDTO(Ad adInfo, PRItemAdInfoDTO adInfoDTO) {
		adInfoDTO.setAdPageNo(adInfo.getPageNumber());
		adInfoDTO.setAdBlockNo(adInfo.getBlockNumber());
		adInfoDTO.setWeeklyAdStartDate(adInfo.getCalendarStartDate());
	}

	private DataInputForAPI getAdDataInput(RecommendationInputDTO recommendationInputDto, int noOfsaleAdDisplayWeeks) {
		DataInputForAPI adDataImput = new DataInputForAPI();
		int noOfDays = (noOfsaleAdDisplayWeeks * 7) - 1;
		LocalDate endDate = DateUtil
				.stringToLocalDate(recommendationInputDto.getQuarterStartDate(), Constants.APP_DATE_FORMAT)
				.plusDays(noOfDays);
		adDataImput.setProductLevelId(recommendationInputDto.getProductLevelId());
		adDataImput.setProductId(recommendationInputDto.getProductId());
		adDataImput.setLocationLevelId(recommendationInputDto.getLocationLevelId());
		adDataImput.setLocationId(recommendationInputDto.getLocationId());
		adDataImput.setCalType(PRConstants.CALENDAR_WEEK);
		adDataImput.setStartDate(recommendationInputDto.getQuarterStartDate());
		adDataImput.setEndDate(DateUtil.localDateToString(endDate, Constants.APP_DATE_FORMAT));
		adDataImput.setFiscalOrAdCalendar(fiscalOrAdCalendar);
		adDataImput.setFetchStorePrice(0);
		return adDataImput;
	}

	private void setBaseData(BaseData baseData, List<Ad> adList) {
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<>();

		Map<Integer, List<Ad>> adByItem = adList.stream().collect(Collectors.groupingBy(Ad::getItemCode));

		adByItem.forEach((itemCode, adItemList) -> {
			List<PRItemAdInfoDTO> saleList = new ArrayList<>();
			adItemList.forEach(ad -> {
				PRItemAdInfoDTO adDTO = new PRItemAdInfoDTO();
				covertToPRItemAdInfoDTO(ad, adDTO);
				saleList.add(adDTO);
			});
			adDetails.put(itemCode, saleList);
		});

		baseData.setAdDetails(adDetails);
	}
}
