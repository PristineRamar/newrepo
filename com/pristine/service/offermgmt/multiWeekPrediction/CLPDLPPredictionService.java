package com.pristine.service.offermgmt.multiWeekPrediction;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dataload.offermgmt.mwr.CommonDataHelper;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.mwr.CLPDLPPredictionDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.mwr.ScenarioType;
import com.pristine.service.offermgmt.DisplayTypeLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.mwr.basedata.BaseData;
import com.pristine.service.offermgmt.prediction.PredictionException;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class CLPDLPPredictionService {
	
	private static Logger logger = Logger.getLogger("CLPDLPPredictionService");
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @param weeklyItemDataMap
	 * @param allWeekCalendarMap
	 * @return clpDlpPredictions
	 * @throws PredictionException
	 */
	public void getCLPDLPPredictions(CommonDataHelper commonDataHelper, BaseData baseData,
			RecommendationInputDTO recommendationInputDTO) throws PredictionException {
		MultiWeekPredictionService multiWeekPredictionService = new MultiWeekPredictionService();
		boolean isOnline = false;
		if(recommendationInputDTO.getRunMode() == PRConstants.RUN_TYPE_ONLINE) {
			isOnline = true;
		}
		List<CLPDLPPredictionDTO> clpDlpInputCol = buildCLPDLPInput(recommendationInputDTO,
				baseData.getWeeklyItemDataMap());

		logger.info("getCLPDLPPredictions() - # of records to be predicted - " + clpDlpInputCol.size());

		List<CLPDLPPredictionDTO> clpDlpOutputCol = multiWeekPredictionService
				.callCLPDLPPredictionEngine(clpDlpInputCol, isOnline);

		logger.info("getCLPDLPPredictions() - # of records predicted - " + clpDlpOutputCol.size());

		updateCalendarId(clpDlpOutputCol, commonDataHelper.getAllWeekCalendarDetails());

		updateScenarioType(clpDlpInputCol, clpDlpOutputCol);

		HashMap<Integer, List<CLPDLPPredictionDTO>> clpDlpByCalendarId = (HashMap<Integer, List<CLPDLPPredictionDTO>>) clpDlpOutputCol
				.stream().collect(Collectors.groupingBy(CLPDLPPredictionDTO::getCalendarId));

		baseData.setClpDlpPredictions(clpDlpByCalendarId);
	}

	/**
	 * 
	 * @param recommendationInputDTO
	 * @param weeklyItemDataMap
	 * @param allWeekDeatils
	 * @return
	 * @throws PredictionException
	 */
	public HashMap<Integer, List<CLPDLPPredictionDTO>> getCLPDLPPredictions(
			RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap,
			HashMap<String, RetailCalendarDTO> allWeekDeatils, boolean isOnline) throws PredictionException {
		MultiWeekPredictionService multiWeekPredictionService = new MultiWeekPredictionService();

		List<CLPDLPPredictionDTO> clpDlpInputCol = buildCLPDLPInput(recommendationInputDTO, weeklyItemDataMap);

		logger.info("getCLPDLPPredictions() - # of records to be predicted - " + clpDlpInputCol.size());

		List<CLPDLPPredictionDTO> clpDlpOutputCol = multiWeekPredictionService
				.callCLPDLPPredictionEngine(clpDlpInputCol, isOnline);

		logger.info("getCLPDLPPredictions() - # of records predicted - " + clpDlpOutputCol.size());

		updateCalendarId(clpDlpOutputCol, allWeekDeatils);

		updateScenarioType(clpDlpInputCol, clpDlpOutputCol);

		HashMap<Integer, List<CLPDLPPredictionDTO>> clpDlpByCalendarId = (HashMap<Integer, List<CLPDLPPredictionDTO>>) clpDlpOutputCol
				.stream().collect(Collectors.groupingBy(CLPDLPPredictionDTO::getCalendarId));

		return clpDlpByCalendarId;
	}
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @param weeklyItemDataMap
	 * @return input for clp dlp prediction
	 */
	private List<CLPDLPPredictionDTO> buildCLPDLPInput(RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap) {
		
		int lastPage = Integer.parseInt(PropertyManager.getProperty("LAST_PAGE_NO", "0"));
		int gatePage = Integer.parseInt(PropertyManager.getProperty("GATE_PAGE_NO", "0"));
		
		List<CLPDLPPredictionDTO> clpDlpInput = new ArrayList<>();

		weeklyItemDataMap.forEach((recWeekKey, itemMap) -> {
			CLPDLPPredictionDTO clpdlpPredictionDTO = new CLPDLPPredictionDTO();

			clpdlpPredictionDTO.setProductLevelId(recommendationInputDTO.getProductLevelId());
			clpdlpPredictionDTO.setProductId(recommendationInputDTO.getProductId());
			clpdlpPredictionDTO.setLocationLevelId(recommendationInputDTO.getLocationLevelId());
			clpdlpPredictionDTO.setLocationId(recommendationInputDTO.getLocationId());
			clpdlpPredictionDTO.setStartDateInput(recWeekKey.weekStartDate);
			clpdlpPredictionDTO.setScenarioId(1);

			List<MWRItemDTO> items = itemMap.values().stream().filter(m -> !m.isLir()).collect(Collectors.toList());

			// Recommended avg price
			OptionalDouble avgMinPriceOption = items.stream().filter(m -> m.getFinalRecPrice() != null)
					.mapToDouble(m -> m.getFinalRecPrice().getUnitPrice()).average();

			if (avgMinPriceOption.isPresent()) {
				clpdlpPredictionDTO.setAvgMinPrice(PRCommonUtil.round(avgMinPriceOption.getAsDouble(), 2));
			}

			// Recommended avg price
			OptionalDouble avgCostOption = items.stream().filter(m -> m.getFinalCost() != null)
					.mapToDouble(m -> m.getFinalCost()).average();

			if (avgCostOption.isPresent()) {
				clpdlpPredictionDTO.setAvgCost(PRCommonUtil.round(avgCostOption.getAsDouble(), 2));
			}

			clpdlpPredictionDTO.setAvgPromoPrice(getAvgPromoPrice(items));
			clpdlpPredictionDTO.setTotalPromoItems(
					(int) items.stream().filter(m -> m.getSalePrice() != null && m.getSalePrice() > 0).count());
			clpdlpPredictionDTO.setTotalAdItems(
					(int) items.stream().filter(m -> m.getAdPageNo() != null && m.getAdPageNo() > 0).count());
			clpdlpPredictionDTO.setTotalDisplayItems(
					(int) items.stream().filter(m -> m.getDisplayTypeId() != null && m.getDisplayTypeId() > 0).count());
			clpdlpPredictionDTO.setTotalItemsOnFirstPage(
					(int) items.stream().filter(m -> m.getAdPageNo() != null && m.getAdPageNo() == 1).count());

			if (lastPage > 0) {
				clpdlpPredictionDTO.setTotalItemsOnLastPage((int) items.stream()
						.filter(m -> m.getAdPageNo() != null && m.getAdPageNo() == lastPage).count());
			}

			if (gatePage > 0) {
				clpdlpPredictionDTO.setTotalItemsOnGatePage((int) items.stream()
						.filter(m -> m.getAdPageNo() != null && m.getAdPageNo() == gatePage).count());
			}

			if (lastPage > 0) {
				clpdlpPredictionDTO.setTotalItemsOnMiddlePage((int) items.stream().filter(m -> (m.getAdPageNo() != null
						&& m.getAdPageNo() > 0 && m.getAdPageNo() != 1 && m.getAdPageNo() != lastPage)).count());
			}

			clpdlpPredictionDTO
					.setTotalItemsOnFastwall(
							(int) items.stream()
									.filter(m -> m.getDisplayTypeId() != null
											&& m.getDisplayTypeId() == DisplayTypeLookup.FAST_WALL.getDisplayTypeId())
									.count());

			clpdlpPredictionDTO
					.setTotalItemsOnEnd(
							(int) items.stream()
									.filter(m -> m.getDisplayTypeId() != null
											&& m.getDisplayTypeId() == DisplayTypeLookup.END.getDisplayTypeId())
									.count());

			clpdlpPredictionDTO
					.setTotalItemsOnShipper(
							(int) items.stream()
									.filter(m -> m.getDisplayTypeId() != null
											&& m.getDisplayTypeId() == DisplayTypeLookup.SHIPPER.getDisplayTypeId())
									.count());

			clpdlpPredictionDTO
					.setTotalItemsOnLobby(
							(int) items.stream()
									.filter(m -> m.getDisplayTypeId() != null
											&& m.getDisplayTypeId() == DisplayTypeLookup.LOBBY.getDisplayTypeId())
									.count());

			clpdlpPredictionDTO
					.setTotalItemsOnWing(
							(int) items.stream()
									.filter(m -> m.getDisplayTypeId() != null
											&& m.getDisplayTypeId() == DisplayTypeLookup.WING.getDisplayTypeId())
									.count());

			clpdlpPredictionDTO.setScenarioTypeId(ScenarioType.TOTAL_NEW.getScenarioTypeId());
			clpDlpInput.add(clpdlpPredictionDTO);

			try {
				// Create current price scenario
				CLPDLPPredictionDTO clpdlpPredictionDTO2 = (CLPDLPPredictionDTO) clpdlpPredictionDTO.clone();
				clpdlpPredictionDTO2.setScenarioId(2);
				// Recommended avg price
				OptionalDouble avgCurrentMinPriceOption = items.stream().filter(m -> m.getCurrentPrice() != null)
						.mapToDouble(m -> m.getCurrentPrice().getUnitPrice()).average();

				if (avgCurrentMinPriceOption.isPresent()) {
					clpdlpPredictionDTO2.setAvgMinPrice(PRCommonUtil.round(avgCurrentMinPriceOption.getAsDouble(), 2));
				}
				clpdlpPredictionDTO2.setScenarioTypeId(ScenarioType.TOTAL_CURRENT.getScenarioTypeId());
				clpDlpInput.add(clpdlpPredictionDTO2);
			} catch (Exception e) {
				logger.error("Error -- Cloning object", e);
			}
		});

		return clpDlpInput;
	}
	
	/**
	 * 
	 * @param items
	 * @return avg promo price
	 */
	private double getAvgPromoPrice(List<MWRItemDTO> items) {

		double avgPromoPrice = 0;
		List<Double> promoPricePoints = new ArrayList<>();
		for (MWRItemDTO mwrItemDTO : items) {
			if (mwrItemDTO.getSalePrice() != null) {
				if (mwrItemDTO.getSalePrice() > 0) {
					double salePrice = PRCommonUtil.getUnitPrice(mwrItemDTO.getSaleMultiple(),
							mwrItemDTO.getSalePrice(), mwrItemDTO.getSalePrice(), true);
					promoPricePoints.add(salePrice);
				}
			}
		}

		OptionalDouble avgPromoPriceOption = promoPricePoints.stream().mapToDouble(p -> p).average();
		if (avgPromoPriceOption.isPresent()) {
			avgPromoPrice = avgPromoPriceOption.getAsDouble();
		}

		return PRCommonUtil.round(avgPromoPrice, 2);
	}
	
	/**
	 * 
	 * @param clpDlpOutput
	 * @param calendarDetails
	 */
	private void updateCalendarId(List<CLPDLPPredictionDTO> clpDlpOutput,
			HashMap<String, RetailCalendarDTO> calendarDetails) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		clpDlpOutput.forEach(clpDlpPredictionDTO -> {
			LocalDate startDate = LocalDate.parse(clpDlpPredictionDTO.getStartDateInput(), formatter);
			String weekStrDate = startDate.format(PRCommonUtil.getDateFormatter());
			clpDlpPredictionDTO.setCalendarId(calendarDetails.get(weekStrDate).getCalendarId());
			clpDlpPredictionDTO.setStartDateInput(weekStrDate);
		});
	}
	
	/**
	 * 
	 * @param clpDlpInputCol
	 * @param clpDlpOutputCol
	 */
	private void updateScenarioType(List<CLPDLPPredictionDTO> clpDlpInputCol,
			List<CLPDLPPredictionDTO> clpDlpOutputCol) {
		for (CLPDLPPredictionDTO clpdlpPredictionInput : clpDlpInputCol) {
			for (CLPDLPPredictionDTO clpdlpPredictionOutput : clpDlpOutputCol) {
				if (clpdlpPredictionInput.getScenarioId() == clpdlpPredictionOutput.getScenarioId()
						&& clpdlpPredictionInput.getStartDateInput()
								.equals(clpdlpPredictionOutput.getStartDateInput())) {
					clpdlpPredictionOutput.setScenarioTypeId(clpdlpPredictionInput.getScenarioTypeId());
				}
			}
		}
	}
	
}
