package com.pristine.service.offermgmt.mwr.core;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.mwr.prediction.MultiWeekPredictionDAO;
import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.multiWeekPrediction.MultiWeekPredictionService;
import com.pristine.service.offermgmt.prediction.PredictionException;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class MutliWeekPredictionComponent {

	private static Logger logger = Logger.getLogger("MutliWeekPredictionComponent");
	
	private Connection conn;
	private MultiWeekPredictionDAO multiWeekPredictionDAO;
	
	public MutliWeekPredictionComponent(Connection conn){
		this.conn = conn;
		multiWeekPredictionDAO = new MultiWeekPredictionDAO();
	}
	
	
	/**
	 * 
	 * @param multiWeekPredInput
	 * @throws GeneralException 
	 * @throws Exception 
	 */
	public List<MultiWeekPredEngineItemDTO> callMultiWeekPrediction(RecommendationInputDTO recommendationInputDTO,
			List<MultiWeekPredEngineItemDTO> multiWeekPredInput, HashMap<String, RetailCalendarDTO> allWeekDetails,
			List<MultiWeekPredEngineItemDTO> predictionCache) throws GeneralException, Exception {

		List<MultiWeekPredEngineItemDTO> multiWeekPredOuput = new ArrayList<>();

		boolean isOnline = false;
		if (recommendationInputDTO.getRunMode() == PRConstants.RUN_TYPE_ONLINE) {
			isOnline = true;
		}

		// 1. Create run header
		long runId = multiWeekPredictionDAO.insertPreditionRunHeader(conn, recommendationInputDTO);

		multiWeekPredictionDAO.updatePredictionRunHeader(conn, 0, "Predicting all items", Constants.EMPTY,
				runId);

		// Changes for handling items with no recent weeks movement/less movement in last X weeks
		// Changes to control number of items passed to prediction. Done by Pradeep on 03/13/2020
		List<MultiWeekPredEngineItemDTO> predInputWithValidItems = multiWeekPredInput.stream()
				.filter(m ->  m.isSendToPrediction()).collect(Collectors.toList());
		/*
		 * List<MultiWeekPredEngineItemDTO> predInputWithValidItems =
		 * multiWeekPredInput.stream() .filter(m -> !m.isNoRecentWeeksMov() &&
		 * m.isSendToPrediction()).collect(Collectors.toList());
		 */
		//logger.info("callMultiWeekPrediction()- #predInputWithValidItems:"+ predInputWithValidItems.size());

		/*
		 * List<MultiWeekPredEngineItemDTO> inputsAlreadyUpdatedWithPredStatus =
		 * multiWeekPredInput.stream() .filter(m
		 * ->m.isNoRecentWeeksMov()).collect(Collectors.toList());
		 */
		//logger.info("callMultiWeekPrediction()- #inputsAlreadyUpdatedWithPredStatus:"+ inputsAlreadyUpdatedWithPredStatus.size());
		//Changes for handling items with less store revenue in last X weeks
		// Changes to control number of items passed to prediction. Done by Karishma on 04/07/2020
		List<MultiWeekPredEngineItemDTO> inputwithLessAStrRevenue = multiWeekPredInput.stream()
				.filter(m -> !m.isSendToPrediction() ).collect(Collectors.toList());
		logger.info("callMultiWeekPrediction()- #inputwithLessAStrRevenue:"+ inputwithLessAStrRevenue.size());
		// Merge inputs which are already updated with prediction status
		//multiWeekPredOuput.addAll(inputsAlreadyUpdatedWithPredStatus);
		// Changes ends
		//Merge input with predcition movement set with average movement for items with less store Revenue
		multiWeekPredOuput.addAll(inputwithLessAStrRevenue);
		
		List<MultiWeekPredEngineItemDTO> filteredInput = usePredictionCacheAndFilterInputs(predInputWithValidItems,
				multiWeekPredOuput, predictionCache);
		//logger.info("callMultiWeekPrediction()- #filteredInput:"+ filteredInput.size());
		
		int totalItems = (int) predInputWithValidItems.stream()
				.mapToInt(MultiWeekPredEngineItemDTO::getItemCode).distinct().count();
		
		int totalItemsToBePredicted = (int) filteredInput.stream()
				.mapToInt(MultiWeekPredEngineItemDTO::getItemCode).distinct().count();
		
		if(totalItems != totalItemsToBePredicted) {
			logger.info("callMultiWeekPrediction() - Cache used for Items: " + (totalItems - totalItemsToBePredicted)
					+ ", Records: " + (predInputWithValidItems.size() - filteredInput.size()));	
		}
		

		logger.info("callMultiWeekPrediction() - Total # of items to be predicted: " + totalItemsToBePredicted
				+ ", Records: " + filteredInput.size());
		
		if (filteredInput.size() > 0) {

			// 1. Process all items
			callPredictionInBatches(filteredInput, multiWeekPredOuput, isOnline);

			if (multiWeekPredOuput.size() > 0) {

				multiWeekPredictionDAO.updatePredictionRunHeader(conn, 100, "Prediction is successful",
						PRConstants.RUN_STATUS_SUCCESS, runId);

				multiWeekPredictionDAO.updateEndTimeInHeader(conn, runId);

				// Update calendar id
				updateCalendarId(multiWeekPredOuput, allWeekDetails);

				// 3. Save prediction
				multiWeekPredictionDAO.saveMultiWeekPrediction(conn, runId, multiWeekPredOuput);
			} else {
				multiWeekPredictionDAO.updatePredictionRunHeader(conn, 100, "No output from prediction",
						PRConstants.RUN_STATUS_ERROR, runId);

				multiWeekPredictionDAO.updateEndTimeInHeader(conn, runId);
			}
		}
		return multiWeekPredOuput;
	}
	
	
	private List<MultiWeekPredEngineItemDTO> usePredictionCacheAndFilterInputs(
			List<MultiWeekPredEngineItemDTO> multiWeekPredInput, List<MultiWeekPredEngineItemDTO> multiWeekPredOuput,
			List<MultiWeekPredEngineItemDTO> predictionCache) {
		List<MultiWeekPredEngineItemDTO> predictionInputFiltered = new ArrayList<>();

		HashMap<Integer, HashMap<Integer, List<MultiWeekPredEngineItemDTO>>> inputScenarioByItems = groupByItemsAndScenario(
				multiWeekPredInput);
		HashMap<Integer, HashMap<Integer, List<MultiWeekPredEngineItemDTO>>> cacheScenarioByItems = groupByItemsAndScenario(
				predictionCache);

		inputScenarioByItems.forEach((itemCode, scenarios) -> {
			if (cacheScenarioByItems.containsKey(itemCode)) {
				HashMap<Integer, List<MultiWeekPredEngineItemDTO>> cacheScenarios = cacheScenarioByItems.get(itemCode);
				for (Map.Entry<Integer, List<MultiWeekPredEngineItemDTO>> inputScenarioEntry : scenarios.entrySet()) {
					boolean cacheUsed = false;
					List<MultiWeekPredEngineItemDTO> scenarioListInput = inputScenarioEntry.getValue();
					for (Map.Entry<Integer, List<MultiWeekPredEngineItemDTO>> cacheScenarioEntry : cacheScenarios
							.entrySet()) {
						List<MultiWeekPredEngineItemDTO> scenarioListCache = cacheScenarioEntry.getValue();
						if (scenarioListCache.size() == scenarioListInput.size()) {
							List<MultiWeekPredEngineItemDTO> cacheList = getPredictionsFromCache(scenarioListInput,
									scenarioListCache);
							if (cacheList != null) {
								multiWeekPredOuput.addAll(cacheList);
								cacheUsed = true;
							}
						}
					}
					
					if(!cacheUsed) {
						predictionInputFiltered.addAll(scenarioListInput);
					}
				}
			} else {
				scenarios.forEach((scenarioId, scenarioList) -> {
					predictionInputFiltered.addAll(scenarioList);
				});
			}
		});
		
		return predictionInputFiltered;
	}
	
	
	public List<MultiWeekPredEngineItemDTO> getPredictionsFromCache(List<MultiWeekPredEngineItemDTO> scenarioListInput,
			List<MultiWeekPredEngineItemDTO> scenarioListCache) {
		List<MultiWeekPredEngineItemDTO> cachedList = new ArrayList<>();;
		
		// Sort by date
		scenarioListInput.sort((m1, m2) -> m1.getStartDateAsLocalDate().compareTo(m2.getStartDateAsLocalDate()));
		scenarioListCache.sort((m1, m2) -> m1.getStartDateAsLocalDate().compareTo(m2.getStartDateAsLocalDate()));
		
		int matchCount = 0;
		for (int i = 0; i < scenarioListInput.size(); i++) {
			MultiWeekPredEngineItemDTO inputScenario = scenarioListInput.get(i);
			MultiWeekPredEngineItemDTO cacheScenario = scenarioListCache.get(i);
			if (MultiWeekPredEngineItemDTO.getMultiWeekPredKey(inputScenario)
					.equals(MultiWeekPredEngineItemDTO.getMultiWeekPredKey(cacheScenario))) {
				cacheScenario.setScenarioId(inputScenario.getScenarioId());
				matchCount++;
				cachedList.add(cacheScenario);
			}
		}
		
		
		if (matchCount != scenarioListCache.size()) {
			cachedList = null;
		}
		
		return cachedList;
	}
	
	/**
	 * 
	 * @param multiWeekPredOuput
	 * @param allWeekDetails
	 */
	private void updateCalendarId(List<MultiWeekPredEngineItemDTO> multiWeekPredOuput,
			HashMap<String, RetailCalendarDTO> allWeekDetails) {
		for (MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO : multiWeekPredOuput) {
			if (multiWeekPredEngineItemDTO.getCalendarId() == 0) {
				RetailCalendarDTO retailCalendarDTO = allWeekDetails.get(multiWeekPredEngineItemDTO.getStartDate());

				multiWeekPredEngineItemDTO.setCalendarId(retailCalendarDTO.getCalendarId());
			}
		}
	}
	
	/**
	 * 
	 * @param multiWeekPredInput
	 * @param multiWeekPredictionService
	 * @param multiWeekPredOuput
	 * @throws PredictionException
	 */
	private void callPredictionInBatches(List<MultiWeekPredEngineItemDTO> multiWeekPredInput,
			List<MultiWeekPredEngineItemDTO> multiWeekPredOutput, 
			boolean isOnline) throws PredictionException {
		int batchCount = 0;
		List<MultiWeekPredEngineItemDTO> multiWeekPredInputBatch = new ArrayList<>();

		HashMap<Integer, HashMap<Integer, List<MultiWeekPredEngineItemDTO>>> itemGroups = new HashMap<>();
		HashMap<Integer, List<MultiWeekPredEngineItemDTO>> ligGroup = groupByLIG(multiWeekPredInput);
		HashMap<Integer, List<MultiWeekPredEngineItemDTO>> nonLigGroup = getNonLIGItems(multiWeekPredInput);

		itemGroups.put(Constants.PRODUCT_LEVEL_ID_LIG, ligGroup);
		itemGroups.put(Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID, nonLigGroup);

		boolean limitReached = false;
		Set<Integer> itemCodes = new HashSet<>();
		for (Map.Entry<Integer, HashMap<Integer, List<MultiWeekPredEngineItemDTO>>> itemGroupEntry : itemGroups
				.entrySet()) {
			for (Map.Entry<Integer, List<MultiWeekPredEngineItemDTO>> itemLigEntry : itemGroupEntry.getValue()
					.entrySet()) {
				for (MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO : itemLigEntry.getValue()) {
					multiWeekPredInputBatch.add(multiWeekPredEngineItemDTO);
					itemCodes.add(multiWeekPredEngineItemDTO.getItemCode());
					if (itemCodes.size() % MultiWeekRecConfigSettings.getMwrPredictionBatchCount() == 0) {
						limitReached = true;
					}
				}

				if (limitReached) {
					limitReached = false;
					batchCount++;
					int noOfWeeks = (int) multiWeekPredInputBatch.stream()
							.mapToInt(MultiWeekPredEngineItemDTO::getCalendarId).distinct().count();
					logger.info("callPredictionInBatches() - Multi Week Prediction Batch: " + batchCount + ", Items: " + itemCodes.size()
							+ ", Records: " + multiWeekPredInputBatch.size() + ", Weeks: " + noOfWeeks);
					callPrediction(multiWeekPredInputBatch, multiWeekPredOutput, isOnline);
					logger.info("callPredictionInBatches() - Multi Week Prediction Batch " + batchCount + " ends.");
					itemCodes = new HashSet<>();
				}
			}
		}

		if (multiWeekPredInputBatch.size() > 0) {
			batchCount++;
			int noOfWeeks = (int) multiWeekPredInputBatch.stream()
					.mapToInt(MultiWeekPredEngineItemDTO::getCalendarId).distinct().count();
			logger.info("callPredictionInBatches() - Multi Week Prediction Batch: " + batchCount + ", Items: " + itemCodes.size()
					+ ", Records: " + multiWeekPredInputBatch.size() + ", Weeks: " + noOfWeeks);
			itemCodes = null;
			callPrediction(multiWeekPredInputBatch, multiWeekPredOutput, isOnline);
			logger.info("callPredictionInBatches() - Multi Week Prediction Batch " + batchCount + " ends.");
		}
	}
	
	
	/**
	 * 
	 * @param multiWeekPredInput
	 * @param multiWeekPredictionService
	 * @param multiWeekPredOuput
	 * @throws PredictionException
	 */
	@SuppressWarnings("unused")
	private void callPredictionForNonLIG(List<MultiWeekPredEngineItemDTO> multiWeekPredInput,
			List<MultiWeekPredEngineItemDTO> multiWeekPredOutput, 
			boolean isOnline) throws PredictionException {
		List<MultiWeekPredEngineItemDTO> multiWeekPredInputBatch = new ArrayList<>();

		HashMap<Integer, List<MultiWeekPredEngineItemDTO>> nonLigGroup = getNonLIGItems(multiWeekPredInput);
		int batchCount = 0;
		boolean limitReached = false;
		Set<Integer> itemCodes = new HashSet<>();
		for (Map.Entry<Integer, List<MultiWeekPredEngineItemDTO>> nonLigEntry : nonLigGroup.entrySet()) {
			for(MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO: nonLigEntry.getValue()){
				multiWeekPredInputBatch.add(multiWeekPredEngineItemDTO);	
				itemCodes.add(multiWeekPredEngineItemDTO.getItemCode());
				if(itemCodes.size() % MultiWeekRecConfigSettings.getMwrPredictionBatchCount() == 0){
					limitReached = true;
				}
			}

			if (limitReached) {
				limitReached = false;
				batchCount++;
//				logger.info("callPrediction() - Batch: " + batchCount + ", Items: " + itemCodes.size() + ", Records: "
//						+ multiWeekPredInputBatch.size());
				itemCodes = new HashSet<>();
				callPrediction(multiWeekPredInputBatch, multiWeekPredOutput, isOnline);
			}
		}

		if (multiWeekPredInputBatch.size() > 0) {
			batchCount++;
//			logger.info("callPrediction() - Batch: " + batchCount + ", Items: " + itemCodes.size() + ", Records: "
//					+ multiWeekPredInputBatch.size());
			itemCodes = null;
			callPrediction(multiWeekPredInputBatch, multiWeekPredOutput, isOnline);
		}

	}
	
	/**
	 * 
	 * @param multiWeekPredInput
	 * @return grouped by scenario
	 */
	private HashMap<Integer, List<MultiWeekPredEngineItemDTO>> groupByLIG(List<MultiWeekPredEngineItemDTO> multiWeekPredInput){
		return (HashMap<Integer, List<MultiWeekPredEngineItemDTO>>) multiWeekPredInput.stream().filter(p -> p.getRetLirId() > 0)
				.collect(Collectors.groupingBy(MultiWeekPredEngineItemDTO::getRetLirId));
	}
	
	
	/**
	 * 
	 * @param multiWeekPredInput
	 * @return grouped by scenario
	 */
	private HashMap<Integer, List<MultiWeekPredEngineItemDTO>> getNonLIGItems(List<MultiWeekPredEngineItemDTO> multiWeekPredInput){
		return (HashMap<Integer, List<MultiWeekPredEngineItemDTO>>) multiWeekPredInput.stream().filter(p -> p.getRetLirId() == 0)
				.collect(Collectors.groupingBy(MultiWeekPredEngineItemDTO::getItemCode));
	}
	
	
	/**
	 * 
	 * @param multiWeekPredEngineItemDTOs
	 * @param predictionThreads
	 * @throws PredictionException 
	 */
	private void callPrediction(List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOs,
			List<MultiWeekPredEngineItemDTO> multiWeekPredEngineOutput, boolean isOnline) throws PredictionException {
		
		MultiWeekPredictionService multiWeekPredictionService = new MultiWeekPredictionService();
		
//		logger.info("callPrediction() - # of price points to be predicted: " + multiWeekPredEngineItemDTOs.size());
		
		List<MultiWeekPredEngineItemDTO> predictions = multiWeekPredictionService
				.callPredictionEngine(multiWeekPredEngineItemDTOs, isOnline);
		
		changeDateFormat(predictions);
		
//		logger.info("callPrediction() - # of price points predicted: " + predictions.size());
		
		multiWeekPredEngineOutput.addAll(predictions);
		
		multiWeekPredEngineItemDTOs.clear();
	}
	
	
	private void changeDateFormat(List<MultiWeekPredEngineItemDTO> predictions) {
		predictions.forEach(m -> {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
			LocalDate startDate = LocalDate.parse(m.getStartDate(), formatter);
			String weekStrDate = startDate.format(PRCommonUtil.getDateFormatter());
			m.setStartDate(weekStrDate);
		});
	}
	
	/**
	 * 
	 * @param weeklyItemDataMap
	 * @param multiWeekPredOuput
	 * @param calDetails
	 */
	public void updateWeeklyPredictedMovement(HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap,
			List<MultiWeekPredEngineItemDTO> multiWeekPredOuput, HashMap<String, RetailCalendarDTO> calDetails) {

		multiWeekPredOuput.stream().forEach(multiWeekPredEngineItemDTO -> {

			ItemKey itemKey = new ItemKey(multiWeekPredEngineItemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);

			RetailCalendarDTO retailCalendarDTO = calDetails.get(multiWeekPredEngineItemDTO.getStartDate());

			RecWeekKey recWeekKey = new RecWeekKey(retailCalendarDTO.getCalendarId(), retailCalendarDTO.getStartDate());

			HashMap<ItemKey, MWRItemDTO> itemMap = weeklyItemDataMap.get(recWeekKey);

			if (itemMap.containsKey(itemKey)) {
				MWRItemDTO mwrItemDTO = itemMap.get(itemKey);
				PricePointDTO pricePointDTO = getPricePointDTOFromMultiWeek(multiWeekPredEngineItemDTO);
				if (pricePointDTO.getSalePrice() > 0) {
					MultiplePrice multiplePrice = new MultiplePrice(pricePointDTO.getSaleQuantity(),
							pricePointDTO.getSalePrice());
					mwrItemDTO.addSalePricePrediction(multiplePrice, pricePointDTO);
				} else {
					MultiplePrice multiplePrice = new MultiplePrice(pricePointDTO.getRegQuantity(),
							pricePointDTO.getRegPrice());
					mwrItemDTO.addRegPricePrediction(multiplePrice, pricePointDTO);
				}
			}
		});
	}
	
	/**
	 * 
	 * @param weeklyItemDataMap
	 * @param retLirMap
	 */
	public void updateLigLevelMovement(HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {

		
		for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weekEntry : weeklyItemDataMap.entrySet()) {

			HashMap<ItemKey, MWRItemDTO> itemMap = weekEntry.getValue();
			
			for(Map.Entry<ItemKey, MWRItemDTO> itemEntry: itemMap.entrySet()){
				MWRItemDTO mwrItemDTO = itemEntry.getValue();
				if(mwrItemDTO.isLir()){
					HashMap<MultiplePrice, PricePointDTO> regPriceMapLig = mwrItemDTO.getRegPricePredictionMap();
					HashMap<MultiplePrice, PricePointDTO> salePriceMapLig = mwrItemDTO.getSalePricePredictionMap();
					if(retLirMap.get(mwrItemDTO.getRetLirId()) != null){
						List<PRItemDTO> ligMemberList = retLirMap.get(mwrItemDTO.getRetLirId());
						boolean atleastOneMemberWithValidPrediction = false;
						int repPredStatusCode = PredictionStatus.SUCCESS.getStatusCode();
						for(PRItemDTO itemDTO: ligMemberList){
							ItemKey itemKey = PRCommonUtil.getItemKey(itemDTO);
							if (itemMap.get(itemKey) != null) {
								MWRItemDTO ligMember = itemMap.get(itemKey);

								HashMap<MultiplePrice, PricePointDTO> regPriceMapLigMember = ligMember
										.getRegPricePredictionMap();
								HashMap<MultiplePrice, PricePointDTO> salePriceMapLigMember = ligMember
										.getSalePricePredictionMap();

								for(PricePointDTO pricePointDTO: regPriceMapLigMember.values()) {
									if (pricePointDTO.getPredictionStatus() != null
											&& pricePointDTO.getPredictionStatus()
													.getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()) {
										atleastOneMemberWithValidPrediction = true;
									} else {
										repPredStatusCode = pricePointDTO.getPredictionStatus() != null
												? pricePointDTO.getPredictionStatus().getStatusCode()
												: PredictionStatus.UNDEFINED.getStatusCode();
									}
								}
								
								for(PricePointDTO pricePointDTO: salePriceMapLigMember.values()) {
									if (pricePointDTO.getPredictionStatus() != null
											&& pricePointDTO.getPredictionStatus()
													.getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()) {
										atleastOneMemberWithValidPrediction = true;
									} else {
										repPredStatusCode = pricePointDTO.getPredictionStatus() != null
												? pricePointDTO.getPredictionStatus().getStatusCode()
												: PredictionStatus.UNDEFINED.getStatusCode();
									}
								}
								aggrgateByPricePoints(regPriceMapLigMember, regPriceMapLig);
								aggrgateByPricePoints(salePriceMapLigMember, salePriceMapLig);
							}
						}
						
						
						// Changes for updating prediction status @ LIG level
						// Changes done by Pradeep on 06/03/2020
						// Issue reported by RiteAid
						if (!atleastOneMemberWithValidPrediction) {
							for (PricePointDTO pricePointDTO : regPriceMapLig.values()) {
								pricePointDTO.setPredictionStatus(PredictionStatus.get(repPredStatusCode));
							}

							for (PricePointDTO pricePointDTO : salePriceMapLig.values()) {
								pricePointDTO.setPredictionStatus(PredictionStatus.get(repPredStatusCode));
							}
						}
					}
				}
			}
		}
	}

	/**
	 * 
	 * @param priceMapLigMember
	 * @param priceMapLig
	 */
	private void aggrgateByPricePoints(HashMap<MultiplePrice, PricePointDTO> priceMapLigMember,
			HashMap<MultiplePrice, PricePointDTO> priceMapLig) {

		for (Map.Entry<MultiplePrice, PricePointDTO> priceEntry : priceMapLigMember.entrySet()) {
			PricePointDTO pricePointDTOLigMem = priceEntry.getValue();
			if (priceMapLig.get(priceEntry.getKey()) != null) {
				PricePointDTO pricePointDTO = priceMapLig.get(priceEntry.getKey());
				if (pricePointDTOLigMem.getPredictedMovement() != null && pricePointDTOLigMem.getPredictedMovement() > 0) {

					pricePointDTO.setPredictedMovement(
							pricePointDTO.getPredictedMovement() + pricePointDTOLigMem.getPredictedMovement());
				}
			} else {
				PricePointDTO pricePointDTO = new PricePointDTO();
				pricePointDTO.copy(pricePointDTOLigMem);
				if (pricePointDTO.getPredictionStatus() != null && pricePointDTO.getPredictionStatus()
						.getStatusCode() != PredictionStatus.SUCCESS.getStatusCode()) {
					pricePointDTO.setPredictedMovement(0D);
					pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
				}
				priceMapLig.put(priceEntry.getKey(), pricePointDTO);
			}
		}
	}
	
	/**
	 * 
	 * @param itemDataMap
	 * @param weeklyItemDataMap
	 */
	public void updateMultiWeekOrQuarterLevelPredMov(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap,
			RecommendationInputDTO recommendationInputDTO) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);

		LocalDate recStartWeek = LocalDate.parse(recommendationInputDTO.getStartWeek(), formatter);

		itemDataMap.forEach((itemKey, prItemDTO) -> {

			HashMap<MultiplePrice, PricePointDTO> regPricePointsMultiWeek = prItemDTO.getRegPricePredictionMap();

			weeklyItemDataMap.forEach((recWeekKey, itemMap) -> {

				LocalDate wkStartDate = LocalDate.parse(recWeekKey.weekStartDate, formatter);

				if (wkStartDate.isAfter(recStartWeek) || wkStartDate.isEqual(recStartWeek)) {

					MWRItemDTO mwrItemDTO = itemMap.get(itemKey);

					HashMap<MultiplePrice, PricePointDTO> regPricePointsWeekly = mwrItemDTO.getRegPricePredictionMap();

					regPricePointsWeekly.forEach((multiPrice, pricePoint) -> {

						if (pricePoint.getPredictionStatus() != null
								&& pricePoint.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS
										.getStatusCode()
								&& pricePoint.getPredictedMovement() != null && pricePoint.getPredictedMovement() > 0) {

							if (regPricePointsMultiWeek.get(multiPrice) != null) {
								PricePointDTO pricePointDTO = regPricePointsMultiWeek.get(multiPrice);
								pricePointDTO.setPredictedMovement(
										pricePointDTO.getPredictedMovement() + pricePoint.getPredictedMovement());
							} else {
								PricePointDTO pricePointDTO = new PricePointDTO();
								pricePointDTO.copy(pricePoint);
								regPricePointsMultiWeek.put(multiPrice, pricePointDTO);
							}
						}
					});
				}
			});
		});
	}
	
	
	/**
	 * 
	 * @param multiWeekPredEngineItemDTO
	 * @return price point object
	 */
	private PricePointDTO getPricePointDTOFromMultiWeek(MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO){
		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegPrice(multiWeekPredEngineItemDTO.getRegPrice());
		pricePointDTO.setRegQuantity(multiWeekPredEngineItemDTO.getRegMultiple());
		pricePointDTO.setSalePrice(multiWeekPredEngineItemDTO.getSalePrice());
		pricePointDTO.setSaleQuantity(multiWeekPredEngineItemDTO.getSaleMultiple());
		pricePointDTO.setAdPageNo(multiWeekPredEngineItemDTO.getAdPageNo());
		pricePointDTO.setAdBlockNo(multiWeekPredEngineItemDTO.getAdBlockNo());
		pricePointDTO.setPromoTypeId(multiWeekPredEngineItemDTO.getPromoTypeId());
		pricePointDTO.setDisplayTypeId(multiWeekPredEngineItemDTO.getDisplayTypeId());
		pricePointDTO.setPredictedMovement(multiWeekPredEngineItemDTO.getPredictedMovement());
		pricePointDTO.setPredictionStatus(PredictionStatus.get(multiWeekPredEngineItemDTO.getPredictionStatus()));
		return pricePointDTO;
	}
	
	
	/**
	 * 
	 * @param multiWeekPredInput
	 * @return grouped by scenario
	 */
	private HashMap<Integer, HashMap<Integer, List<MultiWeekPredEngineItemDTO>>> groupByItemsAndScenario(
			List<MultiWeekPredEngineItemDTO> multiWeekPredInput) {

		HashMap<Integer, HashMap<Integer, List<MultiWeekPredEngineItemDTO>>> itemsByScenarios = new HashMap<>();
		for (MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO : multiWeekPredInput) {
			if (itemsByScenarios.containsKey(multiWeekPredEngineItemDTO.getItemCode())) {
				HashMap<Integer, List<MultiWeekPredEngineItemDTO>> scenarioMap = itemsByScenarios
						.get(multiWeekPredEngineItemDTO.getItemCode());
				List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOs = new ArrayList<>();
				if (scenarioMap.containsKey(multiWeekPredEngineItemDTO.getScenarioId())) {
					multiWeekPredEngineItemDTOs = scenarioMap.get(multiWeekPredEngineItemDTO.getScenarioId());
				}
				multiWeekPredEngineItemDTOs.add(multiWeekPredEngineItemDTO);
				scenarioMap.put(multiWeekPredEngineItemDTO.getScenarioId(), multiWeekPredEngineItemDTOs);
				itemsByScenarios.put(multiWeekPredEngineItemDTO.getItemCode(), scenarioMap);
			} else {
				HashMap<Integer, List<MultiWeekPredEngineItemDTO>> scenarioMap = new HashMap<>();
				List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOs = new ArrayList<>();
				multiWeekPredEngineItemDTOs.add(multiWeekPredEngineItemDTO);
				scenarioMap.put(multiWeekPredEngineItemDTO.getScenarioId(), multiWeekPredEngineItemDTOs);
				itemsByScenarios.put(multiWeekPredEngineItemDTO.getItemCode(), scenarioMap);
			}
		}
		return itemsByScenarios;
	}
}