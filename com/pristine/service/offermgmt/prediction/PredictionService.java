package com.pristine.service.offermgmt.prediction;

import java.sql.Connection;
import java.sql.SQLException;
//import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.offermgmt.prediction.PredictionDAO;
import com.pristine.dataload.offermgmt.mwr.MultiWeekRecBatch;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.ProductDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.ExecutionOrder;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
//import com.pristine.dto.offermgmt.PRSubstituteGroup;
//import com.pristine.dto.offermgmt.PRSubstituteItem;
import com.pristine.dto.offermgmt.prediction.*;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.PricingEngineHelper;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;
//import com.pristine.util.offermgmt.PRFormatHelper;

public class PredictionService {
	private static Logger logger = Logger.getLogger("PredictionService");
	private Connection conn = null;
	private Context initContext;
	private Context envContext;
	private DataSource ds;
	private Boolean isOnline = false;
	private boolean isSubstitueImpact = false;
	private boolean isPromotion = false;
	private boolean isPredictionTest = false;
	private boolean isPassMissingLigMembers = false;
	
	HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory = 
			new HashMap<Integer, HashMap<String, List<RetailPriceDTO>>>();
	HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData = 
			new HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>>();
	HashMap<Integer, List<PRItemDTO>> ligMap = null;
	
	public PredictionService(HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData, 
			HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory, HashMap<Integer, List<PRItemDTO>> ligMap) {
		this.movementData = movementData;
		this.itemPriceHistory = itemPriceHistory;
		this.ligMap = ligMap;
	}
	
	public PredictionService() {

	}

	public static void main(String[] args) {

	}
	
	public PredictionOutputDTO predictMovement(Connection conn, PredictionInputDTO predictionInputDTO, 
			List<ExecutionTimeLog> executionTimeLogs, String predictionType, boolean isOnline) throws GeneralException {

		//logger.debug("Inside predictMovement()");		
		
		long predictionRunHeaderId = -1;
		PredictionOutputDTO predictionOutputDTO = new PredictionOutputDTO();

		//Convert Prediction Input DTO to Prediction Engine Input
		
		if(predictionInputDTO.predictionItems == null ||
				predictionInputDTO.predictionItems.size() == 0)
		{
			logger.warn("No Prediction Items in predictionInputDTO()");
			return predictionOutputDTO;
		}	 

		try {		
			
			
			//logger.debug("Prediction Input to Prediction Service: " + predictionInputDTO.toString());
			// step 1 Create a run id
			predictionInputDTO.runType = PRConstants.RUN_TYPE_BATCH;
			predictionRunHeaderId = createRunId(conn, predictionInputDTO);
			
			List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
			predictionInputDTOs.add(predictionInputDTO);
			this.isPassMissingLigMembers = true;
			predict(conn, predictionRunHeaderId, predictionInputDTOs, executionTimeLogs, predictionType, false);
			
			//Convert PredictionInputDTO to PredictionOutputDTO
			convertPredictionInputToOutput(predictionInputDTO, predictionOutputDTO);
			
			//logger.debug("Prediction Output from Prediction Service: " + predictionOutputDTO.toString());
			
			// update run status to complete
			updateRunStatusAsComplete(conn, predictionRunHeaderId);
			
		} catch (Exception | OfferManagementException e) {
			logger.error("Error in predictMovement() -- " + e.toString(), e);			 
			predictionOutputDTO = new PredictionOutputDTO();	
			try {
				updateRunStatusAsFailed(conn, predictionRunHeaderId);			 
				throw new GeneralException("Error in predictMovement() -- " + e.toString());
			} catch (Exception ex) {
				logger.error("Error while updating run status as failed" + ex.toString());
				throw new GeneralException("Error in predictMovement() -- " + ex.toString());
			}
		} finally {			
		}
		return predictionOutputDTO;
	}

	public RunStatusDTO predictMovementOnDemand(String predictionInput) {
		//logger.debug("Inside predictMovementOnDemand()");
		ObjectMapper mapper = new ObjectMapper();
		RunStatusDTO runStatusDTO = new RunStatusDTO();		
		
		isOnline = true;
		initializeForWS();
		Connection conn = getConnection();	 
		@SuppressWarnings("unused")
		ExecutionOrder executionOrder = new ExecutionOrder();
		try {
			PredictionInputDTO predictionInputDTO;
			List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
			//PredictionInputDTO predictionInputDTO = mapper.readValue(predictionInput, PredictionInputDTO.class);	
			predictionInputDTOs =  mapper.readValue(predictionInput, new TypeReference<List<PredictionInputDTO>>(){});
			if(predictionInputDTOs.size() > 0){
				//pick first in the list and use it in prediction run header
				predictionInputDTO = predictionInputDTOs.get(0);
				predictionInputDTO.runType = PRConstants.RUN_TYPE_ONLINE;
				runStatusDTO.runId = createRunId(conn, predictionInputDTO);
				PristineDBUtil.commitTransaction(conn, "Run Id is Committed");	
				asyncServiceMethod(conn, runStatusDTO.runId, predictionInputDTOs, false);
			}		
		} catch (Exception | GeneralException ex) {
			logger.error("Error while creating run Id" + ex.toString(), ex);
			PristineDBUtil.close(conn);
		}
		return runStatusDTO;
	}
	
	public RunStatusDTO explainPrediction(String predictionInput){
		logger.debug("Inside explainPrediction()");
		ObjectMapper mapper = new ObjectMapper();
		RunStatusDTO runStatusDTO = new RunStatusDTO();		
		
		isOnline = true;
		initializeForWS();
		Connection conn = getConnection();	 
		@SuppressWarnings("unused")
		ExecutionOrder executionOrder = new ExecutionOrder();
		try {
			PredictionInputDTO predictionInputDTO;
			List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
			predictionInputDTOs =  mapper.readValue(predictionInput, new TypeReference<List<PredictionInputDTO>>(){});
			if(predictionInputDTOs.size() > 0){
				//pick first in the list and use it in prediction run header
				predictionInputDTO = predictionInputDTOs.get(0);
				predictionInputDTO.runType = PRConstants.RUN_TYPE_ONLINE;
				runStatusDTO.runId = createRunId(conn, predictionInputDTO);
				PristineDBUtil.commitTransaction(conn, "Run Id is Committed");	
				asyncServiceMethod(conn, runStatusDTO.runId, predictionInputDTOs, true);
			}		
		} catch (Exception | GeneralException ex) {
			logger.error("Error while creating run Id" + ex.toString(), ex);
			PristineDBUtil.close(conn);
		}
		return runStatusDTO;
	}
	
//	public PredictionInputDTO predictSubstituteImpact(Connection conn, PredictionInputDTO predictionInputDTO)
//			throws Exception, GeneralException, OfferManagementException {
//		this.isSubstitueImpact = true;
//		List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
//		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
//
//		predictionInputDTOs.add(predictionInputDTO);
//		predict(conn, 0, predictionInputDTOs, executionTimeLogs, "PREDICTION_SUBSTITUTE", false);
//		return predictionInputDTO;
//	}
	
	public String predictSubstituteImpact(String predictionInput) {
		logger.debug("Inside predictSubstituteImpact()");
		ObjectMapper mapper = new ObjectMapper();
		String jsonOutput = "";
		
		isOnline = true;
		initializeForWS();
		Connection conn = getConnection();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		@SuppressWarnings("unused")
		ExecutionOrder executionOrder = new ExecutionOrder();
		try {
			
			PredictionInputDTO predictionInputDTO = mapper.readValue(predictionInput, PredictionInputDTO.class);		 
			logger.debug("PredictionInputDTO Item Size: " + predictionInputDTO.predictionItems.size());
			this.isSubstitueImpact = true;
			
			List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
			predictionInputDTOs.add(predictionInputDTO);
			
			predict(conn, 0, predictionInputDTOs, executionTimeLogs, "PREDICTION_SUBSTITUTE", false);
			jsonOutput = mapper.writeValueAsString(predictionInputDTO);
			pricingEngineDAO.insertExecutionTimeLog(conn, getRecommendationRunHeader(predictionInputDTO), executionTimeLogs);
			PristineDBUtil.commitTransaction(conn, "Transaction is Committed");	
		} catch (Exception | GeneralException | OfferManagementException ex) {
			logger.error("Error in predictSubstituteImpact" + ex.toString(), ex);			
		}finally {
			PristineDBUtil.close(conn);
		}
		return jsonOutput;
	}
	
	public String predictPromotion(String predictionInput){
		logger.debug("Inside predictPromotion()");
		ObjectMapper mapper = new ObjectMapper();
		String jsonOutput = "";
		
		isOnline = true;
		initializeForWS();
		Connection conn = getConnection();
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		try {			
			PredictionInputDTO predictionInputDTO = mapper.readValue(predictionInput, PredictionInputDTO.class);		 
			logger.debug("PredictionInputDTO Item Size: " + predictionInputDTO.predictionItems.size());
			isPromotion = true;
			
			List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
			predictionInputDTOs.add(predictionInputDTO);
			
			predictionInputDTO = predictionInputDTOs.get(0);
			predictionInputDTO.runType = PRConstants.RUN_TYPE_ONLINE;
			predictionInputDTO.endCalendarId = predictionInputDTO.startCalendarId;
			long runId = createRunId(conn, predictionInputDTO);
			
			predict(conn, runId, predictionInputDTOs, executionTimeLogs, "PREDICTION_PROMOTION", false);
			jsonOutput = mapper.writeValueAsString(predictionInputDTO);

			PristineDBUtil.commitTransaction(conn, "Transaction is Committed");	
		} catch (Exception | GeneralException | OfferManagementException ex) {
			logger.error("Error in predictSubstituteImpact" + ex.toString(), ex);			
		}finally {
			PristineDBUtil.close(conn);
		}
		return jsonOutput;
	}
	
	public String onDemandPredictionTest(String predictionInput) {
		logger.debug("Inside onDemandPredictionTest()");
		ObjectMapper mapper = new ObjectMapper();
		String jsonOutput = "";

		isOnline = true;
		initializeForWS();
		Connection conn = getConnection();
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		try {
			PredictionInputDTO predictionInputDTO;

			List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
			predictionInputDTOs = mapper.readValue(predictionInput, new TypeReference<List<PredictionInputDTO>>() {
			});
			if (predictionInputDTOs.size() > 0) {
				isPredictionTest = true;

				predictionInputDTO = predictionInputDTOs.get(0);
				predictionInputDTO.runType = PRConstants.RUN_TYPE_ONLINE;
				predictionInputDTO.endCalendarId = predictionInputDTO.startCalendarId;
				predictionInputDTO.isForcePrediction = true;

				predict(conn, 0, predictionInputDTOs, executionTimeLogs, "PREDICTION_TEST", false);
				jsonOutput = mapper.writeValueAsString(predictionInputDTO);
			}

		} catch (Exception | GeneralException | OfferManagementException ex) {
			logger.error("Error in predictSubstituteImpact" + ex.toString(), ex);
		} finally {
			PristineDBUtil.close(conn);
		}
		return jsonOutput;
	}
	
	public RunStatusDTO updatePrediction(String predictionInput) {
		ObjectMapper mapper = new ObjectMapper();
		RunStatusDTO runStatusDTO = new RunStatusDTO();		
		isOnline = true;
		initializeForWS();
		Connection conn = getConnection();	 
		@SuppressWarnings("unused")
		ExecutionOrder executionOrder = new ExecutionOrder();
		try {
			PredictionInputDTO predictionInputDTO;
			List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
			predictionInputDTOs =  mapper.readValue(predictionInput, new TypeReference<List<PredictionInputDTO>>(){});
			if(predictionInputDTOs.size() > 0){
				//pick first in the list and use it in prediction run header
				predictionInputDTO = predictionInputDTOs.get(0);
				predictionInputDTO.runType = PRConstants.RUN_TYPE_ONLINE;
				runStatusDTO.runId = createRunId(conn, predictionInputDTO);
				PristineDBUtil.commitTransaction(conn, "Run Id is Committed");	
				asyncServiceUpdatePrediction(conn, runStatusDTO.runId, predictionInputDTOs);
			}			
		} catch (Exception | GeneralException ex) {
			logger.error("Error while creating run Id" + ex.toString(), ex);
			PristineDBUtil.close(conn);
		}
		return runStatusDTO;
	}
	
//	public RunStatusDTO updatePredictionTest(Connection conn, String predictionInput) {
//		ObjectMapper mapper = new ObjectMapper();
//		RunStatusDTO runStatusDTO = new RunStatusDTO();		
//		isOnline = true;
//		
//		ExecutionOrder executionOrder = new ExecutionOrder();
//		try {
//			PredictionInputDTO predictionInputDTO;
//			List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
//			predictionInputDTOs =  mapper.readValue(predictionInput, new TypeReference<List<PredictionInputDTO>>(){});
//			if(predictionInputDTOs.size() > 0){
//				//pick first in the list and use it in prediction run header
//				predictionInputDTO = predictionInputDTOs.get(0);
//				predictionInputDTO.runType = PRConstants.RUN_TYPE_ONLINE;
//				runStatusDTO.runId = createRunId(conn, predictionInputDTO);
//				PristineDBUtil.commitTransaction(conn, "Run Id is Committed");	
//				asyncServiceUpdatePrediction(conn, runStatusDTO.runId, predictionInputDTOs);
//			}					
//		} catch (Exception | GeneralException ex) {
//			logger.error("Error while creating run Id" + ex.toString(), ex);
//			PristineDBUtil.close(conn);
//		}
//		return runStatusDTO;
//	}
//	
//	public String predictSubstituteImpactTest(Connection conn, String predictionInput) {
//		logger.debug("Inside predictSubstituteImpact()");
//		ObjectMapper mapper = new ObjectMapper();
//		String jsonOutput = "";
//		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
//		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
//		try {
//			PredictionInputDTO predictionInputDTO = mapper.readValue(predictionInput, PredictionInputDTO.class);		 
//			logger.debug("PredictionInputDTO Item Size: " + predictionInputDTO.predictionItems.size());
//			this.isSubstitueImpact = true;
//			
//			List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
//			predictionInputDTOs.add(predictionInputDTO);
//			
//			predict(conn, 0, predictionInputDTOs, executionTimeLogs, "PREDICTION_SUBSTITUTE");
//			jsonOutput = mapper.writeValueAsString(predictionInputDTO);
//			pricingEngineDAO.insertExecutionTimeLog(conn, getRecommendationRunHeader(predictionInputDTO), executionTimeLogs);
//			PristineDBUtil.commitTransaction(conn, "Transaction is Committed");	
//		} catch (Exception | GeneralException | OfferManagementException ex) {
//			logger.error("Error in predictSubstituteImpact" + ex.toString(), ex);
//			PristineDBUtil.close(conn);
//		}
//		return jsonOutput;
//	}
//	
//	public RunStatusDTO predictMovementOnDemandTest(Connection conn, String predictionInput) {
//		logger.debug("Inside predictMovementOnDemand()");
//		ObjectMapper mapper = new ObjectMapper();
//		RunStatusDTO runStatusDTO = new RunStatusDTO();		
//		
//		isOnline = true;
//		 
//		@SuppressWarnings("unused")
//		ExecutionOrder executionOrder = new ExecutionOrder();
//		try {
//			PredictionInputDTO predictionInputDTO;
//			List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
//			//PredictionInputDTO predictionInputDTO = mapper.readValue(predictionInput, PredictionInputDTO.class);	
//			predictionInputDTOs =  mapper.readValue(predictionInput, new TypeReference<List<PredictionInputDTO>>(){});
//			if(predictionInputDTOs.size() > 0){
//				//pick first in the list and use it in prediction run header
//				predictionInputDTO = predictionInputDTOs.get(0);
//				predictionInputDTO.runType = PRConstants.RUN_TYPE_ONLINE;
//				runStatusDTO.runId = createRunId(conn, predictionInputDTO);
//				PristineDBUtil.commitTransaction(conn, "Run Id is Committed");	
//				asyncServiceMethod(conn, runStatusDTO.runId, predictionInputDTOs);
//			}		
//		} catch (Exception | GeneralException ex) {
//			logger.error("Error while creating run Id" + ex.toString(), ex);
//			PristineDBUtil.close(conn);
//		}
//		return runStatusDTO;
//	}
	
	private void asyncServiceMethod(final Connection conn, final long predictionRunHeaderId, 
			final List<PredictionInputDTO> predictionInputDTOs, final boolean isGetOnlyExplain){
		Runnable task = new Runnable() {
            @Override
            public void run() {
            	try {
            		logger.debug("Inside asyncServiceMethod()"); 
            		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
            		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
            		//Assumed the start calendar id will be same for all prediction Input DTO's
            		//Get distinct products
					if (predictionInputDTOs.size() > 0) {
						HashMap<String, String> distinctProducts = getDistinctProducts(predictionInputDTOs);

						// Run product by product
						for (Map.Entry<String, String> product : distinctProducts.entrySet()) {
							List<PredictionInputDTO> groupedByProductDTOs = new ArrayList<PredictionInputDTO>();
							for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
								if (product.getKey().split("_")[0].equals(String.valueOf(predictionInputDTO.productLevelId))
										&& product.getKey().split("_")[1].equals(String.valueOf(predictionInputDTO.productId))) {
									groupedByProductDTOs.add(predictionInputDTO);
								}
							}
							if(isGetOnlyExplain)
								predict(conn, predictionRunHeaderId, groupedByProductDTOs, executionTimeLogs, "EXPLAIN_PREDICTION", true);
							else
								predict(conn, predictionRunHeaderId, groupedByProductDTOs, executionTimeLogs, "PREDICTION_ON_DEMAND", false);

							if (groupedByProductDTOs.size() > 0) {
								// Take the first dto to fill location, product, calendar for execution time log
								pricingEngineDAO.insertExecutionTimeLog(conn, getRecommendationRunHeader(groupedByProductDTOs.get(0)),
										executionTimeLogs);
							}
							executionTimeLogs = new ArrayList<ExecutionTimeLog>();
						}
					}
            		updateRunStatusAsComplete(conn, predictionRunHeaderId);
					PristineDBUtil.commitTransaction(conn, "Transaction is Committed");					
				} catch (Exception | GeneralException | OfferManagementException e) {
					logger.error("Error in asyncServiceMethod() -- " + e.toString(), e);
					PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");
					logger.error("Transaction is Rollbacked -- " + e.toString(), e);					 	
					try {
						updateRunStatusAsFailed(conn, predictionRunHeaderId);
						PristineDBUtil.commitTransaction(conn, "Update Fail Status");
					} catch (Exception | GeneralException ex) {
						logger.error("Error while updating run status as failed" + ex.toString(), e);
					}
				} finally {
					PristineDBUtil.close(conn);					 
				}
            }
        };
        new Thread(task, "ServiceThread").start(); 
	}
	
	private void asyncServiceUpdatePrediction(final Connection conn, final long predictionRunHeaderId, 
			final List<PredictionInputDTO> predictionInputDTOFromUi){
		Runnable task = new Runnable() {
            @Override
            public void run() {
            	try {
            		logger.debug("Inside asyncServiceUpdatePrediction()"); 
            		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
            		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
            		List<PredictionInputDTO> predictionInputDTOs;
            		PricingEngineService pricingEngineService = new PricingEngineService();
            		//Get Items whose prediction to be updated from recommendation table(Each zone and Each Store by Store inside it)      
            		//update if it is zone and there is store recommendation
            		//Ignore LIG            		
            		//Fill prediction input
            		//Mark items having store recommendation
            		PredictionDAO predictionDAO = new PredictionDAO();
            		predictionInputDTOs = predictionDAO.getItemsForWhichPredictionToBeUpdated(conn, predictionInputDTOFromUi);
            		
            		//Assumed the start calendar id will be same for all prediction Input DTO's
            		//Get distinct products
					if (predictionInputDTOs.size() > 0) {
						HashMap<String, String> distinctProducts = getDistinctProducts(predictionInputDTOs);

						// Run product by product
						for (Map.Entry<String, String> product : distinctProducts.entrySet()) {
							List<PredictionInputDTO> groupedByProductDTOs = new ArrayList<PredictionInputDTO>();
							for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
								if (product.getKey().split("_")[0].equals(String.valueOf(predictionInputDTO.productLevelId))
										&& product.getKey().split("_")[1].equals(String.valueOf(predictionInputDTO.productId))) {
									groupedByProductDTOs.add(predictionInputDTO);
								}
							}
							predict(conn, predictionRunHeaderId, groupedByProductDTOs, executionTimeLogs, "PREDICTION_ON_DEMAND", false);
							for (PredictionInputDTO groupedByProdcutDTO : groupedByProductDTOs) {
								HashMap<ItemKey, PRItemDTO> overrideItemsZone = new HashMap<ItemKey, PRItemDTO>();
								// Update Recommendation (zone, store, lig) and Recommendation Header
								if (groupedByProdcutDTO.locationLevelId == Constants.STORE_LEVEL_ID) {
									
									predictionDAO.updateOverridePredictionStatusStore(conn, groupedByProdcutDTO.recommendationRunId,
											groupedByProdcutDTO.locationId);
								}
								else {
									//Update override prediction to recommendation table
									if (groupedByProdcutDTO.predictionItems != null
											&& groupedByProdcutDTO.predictionItems.size() > 0) {
										predictionDAO.updateOverridePricePredictionZone(conn,
												groupedByProdcutDTO.recommendationRunId, groupedByProdcutDTO);
										//Get existing data
										overrideItemsZone = predictionDAO.getOverrideDataZone(conn, groupedByProdcutDTO.recommendationRunId);
										//find pred mov, margin & sales for lig
										updateLigLevelRecData(overrideItemsZone);
										//update pred mov for lig, rec margin & sales for non-lig, lig & lig mem										
										predictionDAO.updateRecommendationDataZone(conn, overrideItemsZone.values());
										
										//update dashboard values
										pricingEngineService.updateDashboard(conn, groupedByProdcutDTO.locationLevelId,
												groupedByProdcutDTO.locationId, groupedByProdcutDTO.productLevelId,
												groupedByProdcutDTO.productId, groupedByProdcutDTO.recommendationRunId);
									}
									
									predictionDAO.updateOverridePredictionStatusZone(conn, groupedByProdcutDTO.recommendationRunId,
											groupedByProdcutDTO.locationId);
								}

								predictionDAO.updateOverridePredictionStatusInRunHeader(conn, groupedByProdcutDTO.recommendationRunId);
							}
							if (groupedByProductDTOs.size() > 0) {
								// Take the first dto to fill location, product, calendar for execution time log
								pricingEngineDAO.insertExecutionTimeLog(conn, getRecommendationRunHeader(groupedByProductDTOs.get(0)),
										executionTimeLogs);
							}
							executionTimeLogs = new ArrayList<ExecutionTimeLog>();
						}
					}
            		updateRunStatusAsComplete(conn, predictionRunHeaderId);
            		
					PristineDBUtil.commitTransaction(conn, "Transaction is Committed");					
				} catch (Exception | GeneralException | OfferManagementException e) {
					logger.error("Error in asyncServiceUpdatePrediction() -- " + e.toString(), e);
					PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");
					logger.error("Transaction is Rollbacked -- " + e.toString(), e);					 	
					try {
						updateRunStatusAsFailed(conn, predictionRunHeaderId);
						PristineDBUtil.commitTransaction(conn, "Update Fail Status");
					} catch (Exception | GeneralException ex) {
						logger.error("Error while updating run status as failed" + ex.toString(), e);
					}
				} finally {
					PristineDBUtil.close(conn);					 
				}
            }
        };
        new Thread(task, "ServiceThread").start(); 
	}
	
	private void updateLigLevelRecData(HashMap<ItemKey, PRItemDTO> overrideItemsZone){
		LIGConstraint ligConstriant = new LIGConstraint();
		for(Entry<ItemKey, PRItemDTO> item : overrideItemsZone.entrySet()){
			PRItemDTO ligItem = item.getValue();
			//Pick only lig's
			if(item.getKey().getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR){
				//Get all lig members and update the values
				Collection<PRItemDTO> ligMembers = new ArrayList<PRItemDTO>();
				for(PRItemDTO ligMem : overrideItemsZone.values()){
					if(ligMem.getRetLirId() == ligItem.getItemCode()) {
						ligMembers.add(ligMem);
					}
				}
				ligItem = ligConstriant.sumOverrideRetailPredictionForLIG(ligItem, ligMembers);
				ligItem = ligConstriant.updateOverridePredictionStatus(ligItem, ligMembers);
				ligItem = ligConstriant.sumOverrideRetailSalesDollarForLIG(ligItem, ligMembers);
				ligItem = ligConstriant.sumOverrideRetailMarginDollarForLIG(ligItem, ligMembers);
			}
		}
	}
	
	private void predict(Connection conn, long predictionRunHeaderId, List<PredictionInputDTO> predictionInputDTOs,
			List<ExecutionTimeLog> executionTimeLogs, String predictionType, boolean isGetOnlyExplain) 
			throws Exception, GeneralException, OfferManagementException {
		HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetails;
		PredictionEngineInput predictionEngineInput;
		int productLevelId, productId, startCalendarId, endCalendarId;
		Boolean usePrediction = true;
		boolean isGetAvgMovement = false;
		long recommendationRunId = 0;
		Boolean isForcePrediction = false;
		ItemService itemService = new ItemService(new ArrayList<ExecutionTimeLog>());
		PredictionServiceHelper predictionServiceHelper = new PredictionServiceHelper();
		
		//logger.debug("Inside predict()");
		try {
			boolean isLIGModel = Boolean.parseBoolean(PropertyManager.getProperty("IS_PREDICTION_USING_LIG_MODEL", "FALSE"));
			
			//Following change is done when prediction engine started accepting multiple locations, Also it is decided
			//not to change the way the current program calling the prediction engine
			//Multiple Categories, Multiple Start Date are not supported in the same call, when it comes here
			//the calling program make sure all the predictionInputDTO belongs to one category and one calendar id
			//Take the first item in the collection to get Properties like StartWeekDate, Product Level Id,
			//Product Id, ....
			
			PredictionInputDTO predictionInputDTO = predictionInputDTOs.get(0);
			//Format week start date
			for (PredictionInputDTO prdInpDto : predictionInputDTOs) {
				getWeekStartDate(conn, prdInpDto);	
			}
			
			productLevelId =  predictionInputDTO.productLevelId;
			productId = predictionInputDTO.productId;
			startCalendarId = predictionInputDTO.startCalendarId;
			endCalendarId = predictionInputDTO.startCalendarId;
			usePrediction = predictionInputDTO.usePrediction;
			isGetAvgMovement = predictionInputDTO.isGetAvgMovement;
			isForcePrediction = predictionInputDTO.isForcePrediction;
			//Rec run id is used to find if a category needs prediction or not, since when it comes here there will be only one product id
			//so picking the first recommendation run id will not be a problem
			recommendationRunId = predictionInputDTO.recommendationRunId; 
			
			if(isLIGModel) {
				predictionServiceHelper.addMissingLigMembers(conn, predictionInputDTOs, itemService, isPassMissingLigMembers, ligMap);
			}
			
			//Update certain parameters like Force Prediction, use substitution, start week date at Item Level.
			//As these parameters are give at item level to prediction engine
			//updateHeaderDataToItemLevel(predictionInputDTOs);
			
			// step 2 retrieve stored movement from prediction
			//If the call is only to get the explain, then don't check if the prediction is already done or not,
			// or if the promotion is prediction, as it is not saved in prediction tables
			//if(isForcePrediction || isGetOnlyExplain || isPromotion){
			//22nd Jun 2016, save promotion prediction in to cache
			if(isForcePrediction || isGetOnlyExplain || isSubstitueImpact){
				//If force prediction is true, then don't look for already predicted items,
				//as input items has to be predicted again even it is already predicted
				predictionDetails = new HashMap<PredictionDetailKey, PredictionDetailDTO>();
			}
			else{
				predictionDetails = getAlreadyPredictedItems(conn, productLevelId, productId, startCalendarId, endCalendarId, predictionInputDTOs);
				//logger.info("predict() - # of price points already predicted from cache: " + predictionDetails.size());
			}
			
			// step 3 prepare input data for which we don't have predicted movement
			predictionEngineInput = predictionServiceHelper.findItemsToBePredicted(productLevelId, productId, predictionDetails, predictionInputDTOs, isLIGModel);
			
			if (predictionEngineInput.predictionEngineItems.size() > 0) {
				PredictionEngineOutput peoForExplain = new PredictionEngineOutput();
				peoForExplain.predictionExplain = new ArrayList<PredictionExplain>();
				// step 4 call prediction engine
				updateIfPredictionEngineOrAvgMovToBeUsed(conn, productLevelId, productId, usePrediction,
						isGetAvgMovement, recommendationRunId, predictionEngineInput);
//				if ( conn!= null)
//					PristineDBUtil.close(conn);
				findPredictionForEachItem(conn, predictionRunHeaderId, predictionEngineInput, predictionInputDTOs, executionTimeLogs,
						predictionType, peoForExplain, isGetOnlyExplain, isLIGModel);
//				conn = DBManager.getConnection();
				
				//PP:: 6-Jul-2017 : Adding logic for questionable predictions
				PredictionAnalysis predAnalysis = new PredictionAnalysis(movementData, itemPriceHistory);
				predAnalysis.initiate(conn,predictionInputDTOs);

				// step 5 store predicted results to database
				// Don't store the result for substitute impact or promotion or when its called just to get the explain prediction
				//if (!isSubstitueImpact && !isGetOnlyExplain && !isPromotion){
				//22nd Jun 2016, save promotion 
				if (!isSubstitueImpact && !isGetOnlyExplain && !isPredictionTest){
					storePredictedResult(conn, predictionRunHeaderId, predictionInputDTOs);					
				}
				
				if(isLIGModel) {
					filterInputItems(predictionInputDTOs);
				}
				
				//Store explain log
				if(isGetOnlyExplain)
					storePredictedExplain(conn, predictionRunHeaderId, peoForExplain.predictionExplain);
			}else{
				logger.info("No newer Price Points to Predict");
			}
		} catch (Exception ex) {
			logger.error("Error in predict() -- " + ex.toString(), ex);
			throw new Exception();
		}
	}
	
	private void filterInputItems(List<PredictionInputDTO> predictionInputDTOs) {
		// Send only items which are actually passed
		for (PredictionInputDTO pi : predictionInputDTOs) {
			if (pi.predictionItems != null) {
				// Filter items which are passed
				List<PredictionItemDTO> inputItems = pi.predictionItems.stream().filter(x -> x.isInputItem())
						.collect(Collectors.toList());
				
				//update the list
				pi.predictionItems.clear();
				pi.predictionItems.addAll(inputItems);
				
				for (PredictionItemDTO predInput : pi.predictionItems) {
					if (predInput.pricePoints != null) {
						List<PricePointDTO> inputPricePoints = predInput.pricePoints.stream().filter(x -> x.isInputPricePoint())
								.collect(Collectors.toList());
						
						predInput.pricePoints.clear();
						predInput.pricePoints.addAll(inputPricePoints);
					}
				}
					
			}
		}
	}
	
	private void getWeekStartDate(Connection conn, PredictionInputDTO predictionInputDTO) throws Exception {
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO retailCalendarDTO;
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		try {
			// Fill start date if null
			if (predictionInputDTO.startWeekDate == null) {
				retailCalendarDTO = retailCalendarDAO.getCalendarDetail(conn, predictionInputDTO.startCalendarId);
				predictionInputDTO.startWeekDate = retailCalendarDTO.getStartDate();
			}

			// get period start date
			retailCalendarDTO = retailCalendarDAO.getPeriodDetail(conn, predictionInputDTO.startWeekDate);
			predictionInputDTO.startPeriodDate = retailCalendarDTO.getStartDate();
			predictionInputDTO.startWeekDate = new SimpleDateFormat("yyyy-MM-dd").format(formatter
					.parse(predictionInputDTO.startWeekDate));
			predictionInputDTO.startPeriodDate = new SimpleDateFormat("yyyy-MM-dd").format(formatter
					.parse(predictionInputDTO.startPeriodDate));
		} catch (GeneralException ex) {
			logger.error("Error in fillWeekStartDate() -- " + ex.toString(), ex);
			throw new Exception();
		}
	}
	
	private long createRunId(Connection conn, PredictionInputDTO predictionInputDTO) throws Exception{
		logger.debug("Inside createRunId()");
		PredictionDAO predictionDAO = new PredictionDAO();
		long predictionRunHeaderId;
		PredictionRunHeaderDTO predictionRunHeaderDTO = new PredictionRunHeaderDTO();			
		
		predictionRunHeaderDTO.setRunType(String.valueOf(predictionInputDTO.runType));
		predictionRunHeaderDTO.setPredictedBy(predictionInputDTO.predictedBy);
		predictionRunHeaderDTO.setStartCalendarId(predictionInputDTO.startCalendarId);
		predictionRunHeaderDTO.setEndCalendarId(predictionInputDTO.endCalendarId);
		//predictionRunHeaderDTO.setLocationLevelId(predictionInputDTO.locationLevelId);
		//predictionRunHeaderDTO.setLocationId(predictionInputDTO.locationId);
		
		try {
			predictionRunHeaderId = predictionDAO.insertPredictionRunHeader(conn, predictionRunHeaderDTO);		 
		} catch (GeneralException e) {
			predictionRunHeaderId = -1;
			logger.error("Error in createRunId() -- " + e.toString(), e);
			throw new Exception();
		}
		return predictionRunHeaderId;
	}
	
	private HashMap<PredictionDetailKey, PredictionDetailDTO> getAlreadyPredictedItems(Connection conn,
			int productLevelId, int productId, int startCalendarId, int endCalendarId, 
			List<PredictionInputDTO> predictionInputDTOs) throws Exception{
		//logger.debug("Inside getAlreadyPredictedItems()");
		PredictionDAO predictionDAO = new PredictionDAO();
		HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetails = new HashMap<PredictionDetailKey, PredictionDetailDTO>();
		
		try {
			predictionDetails = predictionDAO.getSucessfullPredictedMovement(conn, productLevelId, productId, startCalendarId, 
					endCalendarId, predictionInputDTOs, isOnline);
		} catch (GeneralException e) {
			logger.error("Error in getAlreadyPredictedItems() -- " + e.toString(), e);
			throw new Exception();
		}
		return predictionDetails;
	}
	
//	private void fillPredictionEngineInput(PredictionEngineInput predictionEngineInput,
//			PredictionInputDTO predictionInputDTO) throws Exception{
//		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
//		
//		predictionEngineInput.setProductLevelId(predictionInputDTO.productLevelId);  
//		predictionEngineInput.setProductId(predictionInputDTO.productId);  
//		predictionEngineInput.setLocationLevelId(predictionInputDTO.locationLevelId);  
//		predictionEngineInput.setLocationId(predictionInputDTO.locationId);  
//		predictionEngineInput.setUseSubstFlag(predictionInputDTO.useSubstFlag);
//		try {
//			predictionEngineInput.setPredictionStartDateStr(
//					new SimpleDateFormat("yyyy-MM-dd").format(formatter.parse(predictionInputDTO.startWeekDate)));
//			//start day of week i.e. sunday
//		} catch (ParseException e) {
//			logger.error("Error in fillPredictionEngineInput() -- " + e.toString(), e);
//			throw new Exception();
//		} 
//	}
	
	private void copyPredictionEngineInput(PredictionEngineInput predictionEngineInputSrc,
			PredictionEngineInput predictionEngineInputSrcDest){
		 
		predictionEngineInputSrcDest.setProductLevelId(predictionEngineInputSrc.getProductLevelId());  
		predictionEngineInputSrcDest.setProductId(predictionEngineInputSrc.getProductId());  
		predictionEngineInputSrcDest.setIsParallel(predictionEngineInputSrc.getIsParallel());
		//predictionEngineInputSrcDest.setLocationLevelId(predictionEngineInputSrc.getLocationLevelId());  
		//predictionEngineInputSrcDest.setLocationId(predictionEngineInputSrc.getLocationId());  
		//predictionEngineInputSrcDest.setPredictionStartDateStr(
				//predictionEngineInputSrc.getPredictionStartDateStr());
		//predictionEngineInputSrcDest.setUseSubstFlag(predictionEngineInputSrc.getUseSubstFlag());
		
	}	
	
	//Find price points which is not already predicted in PredictionEngineInput
	//Update price points which is not already predicted
	@SuppressWarnings("unused")
	private PredictionEngineInput findItemsToBePredicted(int productLevelId, int productId,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetails, List<PredictionInputDTO> predictionInputDTOs) throws Exception {
		logger.debug("Inside findItemsToBePredicted()");
		// PredictionInputDTO itemsToBePredicted = new PredictionInputDTO();
		PredictionEngineInput predictionEngineInput = new PredictionEngineInput();
		List<PredictionEngineItem> itemList = new ArrayList<PredictionEngineItem>();
		PredictionDetailKey predictionDetailKey;
		try {
			// fillPredictionEngineInput(predictionEngineInput, predictionInputDTO);
			predictionEngineInput.setProductLevelId(productLevelId);
			predictionEngineInput.setProductId(productId);
			// If more than one location presents then set parallel to true
			if (predictionInputDTOs.size() > 1) {
				predictionEngineInput.setIsParallel(String.valueOf(Constants.YES));
			}

			for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
				// Check if items are present
				if (predictionInputDTO.predictionItems != null) {
					// Check if item is already predicted, if not then keep it in PredictionEngineItem
					for (PredictionItemDTO predictionItemDTO : predictionInputDTO.predictionItems) {
						if (predictionItemDTO.pricePoints != null) {
							for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
								predictionDetailKey = new PredictionDetailKey(predictionInputDTO.locationLevelId,
										predictionInputDTO.locationId, predictionItemDTO.itemCodeOrLirId, 
										pricePointDTO.getRegQuantity(), pricePointDTO.getFormattedRegPrice(),
										pricePointDTO.getSaleQuantity(), pricePointDTO.getFormattedSalePrice(),
										pricePointDTO.getAdPageNo(), pricePointDTO.getAdBlockNo(),
										pricePointDTO.getPromoTypeId(), pricePointDTO.getDisplayTypeId());

								// predictionDetailKey.setLirOrItemCode(predictionItemDTO.itemCodeOrLirId);
								// predictionDetailKey.setRegQuantity(pricePointDTO.getRegQuantity());
								// Format reg price as data in database will be always with 2 digit, but input price may
								// vary
								// predictionDetailKey.setRegPrice(pricePointDTO.getFormattedRegPrice());

								// If already predicted, update flag, movement,explain
								if (predictionDetails.get(predictionDetailKey) != null) {
									pricePointDTO.setIsAlreadyPredicted(true);
									pricePointDTO.setPredictedMovement(Double.valueOf(predictionDetails.get(predictionDetailKey)
											.getPredictedMovement()));
									pricePointDTO.setExplainPrediction(predictionDetails.get(predictionDetailKey).getExplainPrediction());
									pricePointDTO.setPredictionStatus(PredictionStatus.get(predictionDetails.get(predictionDetailKey)
											.getPredictionStatus()));
									pricePointDTO.setQuestionablePrediction(predictionDetails.get(predictionDetailKey).getQuestionablePrediction());
								} else {
									// Pass formatted reg price to prediction engine
									pricePointDTO.setIsAlreadyPredicted(false);
									// itemList.add(new PredictionEngineItem(PRConstants.PREDICTION_DEAL_TYPE,
									// predictionItemDTO.itemCodeOrLirId, Long.parseLong(predictionItemDTO.upc), pricePointDTO
									// .getFormattedRegPrice(), pricePointDTO.getRegQuantity(),
									// predictionItemDTO.mainOrImpactFlag, predictionItemDTO.usePrediction,
									// predictionItemDTO.isAvgMovAlreadySet));
									itemList.add(new PredictionEngineItem(predictionInputDTO.locationLevelId,
											predictionInputDTO.locationId, predictionInputDTO.startWeekDate,
											PRConstants.PREDICTION_DEAL_TYPE, predictionItemDTO.itemCodeOrLirId, 
											predictionItemDTO.upc, pricePointDTO.getFormattedRegPrice(),
											pricePointDTO.getRegQuantity(),
											(predictionInputDTO.useSubstFlag.equals(String.valueOf(Constants.YES)) ? Constants.YES : Constants.NO),
											predictionItemDTO.mainOrImpactFlag, predictionItemDTO.usePrediction,
											predictionItemDTO.isAvgMovAlreadySet, predictionInputDTO.startPeriodDate, 
											pricePointDTO.getSaleQuantity(), pricePointDTO.getFormattedSalePrice(), pricePointDTO.getAdPageNo(),
											pricePointDTO.getAdBlockNo(), pricePointDTO.getPromoTypeId(), pricePointDTO.getDisplayTypeId(),
											predictionItemDTO.subsScenarioId, predictionItemDTO.predictedMovement, predictionItemDTO.predictionStatus,
											predictionItemDTO.getRetLirId()));
								}
							}
						}
					}
				}
			}
			//logger.debug("No of price points to be predicted:" + itemList.size());
			predictionEngineInput.setPredictionEngineItems(itemList);
		}
		catch (Exception e) {
			logger.error("Error in findItemsToBePredicted() -- " + e.toString(), e);
			throw new Exception();
		}
		return predictionEngineInput;
	}
	
	private void findPredictionForEachItem(Connection conn, long predictionRunHeaderId, PredictionEngineInput predictionEngineInput, 
			List<PredictionInputDTO> predictionInputDTOs, List<ExecutionTimeLog> executionTimeLogs, String predictionType,
			PredictionEngineOutput peo, boolean isGetOnlyExplain, boolean isLIGModel) throws Exception{
		//logger.debug("Inside callPredictionEngine()");
		
		int ignoreProductIdItemCnt = Integer.parseInt(PropertyManager.getProperty("PREDICTION_IGNORE_PRODUCT_ID_ITEM_COUNT"));
		PredictionServiceHelper predictionServiceHelper = new PredictionServiceHelper();
		
		String rScriptPath = "", rSrcPath = "";
		if (isPromotion) {
			rScriptPath = PropertyManager.getProperty("PROMO_PREDICTION_R_SCRIPT_PATH");
			rSrcPath = PropertyManager.getProperty("PROMO_PREDICTION_R_SRC_PATH", "");
		} else if (isSubstitueImpact) {
			rScriptPath = PropertyManager.getProperty("SUBS_ADJ_R_ROOT_PATH");
			rSrcPath = PropertyManager.getProperty("SUBS_ADJ_R_SRC_PATH", "");
		} else {
			rScriptPath = PropertyManager.getProperty("PREDICTION_R_SCRIPT_PATH");
			rSrcPath = PropertyManager.getProperty("PREDICTION_R_SRC_PATH", "");
		}
		
		//PredictionEngine p = new PredictionEngineBatchImpl(rScriptPath, rSrcPath, ignoreProductIdItemCnt, isOnline);
		PredictionEngine p = new PredictionEngineImpl(rScriptPath, rSrcPath, ignoreProductIdItemCnt, isOnline);
		logger.debug("PREDICTION_R_SCRIPT_PATH: " + rScriptPath);
		int predictionEngineBatchCount = Integer.parseInt(PropertyManager.getProperty("PREDICTION_ENGINE_BATCH_COUNT"));
		
    	//PredictionEngineOutput peo = new PredictionEngineOutput();     
    	PredictionEngineInput finalPredictionEngineInput;
    	PredictionEngineOutput peoAll = new PredictionEngineOutput();
    	PredictionEngineKey predictionEngineKey;
    	
    	finalPredictionEngineInput = new PredictionEngineInput();
		copyPredictionEngineInput(predictionEngineInput, finalPredictionEngineInput);
		finalPredictionEngineInput.predictionEngineItems = new ArrayList<PredictionEngineItem>();
		
		peoAll.predictionMap = new HashMap<PredictionEngineKey, PredictionEngineValue>();
		peoAll.predictionExplain = new ArrayList<PredictionExplain>();
		
		HashSet<Integer> totalDistinctItems = new HashSet<Integer>();
		for (int i = 0; i < predictionEngineInput.predictionEngineItems.size(); i++) {
			if (predictionEngineInput.predictionEngineItems.get(i).usePrediction) {
				totalDistinctItems.add(predictionEngineInput.predictionEngineItems.get(i).itemCode);
			}
		}
		logger.debug("Total Distinct Items to be predicted: " + totalDistinctItems.size());
		try {
			//if (isOnline)
				//predictionEngineBatchCount = 0;
			
			if (predictionEngineBatchCount > 0) {
				
				if (isLIGModel) {
					HashMap<Integer, PredictionEngineInput> itemInBatches = predictionServiceHelper.breakItemsToBatches(predictionEngineBatchCount,
							predictionEngineInput);

					for (Map.Entry<Integer, PredictionEngineInput> itemsInBatch : itemInBatches.entrySet()) {
						
						int noOfItems = (int) itemsInBatch.getValue().getPredictionEngineItems().stream()
								.mapToInt(PredictionEngineItem::getItemCode).distinct().count();
						
						logger.info("findItemsToBePredicted() - Weekly Prediction Batch: " + itemsInBatch.getKey() + ", Items: "
								+ noOfItems + ", Price Points: "
								+ itemsInBatch.getValue().getPredictionEngineItems().size());
						callREngine(conn, predictionRunHeaderId, p, itemsInBatch.getValue(), peoAll, executionTimeLogs, predictionType,
								isGetOnlyExplain);
						
						logger.info("findItemsToBePredicted() - Weekly Prediction Batch " + itemsInBatch.getKey() + " ends.");
					}
				} else {
					int itemCount = 0;
					HashSet<Integer> tempItemCodes = new HashSet<Integer>();
					// Split the items and call in batch mode
					for (Integer itemCode : totalDistinctItems) {
						tempItemCodes.add(itemCode);

						// Total item count more than the batch count
						if ((itemCount + 1) % predictionEngineBatchCount == 0) {
							// Add the items
							for (PredictionEngineItem pei : predictionEngineInput.predictionEngineItems) {
								if (tempItemCodes.contains(pei.itemCode)) {
									finalPredictionEngineInput.predictionEngineItems.add(pei);
								}
							}
							//logger.debug("Total No of items in batch mode: " + tempItemCodes.size());
							callREngine(conn,predictionRunHeaderId, p, finalPredictionEngineInput, peoAll, executionTimeLogs, predictionType,
									isGetOnlyExplain);
							tempItemCodes.clear();
							finalPredictionEngineInput = new PredictionEngineInput();
							copyPredictionEngineInput(predictionEngineInput, finalPredictionEngineInput);
							finalPredictionEngineInput.predictionEngineItems = new ArrayList<PredictionEngineItem>();
						}

						if (((totalDistinctItems.size() - itemCount) < predictionEngineBatchCount)
								&& ((itemCount + 1) == totalDistinctItems.size())) {
							// Add the items
							for (PredictionEngineItem pei : predictionEngineInput.predictionEngineItems) {
								if (tempItemCodes.contains(pei.itemCode)) {
									finalPredictionEngineInput.predictionEngineItems.add(pei);
								}
							}
							//logger.debug("Remaning No of items in batch mode: " + tempItemCodes.size());
							callREngine(conn,predictionRunHeaderId, p, finalPredictionEngineInput, peoAll, executionTimeLogs, predictionType,
									isGetOnlyExplain);
						}
						itemCount++;
					}
				}
				
				// // Split the predictionEngineInput and call in batch mode
				/*for (int i = 0; i < predictionEngineInput.predictionEngineItems.size(); i++) {
					// Items which needs prediction
					if (predictionEngineInput.predictionEngineItems.get(i).usePrediction) {
						finalPredictionEngineInput.predictionEngineItems.add(predictionEngineInput.predictionEngineItems.get(i));
						if ((i + 1) % predictionEngineBatchCount == 0) {
							logger.debug("Total No of price point in bacth mode: " + (i + 1));
							callREngine(conn, predictionRunHeaderId, p, finalPredictionEngineInput, peoAll, executionTimeLogs, predictionType,
									isGetOnlyExplain);
							finalPredictionEngineInput = new PredictionEngineInput();
							copyPredictionEngineInput(predictionEngineInput, finalPredictionEngineInput);
							finalPredictionEngineInput.predictionEngineItems = new ArrayList<PredictionEngineItem>();
						}
						// Remaining records, (on last records
						if (((predictionEngineInput.predictionEngineItems.size() - i) < predictionEngineBatchCount)
								&& ((i + 1) == predictionEngineInput.predictionEngineItems.size())) {
							logger.debug("Remaning price point in bacth mode: " + (i + 1));
							callREngine(conn, predictionRunHeaderId, p, finalPredictionEngineInput, peoAll, executionTimeLogs, predictionType,
									isGetOnlyExplain);
						}
					}
				}*/
			} else {
				// Get Prediction from Prediction Engine
				for (int i = 0; i < predictionEngineInput.predictionEngineItems.size(); i++) {
					// Items which needs prediction
					if (predictionEngineInput.predictionEngineItems.get(i).usePrediction) {
						finalPredictionEngineInput.predictionEngineItems.add(predictionEngineInput.predictionEngineItems.get(i));
					}
				}
				//logger.debug("No of Price Points for Prediction: " + finalPredictionEngineInput.predictionEngineItems.size());
				if (finalPredictionEngineInput.predictionEngineItems.size() > 0) {
					callREngine(conn,predictionRunHeaderId, p, finalPredictionEngineInput, peoAll, executionTimeLogs, predictionType,
							isGetOnlyExplain);
					if (peoAll.predictionExplain != null) {
						//logger.debug("Explain log size1:" + peoAll.predictionExplain.size());
						peo.predictionExplain = peoAll.predictionExplain;
						//logger.debug("Explain log size3:" + peo.predictionExplain.size());
					}
				}
			}

			//Commented below codes to support batch processing, as r program not able to handle over a certain no of items
			//Get Prediction from Prediction Engine
			/*for (int i = 0; i < predictionEngineInput.predictionEngineItems.size(); i++) {
				// Items which needs prediction
				if (predictionEngineInput.predictionEngineItems.get(i).usePrediction) {
					finalPredictionEngineInput.predictionEngineItems.add(predictionEngineInput.predictionEngineItems.get(i));
				}
			}
			logger.debug("No of Items for Prediction: " + finalPredictionEngineInput.predictionEngineItems.size());
			if (finalPredictionEngineInput.predictionEngineItems.size() > 0) {
				callREngine(conn, predictionRunHeaderId, p, finalPredictionEngineInput, peoAll, 
						executionTimeLogs, predictionType, isGetOnlyExplain);
				if(peoAll.predictionExplain != null){
					logger.debug("Explain log size1:" + peoAll.predictionExplain.size());
					peo.predictionExplain = peoAll.predictionExplain;
					logger.debug("Explain log size3:" + peo.predictionExplain.size());
				}
			}*/
			
			//Get average movement
			finalPredictionEngineInput = new PredictionEngineInput();
			copyPredictionEngineInput(predictionEngineInput, finalPredictionEngineInput);
			finalPredictionEngineInput.predictionEngineItems = new ArrayList<PredictionEngineItem>();
			String itemsForAvgMov = "";
			for (int i = 0; i < predictionEngineInput.predictionEngineItems.size(); i++) {
				// Items which needs avg movement
				if (!predictionEngineInput.predictionEngineItems.get(i).usePrediction) {
					itemsForAvgMov = itemsForAvgMov + "," + predictionEngineInput.predictionEngineItems.get(i).itemCode;
					finalPredictionEngineInput.predictionEngineItems.add(predictionEngineInput.predictionEngineItems.get(i));
				}
			}
			if(finalPredictionEngineInput.predictionEngineItems.size() > 0){
				/*
				 * logger.debug("No of Item for Avg Mov: " +
				 * finalPredictionEngineInput.predictionEngineItems.size());
				 * logger.debug("Items sent for Avg Mov: " + itemsForAvgMov);
				 */
				getAverageMovement(conn, finalPredictionEngineInput, predictionInputDTOs);
			}
			
			// logger.debug("Total Items Predicted:" +
			// peoAll.predictionMap.size());

			// Update movement in predictionInputDTO
			// Loop PredictionItemDTO
			for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
				for (PredictionItemDTO predictionItemDTO : predictionInputDTO.predictionItems) {
					
					if(predictionItemDTO.pricePoints == null) {
						logger.error("*** - Price points list is null for -> " + predictionItemDTO.toString());
					}else
					// Loop each price point
					for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
						// If not already predicted
						if (!pricePointDTO.getIsAlreadyPredicted()) {
							// Get it from PredictionEngineOutput
//							predictionEngineKey = new PredictionEngineKey(predictionInputDTO.locationLevelId,
//									predictionInputDTO.locationId, Double.valueOf(Long.parseLong(predictionItemDTO.upc)), 
//									pricePointDTO.getFormattedRegPrice());
							
							predictionEngineKey = new PredictionEngineKey(predictionInputDTO.locationLevelId,
									predictionInputDTO.locationId, predictionItemDTO.itemCodeOrLirId,
									pricePointDTO.getRegQuantity(), pricePointDTO.getFormattedRegPrice(),
									pricePointDTO.getSaleQuantity(), pricePointDTO.getFormattedSalePrice(),
									pricePointDTO.getAdPageNo(), pricePointDTO.getAdBlockNo(),
									pricePointDTO.getPromoTypeId(), pricePointDTO.getDisplayTypeId());

							if (peoAll.predictionMap.get(predictionEngineKey) != null) {
								//8th Apr 2016, don't round predicted movement
//								pricePointDTO.setPredictedMovement(Double.valueOf(Math.round(peoAll.predictionMap
//										.get(predictionEngineKey).predictedMovement)));
								pricePointDTO.setPredictedMovement(peoAll.predictionMap.get(predictionEngineKey).predictedMovement);

								if (peoAll.predictionMap.get(predictionEngineKey).predictionStatus != null)
									pricePointDTO.setPredictionStatus(peoAll.predictionMap.get(predictionEngineKey).predictionStatus);
								else
									pricePointDTO.setPredictionStatus(PredictionStatus.UNDEFINED);

								pricePointDTO.setSubstituteImpactMax(peoAll.predictionMap.get(predictionEngineKey).maxImpact);
								pricePointDTO.setSubstituteImpactMin(peoAll.predictionMap.get(predictionEngineKey).minImpact);
								pricePointDTO.setConfidenceLevelLower(peoAll.predictionMap.get(predictionEngineKey).confidenceLevelLower);
								pricePointDTO.setConfidenceLevelUpper(peoAll.predictionMap.get(predictionEngineKey).confidenceLevelUpper);
								pricePointDTO.setPredictedMovementBeforeSubsAdjustment(
										peoAll.predictionMap.get(predictionEngineKey).predictedMovementBeforeSubsAdjustment);
							}
						}
					}
				}
			}
			
		} catch (Exception e) {
			logger.error("Error in callPredictionEngine() -- " + e.toString(), e);
			throw new Exception();
		}
	}

	private void callREngine(Connection conn, long predictionRunHeaderId, PredictionEngine p,
			PredictionEngineInput predictionEngineInputBatch, PredictionEngineOutput peoAll, 
			List<ExecutionTimeLog> executionTimeLogs, String predictionType, boolean isGetOnlyExplain) throws Exception {

		PredictionEngineOutput peo = new PredictionEngineOutput();
		PredictionEngineKey predictionEngineKey;
		ExecutionTimeLog executionTimeLog;
		
		/*logger.debug("******* Items sent for Prediction Engine ******* ");
		for (PredictionEngineItem pei : predictionEngineInputBatch.predictionEngineItems) {
			logger.debug("Prediction Items : Item Code: " + pei.itemCode
					+ " UPC: " + pei.upc + ", Price: " + pei.regPrice);
		}
		logger.debug("******* Items sent for Prediction Engine ******* ");*/
		
		logger.debug("Total Price Points to be predicted: " + predictionEngineInputBatch.predictionEngineItems.size());
		try {
		//	logger.debug("predict() of PredictionEngine is Called");	
			executionTimeLog = getExecutionTimeLog(predictionType, predictionEngineInputBatch.predictionEngineItems.size());
			//peo = p.predict(predictionEngineInputBatch, isGetOnlyExplain, isPromotion, isSubstitueImpact);
			peo = p.weeklyPrediction(predictionEngineInputBatch, isGetOnlyExplain, isPromotion, isSubstitueImpact);
			/*
			 * if(peo !=null && peo.predictionExplain != null){
			 * logger.debug("Explain log size:" + peo.predictionExplain.size()); }
			 */
			executionTimeLog.setEndTime();
			executionTimeLogs.add(executionTimeLog);
			//logger.debug("predict() of PredictionEngine is Completed");
		} catch (PredictionException e) {
			peo = null;
			logger.error("Exception in predict() of PredictionEngine -- " + e.toString(), e);
			//logger.error(e.printStackTrace());
		}

		if(peo == null){
			peo = new PredictionEngineOutput();
			peo.predictionMap = new HashMap<PredictionEngineKey,PredictionEngineValue>();
		}
		logger.debug("Total Items returned from prediction: " + peo.predictionMap.size());
		logger.debug("******* Prediction Results from Prediction Engine ******* ");
		if (peo.predictionMap.size() > 0) {
			/*
			 * for (Entry<PredictionEngineKey, PredictionEngineValue> pm :
			 * peo.predictionMap.entrySet()) { //
			 * logger.debug("Prediction Results : Item Code: " // + pm.getKey().itemCode +
			 * ",Reg Price: " + pm.getKey().regQuantity + "/" + pm.getKey().regPrice // +
			 * ",Sale Price: " + pm.getKey().saleQuantity + "/" + pm.getKey().salePrice // +
			 * ",Ad Page No: " + pm.getKey().adPageNo + ",Ad Block No: " +
			 * pm.getKey().adBlockNo // + ",Display Type Id: " + pm.getKey().displayTypeId +
			 * ",Promo Type Id: " + pm.getKey().promoTypeId // + ", Movement: " +
			 * pm.getValue().predictedMovement // + ", Lower Range: " +
			 * pm.getValue().confidenceLevelLower + ",Upper Range: " +
			 * pm.getValue().confidenceLevelUpper // + ", Status: " +
			 * (pm.getValue().predictionStatus != null ?
			 * pm.getValue().predictionStatus.getStatusCode() : "") // + ",MIN_IMPACT: " +
			 * pm.getValue().minImpact + ",MAX_IMPACT: " + pm.getValue().maxImpact); }
			 */
		} else {
			logger.warn("None of the Items were Predicted ");
		}
		logger.debug("******* Prediction Results from Prediction Engine ******* ");
		
		//Find items that are missed by the prediction engine. i.e. if 10 items are send to
		//prediction engine and if prediction engine returns only 8 items, then mark those
		//two items with flag		
		
		//Loop items that are send to prediction engine
		for (PredictionEngineItem pei : predictionEngineInputBatch.predictionEngineItems) {
			//Check if it is there in prediction engine output
			//predictionEngineKey = new PredictionEngineKey(pei.locationLevelId, pei.locationId, Double.valueOf(pei.upc), pei.regPrice);
			predictionEngineKey = new PredictionEngineKey(pei.locationLevelId, pei.locationId, pei.itemCode, pei.regQuantity,
					pei.regPrice, pei.saleQuantity, pei.salePrice, pei.adPageNo, pei.adBlockNo, pei.promoTypeId, pei.displayTypeId);
			//Prediction engine missed items
			if (peo.predictionMap.get(predictionEngineKey) == null) {
				// PredictionEngineKey pek = new
				// PredictionEngineKey(pei.locationLevelId, pei.locationId,
				// Double.valueOf(pei.upc), pei.regPrice);
				PredictionEngineKey pek = new PredictionEngineKey(pei.locationLevelId, pei.locationId, pei.itemCode,
						pei.regQuantity, pei.regPrice, pei.saleQuantity, pei.salePrice, pei.adPageNo, pei.adBlockNo,
						pei.promoTypeId, pei.displayTypeId);
				PredictionEngineValue pev = new PredictionEngineValue(0, PredictionStatus.PREDICTION_APP_EXCEPTION, "",
						"", 0, 0, 0);
				peo.predictionMap.put(pek, pev);
			}
		}		
		
		peoAll.predictionMap.putAll(peo.predictionMap);
		if(peo.predictionExplain != null)
			peoAll.predictionExplain.addAll(peo.predictionExplain);
	}
	
	private void storePredictedResult(Connection conn, long predictionRunHeaderId, List<PredictionInputDTO> predictionInputDTOs) throws Exception{
		PredictionDAO predictionDAO = new PredictionDAO();
		ArrayList<PredictionDetailDTO> predictionDetailsDTO  = new ArrayList<PredictionDetailDTO>(); 
		//Double itemPrice;
		logger.debug("Inside storePredictedResult()");
		try {
			for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
				// Check if items are present
				if (predictionInputDTO.predictionItems != null) {
					// Loop each predicted item
					for (PredictionItemDTO predictionItemDTO : predictionInputDTO.predictionItems) {
						// Check if item has price points
						if (predictionItemDTO.pricePoints != null) {
							// Loop each price points
							for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
								// If not already saved in the database
								if (!pricePointDTO.getIsAlreadyPredicted()) {
									PredictionDetailDTO predictionDetailDTO = new PredictionDetailDTO();
									predictionDetailDTO.setLirOrItemCode(predictionItemDTO.itemCodeOrLirId);
									predictionDetailDTO.setLocationLevelId(predictionInputDTO.locationLevelId);
									predictionDetailDTO.setLocationId(predictionInputDTO.locationId);

									if (pricePointDTO.getPredictionStatus() != null)
										predictionDetailDTO.setPredictionStatus(pricePointDTO.getPredictionStatus().getStatusCode());
									else
										predictionDetailDTO.setPredictionStatus(PredictionStatus.UNDEFINED.getStatusCode());

									if (predictionItemDTO.lirInd != null) {
										if (predictionItemDTO.lirInd)
											predictionDetailDTO.setLirIndicator('Y');
										else
											predictionDetailDTO.setLirIndicator('N');
									} else {
										predictionDetailDTO.setLirIndicator('N');
									}

									predictionDetailDTO.setRegularPrice(pricePointDTO.getRegPrice());
									predictionDetailDTO.setRegularQuantity(pricePointDTO.getRegQuantity());

									if (pricePointDTO.getPredictedMovement() != null) {
										//8th Apr 2016, don't round predicted movement
										//predictionDetailDTO.setPredictedMovement(Math.round(pricePointDTO.getPredictedMovement()));
										predictionDetailDTO.setPredictedMovement(pricePointDTO.getPredictedMovement()); 
									}
									else {
										predictionDetailDTO.setPredictedMovement(0);
									}

									if (pricePointDTO.getExplainPrediction() != null)
										predictionDetailDTO.setExplainPrediction(pricePointDTO.getExplainPrediction());
									else
										predictionDetailDTO.setExplainPrediction("");
									
									if (pricePointDTO.getConfidenceLevelLower() != null) {
										//8th Apr 2016, don't round predicted movement
										//predictionDetailDTO.setConfidenceLevelLower(Math.round(pricePointDTO.getConfidenceLevelLower()));
										predictionDetailDTO.setConfidenceLevelLower(pricePointDTO.getConfidenceLevelLower());
									}
									else {
										predictionDetailDTO.setConfidenceLevelLower(0);
									}
									
									if (pricePointDTO.getConfidenceLevelUpper() != null) {
										//8th Apr 2016, don't round predicted movement
										//predictionDetailDTO.setConfidenceLevelUpper(Math.round(pricePointDTO.getConfidenceLevelUpper()));
										predictionDetailDTO.setConfidenceLevelUpper(pricePointDTO.getConfidenceLevelUpper());
									} 
									else {
										predictionDetailDTO.setConfidenceLevelUpper(0);
									}
									
									//7-July-2017 : Added for questionable prediction
									if (pricePointDTO.getQuestionablePrediction() != null) {
										String[] quesPred = pricePointDTO.getQuestionablePrediction().split(",");
										Set<String> tempSet = Arrays.stream(quesPred).collect(Collectors.toSet());
										String quesPredReasons = tempSet.stream().filter(val-> val.trim()!=null && !val.trim().isEmpty()).collect(Collectors.joining(","));
										predictionDetailDTO.setQuestionablePrediction(quesPredReasons);
									}
									
									predictionDetailDTO.setSalePrice(pricePointDTO.getSalePrice());
									predictionDetailDTO.setSaleQuantity(pricePointDTO.getSaleQuantity());
									predictionDetailDTO.setAdPageNo(pricePointDTO.getAdPageNo());
									predictionDetailDTO.setBlockNo(pricePointDTO.getAdBlockNo());
									predictionDetailDTO.setDisplayTypeId(pricePointDTO.getDisplayTypeId());
									predictionDetailDTO.setPromoTypeId(pricePointDTO.getPromoTypeId());
									
									predictionDetailsDTO.add(predictionDetailDTO);
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {
			logger.error("Error in storePredictedResult() -- " + e.toString(), e);
			throw new Exception();
		}
		try {
			predictionDAO.insertPredictionDetail(conn, predictionRunHeaderId,
					predictionDetailsDTO);
		} catch (GeneralException e) {
			logger.error("Error in insertPredictionDetail() -- " + e.toString(), e);
			throw new Exception();
		}
	}
	
	private void storePredictedExplain(Connection conn, long predictionRunHeaderId, List<PredictionExplain> predictionExplain) throws Exception{
		PredictionDAO predictionDAO = new PredictionDAO();
		try {
			//logger.debug("Explain log size2:" +  predictionExplain.size());
			predictionDAO.insertPredictionExplain(conn, predictionRunHeaderId, predictionExplain);
		} catch (GeneralException e) {
			logger.error("Error in storePredictedExplain() -- " + e.toString(), e);
			throw new Exception();
		}
	}
	public void convertPredictionInputToOutput(PredictionInputDTO predictionInputDTO,
			PredictionOutputDTO predictionOutputDTO){
		//logger.debug("Inside convertPredictionInputToOutput()");
		
		predictionOutputDTO.locationLevelId = predictionInputDTO.locationLevelId;
		predictionOutputDTO.locationId = predictionInputDTO.locationId;
		predictionOutputDTO.startCalendarId = predictionInputDTO.startCalendarId;
		predictionOutputDTO.endCalendarId = predictionInputDTO.endCalendarId;
		predictionOutputDTO.predictionItems = new ArrayList<PredictionItemDTO>();
		predictionOutputDTO.predictionItems.addAll(predictionInputDTO.predictionItems); 
		
	}
	
	public void updateRunStatusAsComplete(Connection conn, long predictionRunHeaderId) throws Exception{
		//logger.debug("Inside updateRunStatusAsComplete()");
		PredictionDAO predictionDAO = new PredictionDAO();		
		try {
			predictionDAO.updateRunStatusAsComplete(conn, predictionRunHeaderId);		 
		} catch (GeneralException e) {
			predictionRunHeaderId = -1;
			logger.error("Error in updateRunStatusAsComplete() -- " + e.toString(), e);
			throw new Exception();
		}
	}
	
	private void updateRunStatusAsFailed(Connection conn, long predictionRunHeaderId) throws Exception{
		//logger.debug("Inside updateRunStatusAsFailed()");
		PredictionDAO predictionDAO = new PredictionDAO();		
		try {
			predictionDAO.updateRunStatusAsFailed(conn, predictionRunHeaderId);		 
		} catch (GeneralException e) {
			predictionRunHeaderId = -1;
			logger.error("Error in updateRunStatusAsFailed() -- " + e.toString(), e);
			throw new Exception();
		}
	}

//	private void updateMsgAndPCT(Connection conn, long predictionRunHeaderId, String message, int pct) throws Exception{
//		logger.debug("Inside updateMsgAndPCT()");
//		PredictionDAO predictionDAO = new PredictionDAO();		
//		try {
//			predictionDAO.updateRunHeaderMsgAndPCT(conn, predictionRunHeaderId, message, pct);		 
//		} catch (GeneralException e) {			 
//			logger.error("Error in updateMsgAndPCT() -- " + e.toString(), e);			
//		}
//	}
	
	//Update whether to call prediction engine to get avg movement for each item
	private void updateIfPredictionEngineOrAvgMovToBeUsed(Connection conn, int prdLevelId, int prdId, 
			Boolean usePredictionInput, boolean isGetAvgMovement, long recommendationRunId,		 
			PredictionEngineInput predictionEngineInput) throws Exception, OfferManagementException{
		boolean usePred = true;
		//int prdLevelId = predictionInputDTO.productLevelId;
		//int prdId = predictionInputDTO.productId;
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		
		/* If the prediction is called during recommendation, this property would have already found and no
		 * need to find it again 
		 */
		if (usePredictionInput != null) {
			if (usePredictionInput)
				usePred = true;
		}
		//When a zone item has store level recommendation, store level prediction has to be returned.
		//As there is no store level prediction, avg movement will be returned. From ui, zone items
		//for which there is store level recommendation will be sent with below flag
		else if (isGetAvgMovement) {
			usePred = false;
		}
		//Check if recommendationRunId is present or product is defined (will be triggered when on demand prediction is called
		//for categories which requires avg movement
		else if ((recommendationRunId > 0) || (prdLevelId > 0 && prdId > 0)) {
			//recommendationRundId is present
			if(recommendationRunId > 0){
				// Get the product level id and product id from recommendationRundId				
				PRRecommendationRunHeader runHeader =  pricingEngineDAO.getRecommendationRunHeader(conn, recommendationRunId);
				prdLevelId = runHeader.getProductLevelId();
				prdId = runHeader.getProductId();
			}	
			//If product level id or product id is still zero (after getting from recommendationRunId),
			//do nothing which will make prediction to be called
			if(prdLevelId > 0 && prdId > 0){
				// Get the use prediction status	
				List<ProductDTO> productList = new ArrayList<ProductDTO>();
				ProductDTO product = new ProductDTO();
				product.setProductLevelId(prdLevelId);
				product.setProductId(prdId);
				productList.add(product);
				
				//get product property
				List<PRProductGroupProperty> productGroupProperties = pricingEngineDAO.getProductGroupProperties(conn, productList);
				PRProductGroupProperty prProductGroupProperty = PricingEngineHelper.getProductGroupProperty(productGroupProperties,
						product.getProductLevelId(), product.getProductId());
				
				usePred = prProductGroupProperty.getIsUsePrediction();
			}		
		}		
		
		/*When updatePrediction() is called from UI, there may be combination of 
		 * zone items (needs avg mov or prediction depends on configuration), Zone items with store recommendation (needs avg mov)
		 * So this brings the need of finding what to be called (prediction engine or avg mov) at item level
		 */
		//Update either prediction to be called or avg movement to be fetched for each item

		if (predictionEngineInput.predictionEngineItems != null && predictionEngineInput.predictionEngineItems.size() > 0) {
			for (PredictionEngineItem predictionEngineItemDTO : predictionEngineInput.predictionEngineItems) {
				//Give preference if it is already set, this happens when updatePrediction() is called
				if(predictionEngineItemDTO.usePrediction == null){
					predictionEngineItemDTO.usePrediction = usePred;
				}
			}
		}
		
		//return usePrediction;
	}
	
	private void getAverageMovement(Connection conn, PredictionEngineInput predictionEngineInput, 
			List<PredictionInputDTO> predictionInputDTOs) throws Exception{
		logger.debug("Inside getAverageMovement()");
		PredictionInputDTO toBeInserted = new PredictionInputDTO();		
		toBeInserted.predictionItems = new ArrayList<PredictionItemDTO>();
		//HashMap<Integer, PRRecommendation> avgMovFromRecTable =  new HashMap<Integer, PRRecommendation>();
		HashMap<PredictionEngineKey,PredictionEngineValue> avgMovFromRecTable = new HashMap<PredictionEngineKey,PredictionEngineValue>();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		boolean fetchAvgMovFromDb = false;
		PredictionEngineKey predictionEngineKey;
		
		//Check if avg movement is already found, this will happen when updatePrediction() is called,
		//where avg movement is filled while fetching the item itself or during recommendation where
		//avg movement is already available for items
		//Loop each item for which avg movement is needed, check if avg movement need to be fetched from
		//database for any of the item
		for (PredictionEngineItem predictionEngineItemDTO : predictionEngineInput.predictionEngineItems) {
			//Items for which avg mov to be picked from db
			if(predictionEngineItemDTO.isAvgMovAlreadySet == null || !predictionEngineItemDTO.isAvgMovAlreadySet){
				fetchAvgMovFromDb = true;
				break;
			}
		}
		
		//Check if recommendationRunId present (when on demand prediction is done for categories or stores which needs 
		//avg mov instead of prediction movement). As avg mov is already calculated, get the avg movement 
		//from PR_RECOMMENDATION table for location level id other than store.  For store from PR_RECOMMENDATION_STORE (TODO::)
		
		if(fetchAvgMovFromDb){
			if (predictionEngineInput.predictionEngineItems != null
					&& predictionEngineInput.predictionEngineItems.size() > 0) {
				//TODO:: Send only items which needs average movement, instead of reading all the recommendation
				avgMovFromRecTable = pricingEngineDAO.getAllRecommendationItem(conn, predictionInputDTOs);
			} else {
				// If recommendationRunId is not present, the avg movement has to be found on demand,
				// this is not needed as of now, as this situation may not arise
			}
		}

		if (avgMovFromRecTable.size() > 0) {
			for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
				for (PredictionItemDTO predictionItemDTO : predictionInputDTO.predictionItems) {
					// Loop each price point
					for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
						// If not already predicted
						if (!pricePointDTO.getIsAlreadyPredicted()) {
							// Get it from Recommendation table
//							predictionEngineKey = new PredictionEngineKey(predictionInputDTO.locationLevelId,
//									predictionInputDTO.locationId, Double.valueOf(predictionItemDTO.itemCodeOrLirId), 0);
							predictionEngineKey = new PredictionEngineKey(predictionInputDTO.locationLevelId,
									predictionInputDTO.locationId, predictionItemDTO.itemCodeOrLirId, 0, 0, 0, 0, 0, 0, 0, 0);
							if (avgMovFromRecTable.get(predictionEngineKey) != null) {
								//8th Apr 2016, don't round predicted movement
//								pricePointDTO.setPredictedMovement(Double.valueOf(Math.round(
//										avgMovFromRecTable.get(predictionEngineKey).predictedMovement)));
								pricePointDTO.setPredictedMovement(avgMovFromRecTable.get(predictionEngineKey).predictedMovement);
								pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
							}
						}
					}
				}
			}
		}
	}
	
	private ExecutionTimeLog getExecutionTimeLog(String predictionType, int pricePoints){
		ExecutionTimeLog executionTimeLog = null;
		
		if(predictionType.equals("PREDICTION_PRICE_REC"))
			executionTimeLog = new ExecutionTimeLog(PRConstants.PREDICTION_PRICE_REC);
		else if(predictionType.equals("PREDICTION_MAR_OPP"))
			executionTimeLog = new ExecutionTimeLog(PRConstants.PREDICTION_MAR_OPP);
		else if(predictionType.equals("PREDICTION_SALE"))
			executionTimeLog = new ExecutionTimeLog(PRConstants.PREDICTION_SALE);
		else if(predictionType.equals("PREDICTION_ON_DEMAND"))
			executionTimeLog = new ExecutionTimeLog(PRConstants.PREDICTION_ON_DEMAND);
		else if(predictionType.equals("PREDICTION_SUBSTITUTE"))
				executionTimeLog = new ExecutionTimeLog(PRConstants.PREDICTION_SUBSTITUE_IMPACT);
		else if(predictionType.equals("EXPLAIN_PREDICTION"))
			executionTimeLog = new ExecutionTimeLog(PRConstants.PREDICTION_EXPLAIN);
		else if(predictionType.equals("PREDICTION_PROMOTION"))
			executionTimeLog = new ExecutionTimeLog(PRConstants.PREDICTION_EXPLAIN);
		else if(predictionType.equals("PREDICTION_TEST"))
			executionTimeLog = new ExecutionTimeLog(PRConstants.PREDICTION_ON_DEMAND);
		
		executionTimeLog.setPricePoints(pricePoints);
		return executionTimeLog;
	}
	
	private PRRecommendationRunHeader getRecommendationRunHeader(PredictionInputDTO predictionInputDTO) {
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();

		recommendationRunHeader.setRunId(predictionInputDTO.recommendationRunId);
		recommendationRunHeader.setCalendarId(predictionInputDTO.startCalendarId);
		recommendationRunHeader.setLocationLevelId(predictionInputDTO.locationLevelId);
		recommendationRunHeader.setLocationId(predictionInputDTO.locationId);
		recommendationRunHeader.setProductLevelId(predictionInputDTO.productLevelId);
		recommendationRunHeader.setProductId(predictionInputDTO.productId);
		
		return recommendationRunHeader;
	}
	
	/***
	 * Delete substitute items from prediction
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param calendarId
	 * @param substituteGroups
	 * @param itemDataMap
	 * @throws GeneralException
	 */
//	public int deleteSubstituteItemsFromPrediction(Connection conn, int locationLevelId, int locationId,
//			int calendarId, List<PRSubstituteGroup> substituteGroups, HashMap<ItemKey, PRItemDTO> itemDataMap)
//			throws GeneralException {
//		HashSet<Integer> distinctItems = new HashSet<Integer>();
//		List<Integer> items = new ArrayList<Integer>();
//		PredictionDAO predictionDAO = new PredictionDAO();
//
//		for (PRSubstituteGroup subsGroup : substituteGroups) {
//			for (PRSubstituteItem subsItem : subsGroup.getSubtituteItems()) {
//				//Ret lir id
//				if (subsItem.getItemAType() == Constants.PRODUCT_LEVEL_ID_LIG) {
//					for (PRItemDTO item : itemDataMap.values()) {
//						//Get all lig members
//						if(!item.isLir() && item.getRetLirId() == subsItem.getItemAId())
//							distinctItems.add(item.getItemCode());
//					}
//				} else {
//					distinctItems.add(subsItem.getItemAId());
//				}
//
//				//Ret lir id
//				if (subsItem.getItemBType() == Constants.PRODUCT_LEVEL_ID_LIG) {
//					for (PRItemDTO item : itemDataMap.values()) {
//						//Get all lig members
//						if(!item.isLir() && item.getRetLirId() == subsItem.getItemBId())
//							distinctItems.add(item.getItemCode());
//					}
//				} else {
//					distinctItems.add(subsItem.getItemBId());
//				}
//			}
//		}
//
//		items.addAll(distinctItems);
//		int deleteCnt = predictionDAO.deletePrediction(conn, locationLevelId, locationId, calendarId, items);
//		return deleteCnt;
//	}
	
	private HashMap<String, String> getDistinctProducts(List<PredictionInputDTO> predictionInputDTOs){
		HashMap<String, String> distinctProducts = new HashMap<String, String>();
		for(PredictionInputDTO predictionInputDTO : predictionInputDTOs){
			String product = predictionInputDTO.productLevelId + "_" + predictionInputDTO.productId;
			distinctProducts.put(product, "");
		}
		return distinctProducts;
	}
	
	/***
	 * Get already predicted movement for all the price points of the items for a location and calendar
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param startCalendarId
	 * @param endCalendarId
	 * @param items
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<PredictionDetailKey, PredictionDetailDTO> getAlreadyPredictedMovementOfItems(Connection conn,
			int locationLevelId, int locationId, int startCalendarId, int endCalendarId, List<PRItemDTO> items, 
			boolean isConsiderErrorItems)
			throws GeneralException {
		PredictionInputDTO predictionInputDTO = new PredictionInputDTO();
		predictionInputDTO.predictionItems = new ArrayList<PredictionItemDTO>();
		PredictionItemDTO predictionItemDTO = new PredictionItemDTO();
		List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
		PredictionDAO predictionDAO = new PredictionDAO();
		HashMap<PredictionDetailKey, PredictionDetailDTO> predictions = new HashMap<PredictionDetailKey, PredictionDetailDTO>();

		for (PRItemDTO itemDTO : items) {
			// ignore lig representing item
			if (!itemDTO.isLir()){
				predictionItemDTO = new PredictionItemDTO();
				predictionItemDTO.itemCodeOrLirId = itemDTO.getItemCode();
				predictionInputDTO.predictionItems.add(predictionItemDTO);
			}
		}
		predictionInputDTO.locationLevelId = locationLevelId;
		predictionInputDTO.locationId = locationId;
		predictionInputDTOs.add(predictionInputDTO);

		// Get already existing prediction of non-lig, lig members
		if (isConsiderErrorItems)
			predictions = predictionDAO.getAllPredictedMovement(conn, 0, 0, startCalendarId, endCalendarId,
					predictionInputDTOs, true);
		else
			predictions = predictionDAO.getSucessfullPredictedMovement(conn, 0, 0, startCalendarId, endCalendarId,
					predictionInputDTOs, true);

		return predictions;
	}

	/***
	 * Get the average movement from the passed item itself. This function assumes the average movement
	 * is present in the passed item list itself. It converts that to predicted format. As all price point
	 * is going to have same movement, price point in the prediction detail key will be 0 here (i.e. the average
	 * movement will be at item level rather than at price point level
	 * @param conn
	 * @param items
	 * @return
	 */
	public HashMap<PredictionDetailKey, PredictionDetailDTO> getAvgMovementFromItem(int locationLevelId,
			int locationId, List<PRItemDTO> items) {
		HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetails = new HashMap<PredictionDetailKey, PredictionDetailDTO>();
		PredictionDetailDTO predictionDetailDTO;
		PredictionDetailKey predictionDetailKey;

		for (PRItemDTO itemDTO : items) {
			if (!itemDTO.isLir()) {
				predictionDetailDTO = new PredictionDetailDTO();
				predictionDetailKey = new PredictionDetailKey(predictionDetailDTO.getLocationLevelId(),
						predictionDetailDTO.getLocationId(), itemDTO.getItemCode(), 0, 0d, 
						predictionDetailDTO.getSaleQuantity(), predictionDetailDTO.getSalePrice(),
						predictionDetailDTO.getAdPageNo(), predictionDetailDTO.getBlockNo(), 
						predictionDetailDTO.getPromoTypeId(), predictionDetailDTO.getDisplayTypeId());

				predictionDetailDTO.setPredictedMovement(Math.round(itemDTO.getAvgMovement()));
				predictionDetailDTO.setPredictionStatus(PredictionStatus.SUCCESS.getStatusCode());

				predictionDetails.put(predictionDetailKey, predictionDetailDTO);
			}
		}

		return predictionDetails;
	}
	
	/**
	 * Initializes connection. Used when program is accessed through webservice
	 */
	protected void initializeForWS() {
		setConnection(getDSConnection());		
	}
	
	/**
	 * Returns Connection from datasource
	 * @return
	 */
	private Connection getDSConnection() {
		Connection connection = null;
		logger.info("WS Connection - " + PropertyManager.getProperty("WS_CONNECTION"));;
		try{
			if(ds == null){
				initContext = new InitialContext();
				envContext  = (Context)initContext.lookup("java:/comp/env");
				ds = (DataSource)envContext.lookup(PropertyManager.getProperty("WS_CONNECTION"));
			}
			connection = ds.getConnection();
		}catch(NamingException exception){
			logger.error("Error when creating connection from datasource " + exception.toString());
		}catch(SQLException exception){
			logger.error("Error when creating connection from datasource " + exception.toString());
		}
		return connection;
	}
	
	protected Connection getConnection(){
		return conn;
	}
	
	/**
	 * Sets database connection. Used when program runs in batch mode
	 */
	protected void setConnection(){
		if(conn == null){
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}
	
	protected void setConnection(Connection conn){
		this.conn = conn;
	}
	
	public PredictionOutputDTO predictSubstituteImpact(Connection conn, PredictionInputDTO predictionInputDTO,
			List<ExecutionTimeLog> executionTimeLogs) throws Exception, GeneralException, OfferManagementException {
		this.isSubstitueImpact = true;
		PredictionOutputDTO predictionOutputDTO = new PredictionOutputDTO();

		List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
		predictionInputDTOs.add(predictionInputDTO);
		predict(conn, 0, predictionInputDTOs, executionTimeLogs, "PREDICTION_SUBSTITUTE", false);

		// Convert PredictionInputDTO to PredictionOutputDTO
		convertPredictionInputToOutput(predictionInputDTO, predictionOutputDTO);

		return predictionOutputDTO;
	}
	
	
	//Uncompleted function
	public PredictionOutputDTO predictMovement(PredictionInputDTO predictionInputDTO, String predictionType) throws GeneralException {

		logger.debug("Inside predictMovement()");		
		
		PredictionOutputDTO predictionOutputDTO = new PredictionOutputDTO();

		try {		
			
//			this.conn = getNewConnection();
	 
			predictionOutputDTO = predictMovement(this.conn, predictionInputDTO, new ArrayList<ExecutionTimeLog>(), "PREDICTION_PROMOTION", false);
			
			PristineDBUtil.commitTransaction(this.conn, "Commit");
			
		} catch (Exception e) {
			
		} finally {	
			PristineDBUtil.close(this.conn);
		}
		return predictionOutputDTO;
	}
}
