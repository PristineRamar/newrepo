package com.pristine.webservice;

import java.io.IOException;
import java.util.*;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
//import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
//import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dataload.offermgmt.AuditEngineWS;
//import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dataload.offermgmt.StorePriceExportV3;
import com.pristine.dataload.offermgmt.mwr.RecommendationWS;
//import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.RunStatusDTO;
import com.pristine.dto.webservice.Audit;
import com.pristine.dto.webservice.AuditFilter;
import com.pristine.dto.webservice.StorePriceExportSimulation;
import com.pristine.dto.webservice.Strategy;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.AuditService;
import com.pristine.service.offermgmt.PriceAdjustment;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.RerecommendationService;
import com.pristine.service.offermgmt.mwr.core.MWRUpdateRecommendationService;
import com.pristine.service.offermgmt.prediction.*;
//import com.pristine.util.offermgmt.PRCommonUtil;


@Path("/")
public class RecommendationResource  extends BaseResource {
	private static Logger logger = Logger.getLogger("RecommendationResource");
	
	public RecommendationResource() throws GeneralException {
		setLog4jProperties();
	}
	
	@Path("/recommend")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public RunStatusDTO recommendation(String strategy) {
		logger.info("recommend service hit successfully");
		logger.info(strategy.toString());
		RunStatusDTO runStatusDTO = new RunStatusDTO();
		PricingEngineWS pricingEngine = new PricingEngineWS();
		ObjectMapper mapper = new ObjectMapper();
		try {
			Strategy objStrategy = mapper.readValue(strategy, Strategy.class);
			long runId = pricingEngine.getPriceRecommendation(objStrategy.locationLevelId, objStrategy.locationId, objStrategy.productLevelId,
					objStrategy.productId, objStrategy.predictedUserId);
			runStatusDTO.runId = runId;
		} catch (Exception | GeneralException exception) {
			exception.printStackTrace();
			logger.error("Exception in predictStrategy()" + exception.toString(), exception);
			runStatusDTO.runId = -1;
		}
		return runStatusDTO;
	}

	@Path("/strategyWhatIf")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public RunStatusDTO strategyWhatIf(String strategy) {
		logger.info("strategyWhatIf service hit successfully");
		logger.debug(strategy);
		RunStatusDTO runStatusDTO = new RunStatusDTO();
		PricingEngineWS pricingEngine = new PricingEngineWS();
		ObjectMapper mapper = new ObjectMapper();
		try {
			Strategy objStrategy = mapper.readValue(strategy, Strategy.class);
			runStatusDTO.runId = pricingEngine.getPriceRecommendation(objStrategy);
		} catch (Exception | GeneralException exception) {
			exception.printStackTrace();
			logger.error("Exception in predictStrategy()" + exception.toString(), exception);
			runStatusDTO.runId = -1;
		}
		return runStatusDTO;
	}
	
	// This method is called if HTML is request
	@Path("/sayHtmlHello")
	@GET
	@Produces(MediaType.TEXT_HTML)
	public String sayHtmlHello() {
	    return "<html> " + "<title>" + "Recommendation Service" + "</title>"
	        + "<body><h1>" + "Recommendation Service is running" + "</body></h1>" + "</html> ";
	}
		
	@Path("/predict")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public RunStatusDTO predict(String predictionInput) {
		logger.info("predictMovement service hit successfully");
		// logger.info("PredictionInputDTO Item Size: " +
		// predictionInputDTO.predictionItems.size());
		// logger.debug("Input Content:" + predictionInputDTO.toString());
		logger.debug("Input Content:" + predictionInput);
		RunStatusDTO runStatusDTO = new RunStatusDTO();

		PredictionService predictionService = new PredictionService();
		runStatusDTO = predictionService.predictMovementOnDemand(predictionInput);
		return runStatusDTO;
	}
	
	@Path("/explainPrediction")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public RunStatusDTO explainPrediction(String predictionInput) {
		logger.info("explainPrediction service hit successfully");
		logger.debug("Input Content:" + predictionInput);
		RunStatusDTO runStatusDTO = new RunStatusDTO();
		PredictionService predictionService = new PredictionService();
		runStatusDTO = predictionService.explainPrediction(predictionInput);
		return runStatusDTO;
	}
	
	@Path("/predictSubstitueImpact")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String predictSubstitueImpact(String predictionInput){
		logger.info("predictSubstitueImpact service hit successfully");			 	
		logger.debug("Input Content:" + predictionInput.toString());
		String jsonOutput = "";
		PredictionService predictionService = new PredictionService();
		
		jsonOutput = predictionService.predictSubstituteImpact(predictionInput);
		return jsonOutput;
	}
	
	@Path("/updatePrediction")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public RunStatusDTO updatePrediction(String predictionInput){
		RunStatusDTO runStatusDTO = new RunStatusDTO();
		PredictionService predictionService = new PredictionService();
		logger.info("updatePrediction service hit successfully");			 	
		logger.debug("Input Content:" + predictionInput.toString());
		runStatusDTO = predictionService.updatePrediction(predictionInput);
		return runStatusDTO;
	}
	
	@Path("/predictPromotion")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String predictPromotion(String predictionInput) throws GeneralException{
//		PRCommonUtil.setLog4jProperties("predict-promotion");
		logger.info("predictPromotion service hit successfully");			 	
		logger.debug("Input Content:" + predictionInput.toString());
		String jsonOutput = "";
		PredictionService predictionService = new PredictionService();
		
		jsonOutput = predictionService.predictPromotion(predictionInput);
		return jsonOutput;
	}
	
	@Path("/adjustPriceToMeetPI")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String adjustPriceToMeetPI(String runIds){
		logger.info("adjustPriceToMeetPI service hit successfully");			 	
		logger.debug("Input Content:" + runIds.toString());
		ObjectMapper mapper = new ObjectMapper();
		String jsonOutput = "";
		PriceAdjustment priceAdjustment = new PriceAdjustment();
		List<RunStatusDTO> runStatus = new ArrayList<RunStatusDTO>();
		runStatus = priceAdjustment.balancePIAndMargin(runIds);
		try {
			jsonOutput = mapper.writeValueAsString(runStatus);
		} catch (JsonProcessingException e) {
			logger.error("Exception in adjustPriceToMeetPI(). Error while converting to Json String" + e.toString(), e);
		}
		logger.debug("Output:" + jsonOutput);
		return jsonOutput;
	}
	
	@Path("/rollbackAdjustedPrice")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String rollbackAdjustedPrice(String runIds){
		logger.info("rollbackAdjustedPrice service hit successfully");			 	
		logger.debug("Input Content:" + runIds.toString());
		ObjectMapper mapper = new ObjectMapper();
		String jsonOutput = "";
		PriceAdjustment priceAdjustment = new PriceAdjustment();
		List<RunStatusDTO> runStatus = new ArrayList<RunStatusDTO>();
		runStatus = priceAdjustment.rollbackAdjustedPrice(runIds);		
		try {
			jsonOutput = mapper.writeValueAsString(runStatus);
		} catch (JsonProcessingException e) {
			logger.error("Exception in rollbackAdjustedPrice(). Error while converting to Json String" + e.toString(), e);
		}
		logger.debug("Output:" + jsonOutput);
		return jsonOutput;
	}
	
	@Path("/getStorePrice")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String getStorePrice(@QueryParam("recommendationRunId") String recommendationRunId, @QueryParam("itemCodes") String itemCodes) {
		ObjectMapper mapper = new ObjectMapper();
		String jsonOutput = "";
		PricingEngineService pricingEngineservice = new PricingEngineService();
		try {
			logger.debug("recommendationRunId:" + recommendationRunId + "," + "itemCodes:" + itemCodes);
			List<Integer> items = mapper.readValue(itemCodes, new TypeReference<List<Integer>>(){});
			jsonOutput = mapper.writeValueAsString(pricingEngineservice.getStorePrices(Long.valueOf(recommendationRunId), items));
		} catch (OfferManagementException | IOException e) {
			logger.error("Exception in getStorePrice()" + e.toString(), e);
		}
		logger.debug("Output:" + jsonOutput);
		return jsonOutput;
	}
	
	@Path("/audit")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Audit runAudit(Audit audit){
		logger.info("run audit service hit successfully");
		logger.info(audit.toString());
		try{
			AuditEngineWS auditEngineWS = new AuditEngineWS();
			int locationLevelId = Integer.parseInt(audit.locationLevelId);
			int locationId = Integer.parseInt(audit.locationId);
			int productLevelId = Integer.parseInt(audit.productLevelId);
			int productId = Integer.parseInt(audit.productId);
			String userId = audit.predictedUserId;
			long runId = auditEngineWS.getAuditHeader(locationLevelId, locationId, productLevelId, productId, userId);
			audit.reportId = String.valueOf(runId);
		}catch(Exception exception){
			exception.printStackTrace();
			logger.error("Exception in runAudit()" + exception.toString(), exception);
			audit.reportId = String.valueOf(-1);
		}
		catch(GeneralException exception){
			exception.printStackTrace();
			logger.error("Exception in runAudit()" + exception.toString(), exception);
			audit.reportId = String.valueOf(-1);
		}
		return audit;
	}
	
	@Path("/auditFilter")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public AuditFilter filterItems(AuditFilter auditFilter){
		logger.info("audit filter service hit successfully.");
		logger.info(auditFilter.toString());
		AuditFilter retAuditFilter = new AuditFilter();
		try{
			retAuditFilter.reportId = auditFilter.reportId;
			retAuditFilter.runId = auditFilter.runId;
			retAuditFilter.filterType = auditFilter.filterType;
			AuditService auditService = new AuditService();
			long runId = Long.parseLong(auditFilter.runId);
			long reportId = Long.parseLong(auditFilter.reportId);
			int filterType = Integer.parseInt(auditFilter.filterType);
			auditService.filterAuditItems(runId, reportId, filterType, retAuditFilter);
			retAuditFilter.statusCode = 1;
			retAuditFilter.statusMessage = "success";
		}
		catch(Exception exception){
			exception.printStackTrace();
			logger.error("Exception in filterItems()" + exception.toString(), exception);
			retAuditFilter.statusCode = 0;
			retAuditFilter.statusMessage = exception.getMessage();
		}
		catch(GeneralException exception){
			exception.printStackTrace();
			logger.error("Exception in filterItems()" + exception.toString(), exception);
			retAuditFilter.statusCode = 0;
			retAuditFilter.statusMessage = exception.getMessage();
		}
		return retAuditFilter;
	}
	
	//TODO:: put it in different class file
	@Path("/getPredictionReport")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String getPredictionReport(@QueryParam("locationLevelId") int locationLevelId, @QueryParam("locationId") int locationId,
			@QueryParam("productLevelId") int productLevelId, @QueryParam("productId") int productId,
			@QueryParam("weekCalendarId") int weekCalendarId) {
		ObjectMapper mapper = new ObjectMapper();
		String jsonOutput = "";
		PredictionReportService predictionReportService = new PredictionReportService();
		try {
			logger.debug("locationLevelId:" + locationLevelId + "," + "locationId:" + locationId + "," + "weekCalendarId:" + weekCalendarId);
			jsonOutput = mapper.writeValueAsString(
					predictionReportService.generatePredictionReportForUI(locationLevelId, locationId, productLevelId, productId, weekCalendarId));
		} catch (IOException e) {
			logger.error("Exception in getPredictionReport()" + e.toString(), e);
		}
		//logger.debug("Output:" + jsonOutput);
		return jsonOutput;
	}
	
	@Path("/rerecommendation")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String rerecommendation(@QueryParam("runIds") String runIds, @QueryParam("userId") String userId){
		logger.info("Re-Recommendation service hit successfully");			 	
		logger.info("Input Content:" + runIds.toString() + "," + userId);
		ObjectMapper mapper = new ObjectMapper();
		String jsonOutput = "";
		RerecommendationService rerecommendationService = new RerecommendationService();
		List<RunStatusDTO> runStatus = new ArrayList<RunStatusDTO>();
		runStatus = rerecommendationService.rerecommendRetails(runIds, userId);
		try {
			jsonOutput = mapper.writeValueAsString(runStatus);
		} catch (JsonProcessingException e) {
			logger.error("Exception in rerecommendation(). Error while converting to Json String" + e.toString(), e);
		}
		logger.debug("Output:" + jsonOutput);
		return jsonOutput;
	}
	
	@Path("/onDemandPredictionTest")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String onDemandPredictionTest(String predictionInput) {
		String jsonOutput = "";
		logger.info("predictMovement service hit successfully");
		// logger.info("PredictionInputDTO Item Size: " +
		// predictionInputDTO.predictionItems.size());
		// logger.debug("Input Content:" + predictionInputDTO.toString());
		logger.debug("Input Content:" + predictionInput);
		PredictionService predictionService = new PredictionService();
		jsonOutput = predictionService.onDemandPredictionTest(predictionInput);
		return jsonOutput;
	}
	
	
	@Path("/mwrupdaterecommendation")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String mwrupdaterecommendation(@QueryParam("runIds") String runIds, @QueryParam("userId") String userId){
		logger.info("Re-Recommendation service hit successfully");			 	
		logger.info("Input Content:" + runIds.toString() + "," + userId);
		ObjectMapper mapper = new ObjectMapper();
		String jsonOutput = "";
		MWRUpdateRecommendationService mwrUpdateRecommendationService = new MWRUpdateRecommendationService();
		List<RunStatusDTO> runStatus = new ArrayList<RunStatusDTO>();
		runStatus = mwrUpdateRecommendationService.rerecommendRetails(runIds, userId);
		try {
			jsonOutput = mapper.writeValueAsString(runStatus);
		} catch (JsonProcessingException e) {
			logger.error("Exception in rerecommendation(). Error while converting to Json String" + e.toString(), e);
		}
		logger.debug("Output:" + jsonOutput);
		return jsonOutput;
	}
	
	
	@Path("/qrrecommendation")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public RunStatusDTO qrrecommendation(String strategy) {
		logger.info("recommend service hit successfully");
		logger.info(strategy.toString());
		RunStatusDTO runStatusDTO = new RunStatusDTO();
		RecommendationWS pricingEngine = new RecommendationWS();
		ObjectMapper mapper = new ObjectMapper();
		try {
			Strategy objStrategy = mapper.readValue(strategy, Strategy.class);
			long runId = pricingEngine.runPriceRecommendation(objStrategy.locationLevelId, objStrategy.locationId, objStrategy.productLevelId,
					objStrategy.productId, objStrategy.predictedUserId);
			runStatusDTO.runId = runId;
		} catch (Exception | GeneralException exception) {
			exception.printStackTrace();
			logger.error("Exception in predictStrategy()" + exception.toString(), exception);
			runStatusDTO.runId = -1;
		}
		return runStatusDTO;
	}
	
	@Path("/qrstrategywhatif")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public RunStatusDTO qrstrategywhatif(String strategy) {
		logger.info("recommend service hit successfully");
		logger.info(strategy.toString());
		RunStatusDTO runStatusDTO = new RunStatusDTO();
		RecommendationWS pricingEngine = new RecommendationWS();
		ObjectMapper mapper = new ObjectMapper();
		try {
			Strategy objStrategy = mapper.readValue(strategy, Strategy.class);
			long runId = pricingEngine.runStrategyWhatIf(objStrategy.locationLevelId, objStrategy.locationId, objStrategy.productLevelId,
					objStrategy.productId, objStrategy.predictedUserId, objStrategy.strategyId,objStrategy.runOnlyTempStrat);
			runStatusDTO.runId = runId;
		} catch (Exception | GeneralException exception) {
			exception.printStackTrace();
			logger.error("Exception in predictStrategy()" + exception.toString(), exception);
			runStatusDTO.runId = -1;
		}
		return runStatusDTO;
	}
	
	@Path("/simulateexport")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String simulateExport(String exportParams) {
		
		logger.info("simulateexport service hit successfully ....");
		logger.debug("exportParams: "+exportParams);
		String returnJson = new String("{\"exportFilePath\":\"%s\",\"exportFileName\":\"%s\"}");
		
		ObjectMapper mapper = new ObjectMapper();
		StorePriceExportSimulation priceExportSim;
		try {
			priceExportSim = mapper.readValue(exportParams, StorePriceExportSimulation.class);
		} catch (JsonParseException e) {
			logger.info("The json passed by the Price Export Simulation may not be in the right format!");
			logger.debug("exportParams: "+exportParams);
			logger.error("Parsing problem in parsing the input json string.", e);
			return String.format(returnJson, "","");
		} catch (JsonMappingException e) {
			logger.info("The json passed by the Price Export Simulation may not have the correct attributes!");
			logger.debug("exportParams: "+exportParams);
			logger.error("Mapping problem in mapping the input json string.", e);
			return String.format(returnJson, "","");
		} catch (IOException e) {
			logger.info("Problem occured while reading the parameters passed by the Price Export Simulation!");
			logger.debug("exportParams: "+exportParams);
			logger.error("I/O problem in reading the input json string.", e);
			return String.format(returnJson, "","");
		}
		if (null == priceExportSim.salesFloorItemLimit 
				|| priceExportSim.salesFloorItemLimit.equals("0"))
			return String.format(returnJson, "","");
		logger.debug(String.format("effectiveDate: %s, exportType: %s, salesFloorItemLimit: %s, localTime: %s", 
				priceExportSim.effectiveDate, priceExportSim.exportType, priceExportSim.salesFloorItemLimit, priceExportSim.localTime));
		priceExportSim.effectiveDate = priceExportSim.effectiveDate.replace("/","");
		logger.debug("Request local time: "+priceExportSim.localTime);
		
		String exportedFilePath;
		StorePriceExportV3 storePriceExport = new StorePriceExportV3(false);
		storePriceExport.setSaleFloorLimitStr(priceExportSim.salesFloorItemLimit);
		logger.debug(String.format("salesFloorItemLimit set to: %s", storePriceExport.getSaleFloorLimitStr()));
		storePriceExport.setPriceExportTime(priceExportSim.localTime);
		exportedFilePath = storePriceExport.generateStorePriceExport(priceExportSim.exportType, priceExportSim.effectiveDate);
		if(null==exportedFilePath) {
			exportedFilePath = "";
			logger.debug("The store price export program did NOT return a file path!");
			return String.format(returnJson, "","");
		}
		
		logger.debug("Simulated price export file: "+exportedFilePath);
		int indexOfLastSeapartor = exportedFilePath.lastIndexOf(92); //'\' (reverse solidus) = 0x005C = 92
		String fileLocation = exportedFilePath.substring(0,1+indexOfLastSeapartor);
		logger.debug("Simulated price export file location: "+fileLocation);
		String fileName = exportedFilePath.substring(1+indexOfLastSeapartor);
		logger.debug("Simulated price export file name: "+fileName);
		
		fileLocation = fileLocation.replace("\\","\\\\"); // convert C:\xy\z to C:\\xy\\z. UI needs it like this.
		logger.debug("Escaping the \\ in the fileLocation: "+fileLocation);
		return String.format(returnJson, fileLocation, fileName);
		
	}
	
}