package com.pristine.dataload.offermgmt;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.offermgmt.AuditEngineDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.DashboardDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.audittool.AuditDashboardDTO;
import com.pristine.dto.offermgmt.audittool.AuditParameterDTO;
import com.pristine.dto.offermgmt.audittool.AuditParameterHeaderDTO;
import com.pristine.dto.offermgmt.audittool.AuditReportDTO;
import com.pristine.dto.offermgmt.audittool.AuditReportHeaderDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.AuditService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;


public class AuditEngineWS {
	private static Logger logger = Logger.getLogger("AuditEngineWS");
	private Connection conn = null;
	private boolean isOnline;
	private Context initContext;
	private Context envContext;
	private DataSource ds;
	private AuditEngineDAO auditEngineDAO = new AuditEngineDAO();
	private static final String ZONE_ID = "ZONE_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";
	private static final String STRATEGY_ID = "STRATEGY_ID=";
	private static final String DATE = "DATE=";
	
	private int batchLocationLevelId = -1;
	private int batchLocationId = -1;
	private int batchProductLevelId = -1;
	private int batchProductId = -1;
	private int batchStrategyId = -1;
	private String batchInputDate = null;
	private String chainId = null;
	private int divisionId = 0;
	public boolean isIntegratedProcess = false;
	public AuditEngineWS(){
		String runType = PropertyManager.getProperty("PR_RUN_TYPE");
		logger.debug("Run Type - " + runType);
		if (runType != null && PRConstants.RUN_TYPE_ONLINE == runType.charAt(0)) {
			isOnline = true;
		}else
			isOnline = false;
	}
	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j-audit-engine.properties");
		PropertyManager.initialize("recommendation.properties");
		AuditEngineWS engine = new AuditEngineWS();
		if(args.length > 0){
			for(String arg : args){
				if(arg.startsWith(ZONE_ID)){
					engine.setBatchLocationLevelId(Constants.ZONE_LEVEL_ID);
					engine.setBatchLocationId(Integer.parseInt(arg.substring(ZONE_ID.length())));
				}else if(arg.startsWith(PRODUCT_LEVEL_ID)){
					engine.setBatchProductLevelId(Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length())));
				}else if(arg.startsWith(PRODUCT_ID)){
					engine.setBatchProductId(Integer.parseInt(arg.substring(PRODUCT_ID.length())));
				}else if(arg.startsWith(STRATEGY_ID)){
					engine.setBatchProductId(Integer.parseInt(arg.substring(STRATEGY_ID.length())));
				}else if(arg.startsWith(DATE)){
					engine.setBatchInputDate(arg.substring(DATE.length()));
				}
			}
		}
		
		
		try{
			engine.runAudit();
		}catch(Exception e){
			logger.error("Error while running audit." + e.toString());
		}
		catch(GeneralException e){
			logger.error("Error while running audit." + e.toString());
		}
		
		
	}
	
	
	private void runAudit() throws GeneralException{
		long reportId = this.getAuditHeader(this.getBatchLocationLevelId(), this.getBatchLocationId(), this.getBatchProductLevelId(), this.getBatchProductId(), PRConstants.BATCH_USER);
		logger.info("Audit is in progress for report id - " + reportId);
	}
	
	public long getAuditHeader(int locationLevelId, int locationId, int productLevelId, int productId, String userId) throws GeneralException
	{
		if(isOnline)
			initializeForWS();
		else{
			initialize();
		}
		AuditReportHeaderDTO auditReportHeaderDTO = null;
		try{
			auditReportHeaderDTO = insertAuditReportHeader(locationLevelId, locationId, productLevelId, productId, userId,"");
		}
		catch(GeneralException e){
			logger.error("getAuditHeader() - Error while executing audit report - " + e.toString());
			throw new GeneralException("getAuditHeader() - Error while executing audit report - " + e.toString());
		}
		
		return auditReportHeaderDTO.getReportId();
	}
	
	// for integrating recommendation and audit
	public long getAuditHeader(Connection con, int locationLevelId, int locationId, int productLevelId, int productId,
			String userId, String reccweek) throws GeneralException {
		AuditReportHeaderDTO auditReportHeaderDTO = null;

		this.setConnection(con);

		try {
			auditReportHeaderDTO = insertAuditReportHeader(locationLevelId, locationId, productLevelId, productId,
					userId, reccweek);
		} catch (GeneralException e) {
			logger.error("getAuditHeader() - Error while executing audit report - " + e.toString());
			throw new GeneralException("getAuditHeader() - Error while executing audit report - " + e.toString());
		}

		return auditReportHeaderDTO.getReportId();
	}
	
	private AuditReportHeaderDTO insertAuditReportHeader(int locationLevelId, int locationId, int productLevelId,
			int productId, String userId, String reccweek) throws GeneralException {
		AuditReportHeaderDTO auditReportHeaderDTO = new AuditReportHeaderDTO();
		auditReportHeaderDTO.setReportId(-1);
		try{
			AuditDashboardDTO auditDashboardDTO = new AuditDashboardDTO();
			auditDashboardDTO.setProductId(productId);
			auditDashboardDTO.setProductLevelId(productLevelId);
			auditDashboardDTO.setLocationId(locationId);
			auditDashboardDTO.setLocationLevelId(locationLevelId);
			//Check audit is available for current product and location. If so get the audit param header id.
			long auditParamHeaderId = auditEngineDAO.getAuditParamHeaderId(getConnection(), auditDashboardDTO);
			//get run id for filling AuditReportHeaderDTO
			if(auditParamHeaderId == -1){
				auditParamHeaderId = auditEngineDAO.getDefaultAuditParamHeaderId(getConnection());
			}			
			AuditParameterHeaderDTO auditParameterHeaderDTO = auditEngineDAO.getAuditParameterHeader(getConnection(),
					auditParamHeaderId);
			List<AuditParameterDTO> auditParameters = auditEngineDAO.getAuditParameters(getConnection(),
					auditParamHeaderId, auditParameterHeaderDTO.getApVerId());
			DashboardDTO dashboardDTO = auditEngineDAO.getPRDashboard(getConnection(), auditDashboardDTO);
			dashboardDTO.setLocationId(locationId);
			dashboardDTO.setLocationLevelId(locationLevelId);
			dashboardDTO.setProductId(productId);
			dashboardDTO.setProductLevelId(productLevelId);

			RetailCalendarDTO retailCalendarDTO;
			if ( (reccweek != null && !reccweek.equals("") ) || (getBatchInputDate() != null && getBatchInputDate ()!="" )) {
				
				if (reccweek != null && !reccweek.equals(Constants.EMPTY)) {
					retailCalendarDTO = new RetailCalendarDAO().getWeekCalendarFromDate(conn, reccweek);
				} else {
					retailCalendarDTO = new RetailCalendarDAO().getWeekCalendarFromDate(conn, getBatchInputDate());
				}

				long runId = new PricingEngineDAO().getLatestRecommendationRunId(conn, locationLevelId, locationId,
						productLevelId, productId, retailCalendarDTO.getCalendarId());

				auditReportHeaderDTO.setRunId(runId);
			} else if (dashboardDTO.getRecommendationRunId() != 0) {
				long runId = new PricingEngineDAO().getLatestRecommendationRunIdForAudit(getConnection(), locationLevelId, locationId,
						productLevelId, productId);
				logger.info("recc Run id:" + runId);
				auditReportHeaderDTO.setRunId(runId);
			} else {
				auditReportHeaderDTO.setMessage(
						PRConstants.MESSAGE_AUDIT_ERROR + " - " + PRConstants.MESSAGE_RECOMMENDATION_NOT_FOUND);
				auditEngineDAO.updateAuditReportHeader(getConnection(), auditReportHeaderDTO);
				return auditReportHeaderDTO;
			}
			auditReportHeaderDTO.setParamHeaderId(auditParamHeaderId);
			auditReportHeaderDTO.setAuditBy(userId);
			auditReportHeaderDTO.setApVersionId(auditParameterHeaderDTO.getApVerId());
			if (isOnline)
				auditReportHeaderDTO.setRunType(PRConstants.RUN_TYPE_ONLINE);
			else
				auditReportHeaderDTO.setRunType(PRConstants.RUN_TYPE_BATCH);
			// Insert Audit report header
			auditEngineDAO.insertAuditReportHeader(conn, auditReportHeaderDTO);
			auditDashboardDTO.setParamHeaderId(auditParamHeaderId);
			auditDashboardDTO.setReportId(auditReportHeaderDTO.getReportId());
			auditEngineDAO.insertOrUpdateAuditDashboard(getConnection(), auditDashboardDTO);
			//isIntegratedProcess : denotes if the Audit is executed from Pricing Engine directly
			if(isIntegratedProcess){
				logger.debug("Audit Engine :Synchronous mode: syncServiceMethod() called" ); 
				syncServiceMethod(auditReportHeaderDTO, dashboardDTO, auditParameters);
			}else{
				logger.debug("Audit Engine :Asynchronous mode: asyncServiceMethod() called");
				asyncServiceMethod(auditReportHeaderDTO, dashboardDTO, auditParameters);
			}
		}
		catch( GeneralException | Exception e){
			logger.error("insertAuditReportHeader() - Error while inserting audit header - " + e.toString());
			throw new GeneralException("insertAuditReportHeader() - Error while inserting audit header - " + e.toString());
		}
	
		return auditReportHeaderDTO; 
	}
	
	//The Synchronous method added to execute the Audit from Pricing engine directly
	public void syncServiceMethod(final AuditReportHeaderDTO auditReportHeaderDTO, final DashboardDTO dashboardDTO, 
			final List<AuditParameterDTO> auditParameters) throws GeneralException{
    	try {
			runAuditing(auditReportHeaderDTO, dashboardDTO, auditParameters);
		} catch (GeneralException | Exception e) {
			
			logger.error("asyncServiceMethod() - Error during audit - " + e.toString(), e);
			e.printStackTrace();
			auditReportHeaderDTO.setMessage(PRConstants.MESSAGE_AUDIT_ERROR + " - " + e.getMessage());
			auditEngineDAO.updateAuditReportHeader(getConnection(), auditReportHeaderDTO);
			throw new GeneralException("syncServiceMethod() - Error while executing runAuditing() - " + e.toString());
		} 
	}
	
	public void asyncServiceMethod(final AuditReportHeaderDTO auditReportHeaderDTO, final DashboardDTO dashboardDTO,
			final List<AuditParameterDTO> auditParameters){
		Runnable task = new Runnable() {
            @Override
            public void run() {
            	try {
					runAuditing(auditReportHeaderDTO, dashboardDTO, auditParameters);
				} catch (GeneralException | Exception e) {
					
					logger.error("asyncServiceMethod() - Error during audit - " + e.toString(), e);
					e.printStackTrace();
					auditReportHeaderDTO.setMessage(PRConstants.MESSAGE_AUDIT_ERROR + " - " + e.getMessage());
					auditEngineDAO.updateAuditReportHeader(getConnection(), auditReportHeaderDTO);
				} finally {
        			try {
            			getConnection().commit();
            		} catch (SQLException e) {
            			
            			e.printStackTrace();
            		}
        			PristineDBUtil.close(getConnection());
        		}
            }
        };
        new Thread(task, "ServiceThread").start(); 
	}
	
	
	
	public void runAuditing(AuditReportHeaderDTO auditReportHeaderDTO, DashboardDTO dashboardDTO,
			List<AuditParameterDTO> auditParameters) throws GeneralException, Exception {
		// Get the required columns from recommendation.
		List<PRItemDTO> prItemDTOList = auditEngineDAO.getAuditDetailFromRecommendation(getConnection(),
				auditReportHeaderDTO);
		SimpleDateFormat formatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		AuditService auditService = new AuditService();
		double listCostChangeGTPct = 0;
		int retailChangeNTimes = 0;
		for (AuditParameterDTO auditParam : auditParameters) {
			if (auditParam.getParamsType().trim().equals(String.valueOf(PRConstants.LIST_COST_CHANGE_GT_PCT))) {
				listCostChangeGTPct = auditParam.getParameterValue();
			}
			else if (auditParam.getParamsType().trim().equals(String.valueOf(PRConstants.RETAIL_CHANGES_X_TIMES))) {
				retailChangeNTimes = (int) auditParam.getParameterValue();
			}
		}
		
		// Do calculations for columns which are not straight forward.
		Date startDate = auditEngineDAO.getStartDateFromRecommendation(getConnection(),
				auditReportHeaderDTO.getRunId());
		if (startDate == null) {
			logger.error("Error -- runAuditing() - Unable to get startDate of recommendation");
			auditReportHeaderDTO
					.setMessage(PRConstants.MESSAGE_AUDIT_ERROR + " - Unable to get startDate of recommendation");
			auditEngineDAO.updateAuditReportHeader(getConnection(), auditReportHeaderDTO);
			throw new GeneralException(
					PRConstants.MESSAGE_AUDIT_ERROR + " - Unable to get startDate of recommendation");
		}
		int retailChangesXMonths = Integer.parseInt(PropertyManager.getProperty("AUDIT_RETAIL_CHANGES_IN_X_MONTHS"));
		int costChangesXMonths = Integer.parseInt(PropertyManager.getProperty("AUDIT_COST_CHANGES_IN_X_MONTHS"));
		
		Date endDateForPrice = DateUtil.incrementDate(startDate, -(retailChangesXMonths * 30));
		Date endDateForCost = DateUtil.incrementDate(startDate, -(costChangesXMonths * 30));
		String startDateStr = formatter.format(startDate);
		String endDateStrForPrice = formatter.format(endDateForPrice);
		String endDateStrForCost = formatter.format(endDateForCost);
		
		Set<Integer> retailChangeSet = auditService.getRetailsChangedXtimesInLastXmonths(conn, startDateStr,
				endDateStrForPrice, dashboardDTO.getLocationId(), auditReportHeaderDTO.getRunId(), retailChangeNTimes);
		
		Set<Integer> costChangeSet = auditService.getCostChangGtXPctInLastXmonths(getConnection(), startDateStr,
				endDateStrForCost, dashboardDTO.getLocationId(), auditReportHeaderDTO.getRunId(), listCostChangeGTPct);
		AuditDashboardDTO auditDashboardDTO = new AuditDashboardDTO();
		auditDashboardDTO.setLocationId(dashboardDTO.getLocationId());
		auditDashboardDTO.setLocationLevelId(dashboardDTO.getLocationLevelId());
		auditDashboardDTO.setProductLevelId(dashboardDTO.getProductLevelId());
		auditDashboardDTO.setProductId(dashboardDTO.getProductId());
		HashMap<Integer, CompetitiveDataDTO> compData = auditService.getCompDataHistory(getConnection(), auditDashboardDTO, startDateStr);
		
		AuditReportDTO auditReportDTO = new AuditReportDTO();
		if (prItemDTOList.size() != 0) {
			auditReportHeaderDTO.setMessage(PRConstants.MESSAGE_RECOMMENDATION_DETAILS);
			auditReportHeaderDTO.setPercentCompleted(PRConstants.AUDIT_COMPLETE_PRECENT_FIFTY);
			logger.info("runAuditing() - Audit is done  - " + PRConstants.AUDIT_COMPLETE_PRECENT_FIFTY + "%");
			auditReportDTO.setReportId(auditReportHeaderDTO.getReportId());
			auditEngineDAO.updateAuditReportHeader(getConnection(), auditReportHeaderDTO);
			auditReportDTO.setRunID(auditReportHeaderDTO.getRunId());
			calculateMetricsForAudit(prItemDTOList, auditReportDTO, dashboardDTO, auditParameters, retailChangeSet,
					costChangeSet, compData, startDate);
			boolean auditSuccess = false;
			try {
				logger.info(
						"runAuditing() - Audit is done  - " + PRConstants.AUDIT_COMPLETE_PRECENT_SEVENTY_FIVE + "%");
				auditSuccess = auditEngineDAO.insertAuditReport(getConnection(), auditReportDTO);
			} catch (GeneralException | Exception e) {
				logger.error("runAuditing() - Audit report failed - " + e.toString());
				// update audit header with failure.
				auditReportHeaderDTO.setMessage(PRConstants.MESSAGE_AUDIT_ERROR + " - " + e.getMessage());
				auditEngineDAO.updateAuditReportHeader(getConnection(), auditReportHeaderDTO);
				throw e;
			}

			if (auditSuccess) {
				// update audit header with success.
				auditReportHeaderDTO.setMessage(PRConstants.MESSAGE_AUDIT_COMPLETED);
				auditReportHeaderDTO.setPercentCompleted(PRConstants.AUDIT_COMPLETE_PRECENT_FULL);
				auditEngineDAO.updateAuditReportHeader(getConnection(), auditReportHeaderDTO);
				logger.info("runAuditing() - Auditing is completed successfully");
			}
		} else {
			auditReportHeaderDTO
					.setMessage(PRConstants.MESSAGE_AUDIT_ERROR + " - " + PRConstants.MESSAGE_AUDIT_INFO_NOT_AVAILABLE);
			auditEngineDAO.updateAuditReportHeader(getConnection(), auditReportHeaderDTO);
			logger.error("runAuditing() - Audit information is not available for run id - "
					+ auditReportHeaderDTO.getRunId());
		}
	}
	
	@SuppressWarnings("unused")
	private void calculateMetricsForAudit(List<PRItemDTO> prItemDTOList, AuditReportDTO auditReportDTO,
			DashboardDTO dashboardDTO, List<AuditParameterDTO> auditParameters, 
			Set<Integer> retailChangeSet, Set<Integer> costChangeSet, 
			HashMap<Integer, CompetitiveDataDTO> compData, Date startDate) throws Exception, GeneralException {
		try {
			AuditService auditService = new AuditService();
			HashMap<Integer, List<PRItemDTO>> uniqueMap = new HashMap<Integer, List<PRItemDTO>>();
			HashMap<Integer, List<PRItemDTO>> itemMap = new HashMap<Integer, List<PRItemDTO>>();
			HashMap<Integer, List<PRItemDTO>> brandMap = new HashMap<Integer, List<PRItemDTO>>();
			HashMap<Integer, List<PRItemDTO>> ligMap = new HashMap<Integer, List<PRItemDTO>>();
			int totalItems = 0;
			int totalLig = 0;
			double marginLTPct = 0;
			double marginGTPct = 0;
			double lowerSizeGTPct = 0;
			double similarSizeGTPct = 0;
			double retailChangeLTPlusOrMinusPct = 0;
			double retailChangeGTPlusOrMinusPct = 0;
			double retailLTPrimeCompPct = 0;
			double retailGTPrimeCompPct = 0;
			double listCostChangeGTPct = 0;
			double primaryCompChangeGTPct = 0;
			int retailChangeNTimes = 0;
			int endDigitToValidate = 0;
			List<PRItemDTO> outOfNormList = new ArrayList<PRItemDTO>();
			List<PRItemDTO> ligList = new ArrayList<PRItemDTO>();
			for (AuditParameterDTO auditParameterDTO : auditParameters) {
				if (auditParameterDTO.getParamsType().trim()
						.equals(String.valueOf(PRConstants.LOWER_SIZE_ITEMS_GT_PCT))) {
					lowerSizeGTPct = auditParameterDTO.getParameterValue();
				} else if (auditParameterDTO.getParamsType().trim()
						.equals(String.valueOf(PRConstants.SIMILAR_SIZE_ITEMS_GT_PCT))) {
					similarSizeGTPct = auditParameterDTO.getParameterValue();
				} else if (auditParameterDTO.getParamsType().trim()
						.equals(String.valueOf(PRConstants.RETAIL_CHANGED_GT_PCT))) {
					retailChangeGTPlusOrMinusPct = auditParameterDTO.getParameterValue();
				} else if (auditParameterDTO.getParamsType().trim()
						.equals(String.valueOf(PRConstants.RETAIL_CHANGED_LT_PCT))) {
					retailChangeLTPlusOrMinusPct = auditParameterDTO.getParameterValue();
				} else if (auditParameterDTO.getParamsType().trim()
						.equals(String.valueOf(PRConstants.RETAIL_GT_PRIME_COMP_PCT))) {
					retailGTPrimeCompPct = auditParameterDTO.getParameterValue();
				} else if (auditParameterDTO.getParamsType().trim()
						.equals(String.valueOf(PRConstants.RETAIL_LT_PRIME_COMP_PCT))) {
					retailLTPrimeCompPct = auditParameterDTO.getParameterValue();
				} else if (auditParameterDTO.getParamsType().trim()
						.equals(String.valueOf(PRConstants.LIST_COST_CHANGE_GT_PCT))) {
					listCostChangeGTPct = auditParameterDTO.getParameterValue();
				} else if (auditParameterDTO.getParamsType().trim()
						.equals(String.valueOf(PRConstants.PRIMARY_COMP_CHANGE_GT_PCT))) {
					primaryCompChangeGTPct = auditParameterDTO.getParameterValue();
				} else if (auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.MARGIN_GT_PCT))) {
					marginGTPct = auditParameterDTO.getParameterValue();
				} else if (auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.MARGIN_LT_PCT))) {
					marginLTPct = auditParameterDTO.getParameterValue();
				} else if (auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.RETAIL_CHANGES_X_TIMES))) {
					retailChangeNTimes = (int) auditParameterDTO.getParameterValue();
				} else if (auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.END_DIGIT_VIOL))) {
					endDigitToValidate = (int) auditParameterDTO.getParameterValue();
				}
			}
			for (PRItemDTO prItemDTO : prItemDTOList) {
				if (prItemDTO.isLir() || prItemDTO.getRetLirId() == 0) {
					if (prItemDTO.getRetLirId() == 0 && !prItemDTO.isLir()) {
						totalItems++;
					}
					if (prItemDTO.isLir()) {
						ligList.add(prItemDTO);
					}
					totalLig++;
				} else {
					totalItems++;
				}
				
				auditService.groupBrandItems(prItemDTO, brandMap);
				boolean isRetailChanged = auditService.checkRetailChanged(prItemDTO);
				if (isRetailChanged) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAILS_CHANGED.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAILS_CHANGED.getAuditParamId());
				}
				
				boolean isRetailIncreased = auditService.checkRetailIncreased(prItemDTO);
				if (isRetailIncreased) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAIL_INCREASED.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAIL_INCREASED.getAuditParamId());
				}
				
				boolean isRetailDecreased = auditService.checkRetailDecreased(prItemDTO);
				if (isRetailDecreased) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAIL_DECREASED.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAIL_DECREASED.getAuditParamId());
				}
				
				boolean isRetailOverridden = auditService.checkRetailOverridden(prItemDTO);
				if (isRetailOverridden) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAILS_OVERIDDEN.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAILS_OVERIDDEN.getAuditParamId());
				}
				
				boolean isItemMarked = auditService.checkItemMarked(prItemDTO);
				if (isItemMarked) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.MANAGER_REVIEW.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.MANAGER_REVIEW.getAuditParamId());
				}
				
				boolean isRetailBelowListCost = auditService.checkRetailsBelowListCost(prItemDTO);
				if (isRetailBelowListCost) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAILS_LT_LC.getAuditParamId());
					addOutOfNorm(outOfNormList, prItemDTO);
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAILS_LT_LC.getAuditParamId());
				}
				
				boolean isRetailBelowListCostNoPromo = auditService.checkRetailsBelowListCostNoPromo(prItemDTO);
				if (isRetailBelowListCostNoPromo) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAILS_LT_LC_NO_PROMO.getAuditParamId());
					addOutOfNorm(outOfNormList, prItemDTO);
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAILS_LT_LC_NO_PROMO.getAuditParamId());
				}

				/*
				 * boolean isRetailBelowPromo =
				 * auditService.checkRetailsBelowPromoCost(prItemDTO); if (isRetailBelowPromo) {
				 * addItems(itemMap, prItemDTO,
				 * AuditFilterTypes.RETAILS_LT_PROMO.getAuditParamId());
				 * addOutOfNorm(outOfNormList, prItemDTO); addUniqueItems(uniqueMap, prItemDTO,
				 * AuditFilterTypes.RETAILS_LT_PROMO.getAuditParamId()); }
				 */

				boolean isMarginBelowXPct = auditService.checkMarginLTXPct(prItemDTO, marginLTPct);
				if (isMarginBelowXPct) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.MARGIN_LT_X_PCT.getAuditParamId());
					addOutOfNorm(outOfNormList, prItemDTO);
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.MARGIN_LT_X_PCT.getAuditParamId());
				}
				
				boolean isMarginAboveXPct = auditService.checkMarginGTXPct(prItemDTO, marginGTPct);
				if (isMarginAboveXPct) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.MARGIN_RT_X_PCT.getAuditParamId());
					addOutOfNorm(outOfNormList, prItemDTO);
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.MARGIN_RT_X_PCT.getAuditParamId());
				}
				
				boolean isMarginViolation = auditService.checkMarginViolation(prItemDTO, marginLTPct, marginGTPct);
				if (isMarginViolation) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.MARGIN_VIOLATION.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.MARGIN_VIOLATION.getAuditParamId());
				}
				
				boolean isBrandViol = auditService.checkBrandViol(prItemDTO);
				if(isBrandViol){
					addItems(itemMap, prItemDTO, AuditFilterTypes.BRAND_VIOLATION.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.BRAND_VIOLATION.getAuditParamId());
				}

				boolean isSizeViol = auditService.checkSizeViol(prItemDTO);
				if(isSizeViol){
					addItems(itemMap, prItemDTO, AuditFilterTypes.SIZE_VIOLATION.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.SIZE_VIOLATION.getAuditParamId());
				}				
								
				boolean isRetailChangeAboveXPct = auditService.checkRetailChangeGTXPct(prItemDTO,
						retailChangeGTPlusOrMinusPct);
				if (isRetailChangeAboveXPct) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAIL_CHANGE_GT_X_PCT.getAuditParamId());
					addOutOfNorm(outOfNormList, prItemDTO);
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAIL_CHANGE_GT_X_PCT.getAuditParamId());
				}
				boolean isRetailChangeBelowXPct = auditService.checkRetailChangeLTXPct(prItemDTO,
						retailChangeLTPlusOrMinusPct);
				if (isRetailChangeBelowXPct) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAIL_CHANGE_LT_X_PCT.getAuditParamId());
					addOutOfNorm(outOfNormList, prItemDTO);
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAIL_CHANGE_LT_X_PCT.getAuditParamId());
				}
				
				//Retail violation block 
				boolean isRetailViolation = auditService.checkRetailChangeViolation(prItemDTO,
									retailChangeLTPlusOrMinusPct, retailChangeGTPlusOrMinusPct);
				if (isRetailViolation) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAILS_CHANGE_VIOLATION.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAILS_CHANGE_VIOLATION.getAuditParamId());
				}
				
				boolean isRetailToReduceMarginOpp = auditService.checkRetailsToReduceForMarginOpp(prItemDTO);
				if (isRetailToReduceMarginOpp) {
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAILS_TO_REDUCE_MAR_OPP.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAILS_TO_REDUCE_MAR_OPP.getAuditParamId());
				}
				
				boolean isRetailBelowXPctOfComp = auditService.checkRetailBelowXPctOfPriComp(prItemDTO, retailLTPrimeCompPct);
				if(isRetailBelowXPctOfComp){
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAILS_LT_X_PCT_COMP.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAILS_LT_X_PCT_COMP.getAuditParamId());
					addOutOfNorm(outOfNormList, prItemDTO);
				}
				
				boolean isRetailAboveXPctOfComp = auditService.checkRetailAboveXPctOfPriComp(prItemDTO, retailGTPrimeCompPct);
				if(isRetailAboveXPctOfComp){
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAILS_GT_X_PCT_COMP.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAILS_GT_X_PCT_COMP.getAuditParamId());
					addOutOfNorm(outOfNormList, prItemDTO);
				}
				
				// Code to calculate competitor price violation
				boolean isRetailBelowOrAboveXPctOfComp = auditService.checkRetailPriCompViolation(prItemDTO, retailLTPrimeCompPct, retailGTPrimeCompPct);
				if(isRetailBelowOrAboveXPctOfComp){
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAILS_COMP_VIOLATION.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAILS_COMP_VIOLATION.getAuditParamId());
				}
				
				boolean isCostZero = auditService.checkZeroCost(prItemDTO);
				if(isCostZero){
					addItems(itemMap, prItemDTO, AuditFilterTypes.ZERO_COST.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.ZERO_COST.getAuditParamId());
					addOutOfNorm(outOfNormList, prItemDTO);
				}
				
				boolean isCurrPriceZero = auditService.checkZeroCurrentRetail(prItemDTO);
				if(isCurrPriceZero){
					addItems(itemMap, prItemDTO, AuditFilterTypes.ZERO_CURR_RETAILS.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.ZERO_CURR_RETAILS.getAuditParamId());
					addOutOfNorm(outOfNormList, prItemDTO);
				}
				
				boolean isCostChangedGTXPct = auditService.checkCostChangedXPctInLastXmonths(prItemDTO, costChangeSet);
				if(isCostChangedGTXPct){
					addItems(itemMap, prItemDTO, AuditFilterTypes.COST_CAHNGE_GT_X_PCT.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.COST_CAHNGE_GT_X_PCT.getAuditParamId());
				}
				
				boolean isPriceChangedXTimes = auditService.checkRetailChangedXTimesInLastXmonths(prItemDTO, retailChangeSet);
				if(isPriceChangedXTimes){
					addItems(itemMap, prItemDTO, AuditFilterTypes.RETAIL_CHANGE_X_TIMES.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.RETAIL_CHANGE_X_TIMES.getAuditParamId());
				}
				
				boolean isKviItemWithXMonthsOld = auditService.checkKviItemWithCompPriceXmonthsOld(compData, prItemDTO, startDate);
				if(isKviItemWithXMonthsOld){
					addItems(itemMap, prItemDTO, AuditFilterTypes.KVI_WITH_COMP_X_MONTHS_OLD.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.KVI_WITH_COMP_X_MONTHS_OLD.getAuditParamId());
				}
				
				boolean isKviItemWithNoComp = auditService.checkKviItemWithNoComp(prItemDTO);
				if(isKviItemWithNoComp){
					addItems(itemMap, prItemDTO, AuditFilterTypes.KVI_WITH_NO_COMP.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.KVI_WITH_NO_COMP.getAuditParamId());
				}
				
				boolean isEndDigitViol = auditService.checkEndDigitViol(prItemDTO, endDigitToValidate);
				if(isEndDigitViol){
					addItems(itemMap, prItemDTO, AuditFilterTypes.ENDING_DIGIT_VIOL.getAuditParamId());
					addUniqueItems(uniqueMap, prItemDTO, AuditFilterTypes.ENDING_DIGIT_VIOL.getAuditParamId());
				}
				
				auditService.groupLigItems(prItemDTO, ligMap);
			}
			/*
			 * auditReportDTO.setTotalItems(totalItems);
			 * auditReportDTO.setTotalLig(totalLig);
			 */
			auditReportDTO.setOutOfNorm(outOfNormList.size());
			auditReportDTO.setOutOfNormLig(findOutOfNormLig(outOfNormList, ligList));
			auditService.findLinePriceViolations(ligMap, auditReportDTO);
			setUniqueItemsCount(uniqueMap, auditReportDTO);
			setItemsCount(itemMap, auditReportDTO);

			auditEngineDAO.getQuarterlySummary(conn, auditReportDTO);

			calculateSummaryData(prItemDTOList, auditReportDTO);
		} catch (Exception ex) {
			logger.error("calculateMetricsForAudit() - Error while calculating metrics - ", ex);
			ex.printStackTrace();
			throw new Exception("calculateMetricsForAudit() - Error while calculating metrics - " + ex);
		}
	}

	/*
	 * private void checkSizeViolatedItems(HashMap<Integer, List<PRItemDTO>>
	 * brandMap){ for(Map.Entry<Integer, List<PRItemDTO>> entry:
	 * brandMap.entrySet()){ HashMap<Double, List<PRItemDTO>> sizeMap = new
	 * HashMap<Double, List<PRItemDTO>>(); for(PRItemDTO prItemDTO:
	 * entry.getValue()){ double itemSize = prItemDTO.getItemSize(); if(itemSize >
	 * 0){ if(sizeMap.get(itemSize) == null){ List<PRItemDTO> itemList = new
	 * ArrayList<PRItemDTO>(); itemList.add(prItemDTO); sizeMap.put(itemSize,
	 * itemList); } else{ List<PRItemDTO> itemList = sizeMap.get(itemSize);
	 * itemList.add(prItemDTO); sizeMap.put(itemSize, itemList); } } } } }
	 */

	/**
	 * Calculates summary data.
	 * 
	 * @param itemList
	 * @param auditReportDTO
	 */
	private void calculateSummaryData(final List<PRItemDTO> itemList, final AuditReportDTO auditReportDTO) {
		double projectedVolume = 0.0;
		double currentVolume = 0.0;
		double baseMovTotal = 0.0;
		double newRetMovTotal = 0.0;
		double CompStr1Mov = 0.0;
		double CompStr2Mov = 0.0;
		double CompStr3Mov = 0.0;
		double CompStr4Mov = 0.0;
		double CompStr5Mov = 0.0;
		double CompStr6Mov = 0.0;
		double currRetailTot = 0.0;
		double reccRetailTot = 0.0;
		double CompStr1Tot = 0.0;
		double CompStr2Tot = 0.0;
		double CompStr3Tot = 0.0;
		double CompStr4Tot = 0.0;
		double CompStr5Tot = 0.0;
		double CompStr6Tot = 0.0;
		int CompStr1GreaterCount = 0;
		int CompStr1EqualCount = 0;
		int CompStr1LesserCount = 0;
		int CompStr2GreaterCount = 0;
		int CompStr2EqualCount = 0;
		int CompStr2LesserCount = 0;
		int CompStr3GreaterCount = 0;
		int CompStr3EqualCount = 0;
		int CompStr3LesserCount = 0;
		int CompStr4GreaterCount = 0;
		int CompStr4EqualCount = 0;
		int CompStr4LesserCount = 0;
		int CompStr5GreaterCount = 0;
		int CompStr5EqualCount = 0;
		int CompStr5LesserCount = 0;
		int CompStr6GreaterCount = 0;
		int CompStr6EqualCount = 0;
		int CompStr6LesserCount = 0;
		//Added for calculating the AUR for AZ  recc retail with each competitor
		double reccRetailTotforCompStr1 = 0.0;
		double reccRetailTotforCompStr2 = 0.0;
		double reccRetailTotforCompStr3 = 0.0;
		double reccRetailTotforCompStr4 = 0.0;
		double reccRetailTotforCompStr5 = 0.0;
		double reccRetailTotforCompStr6 = 0.0;
		double newRetMovTotalforCompStr1 = 0.0;
		double newRetMovTotalforCompStr2 = 0.0;
		double newRetMovTotalforCompStr3 = 0.0;
		double newRetMovTotalforCompStr4 = 0.0;
		double newRetMovTotalforCompStr5 = 0.0;
		double newRetMovTotalforCompStr6 = 0.0;
		//Added for calculating the AUR for AZ  curr retail with each competitor
		double currRetailTotforCompStr1 = 0.0;
		double currRetailTotforCompStr2 = 0.0;
		double currRetailTotforCompStr3 = 0.0;
		double currRetailTotforCompStr4 = 0.0;
		double currRetailTotforCompStr5 = 0.0;
		double currRetailTotforCompStr6 = 0.0;
		double curRetMovTotalforCompStr1 = 0.0;
		double curRetMovTotalforCompStr2 = 0.0;
		double curRetMovTotalforCompStr3 = 0.0;
		double curRetMovTotalforCompStr4 = 0.0;
		double curRetMovTotalforCompStr5 = 0.0;
		double curRetMovTotalforCompStr6 = 0.0;
		//added to check no of items whose curr retail is above ,below and equal to each competitor
		int CompStr1VsCurrGreaterCount = 0;
		int CompStr1VsCurrEqualCount = 0;
		int CompStr1VsCurrLesserCount = 0;
		int CompStr2VsCurrGreaterCount = 0;
		int CompStr2VsCurrEqualCount = 0;
		int CompStr2VsCurrLesserCount = 0;
		int CompStr3VsCurrGreaterCount = 0;
		int CompStr3VsCurrEqualCount = 0;
		int CompStr3VsCurrLesserCount = 0;
		int CompStr4VsCurrGreaterCount = 0;
		int CompStr4VsCurrEqualCount = 0;
		int CompStr4VsCurrLesserCount = 0;
		int CompStr5VsCurrGreaterCount = 0;
		int CompStr5VsCurrEqualCount = 0;
		int CompStr5VsCurrLesserCount = 0;
		int CompStr6VsCurrGreaterCount = 0;
		int CompStr6VsCurrEqualCount = 0;
		int CompStr6VsCurrLesserCount = 0;
		
		for (PRItemDTO prItemDTO : itemList) {
			if (prItemDTO.getIsIncludeForSummaryCalculation()) {
				if (!prItemDTO.isLir()) {
					
					double itemMovement = round(prItemDTO.getPredictedMovement(), 0);
					
					prItemDTO.setPredictedMovement(Double.valueOf(itemMovement));
					double currMovement = round(prItemDTO.getCurRegPricePredictedMovement(), 0);
					prItemDTO.setCurRegPricePredictedMovement(Double.valueOf(currMovement));
					double overrideMovement = round(prItemDTO.getOverridePredictedMovement(), 0);
					prItemDTO.setOverridePredictedMovement(Double.valueOf(overrideMovement));
					itemMovement = (Double.isNaN(itemMovement) ? 0.0 : itemMovement);
					currMovement = (Double.isNaN(currMovement) ? 0.0 : currMovement);
					overrideMovement = (Double.isNaN(overrideMovement) ? 0.0 : overrideMovement);
					final double itemSize = prItemDTO.getItemSize();
					final String uomName = (prItemDTO.getUOMName() == null) ? Constants.EMPTY
							: prItemDTO.getUOMName().trim();
					if (prItemDTO.getOverrideRegPrice() > 0.0) {
						if (overrideMovement > 0.0) {
							projectedVolume += this.convertMovementToVolume(uomName, itemSize, overrideMovement);
						}
					} else if (itemMovement > 0.0) {
						projectedVolume += this.convertMovementToVolume(uomName, itemSize, itemMovement);
					}
					if (currMovement > 0.0) {
						currentVolume += this.convertMovementToVolume(uomName, itemSize, currMovement);
					}
				}
				
				double movement = prItemDTO.getMovementData();
				/*  origial movement should be used.
				 * if (movement <= 0.0) { movement = 1.0; }
				 */
				
				final Double CurRegPrice = prItemDTO.getWeightedRegretail();
				final Double recRegUnitPrice = prItemDTO.getWeightedRecRetail();
				final Double comp1UnitPrice = prItemDTO.getWeightedComp1retail();
				final Double comp2UnitPrice = prItemDTO.getWeightedComp2retail();
				final Double comp3UnitPrice = prItemDTO.getWeightedComp3retail();
				final Double comp4UnitPrice = prItemDTO.getWeightedComp4retail();
				final Double comp5UnitPrice = prItemDTO.getWeightedComp5retail();
				//COMP6 IS THE BASE COMPETITOR
				final Double comp6UnitPrice = prItemDTO.getWeightedComp6retail();
				final Double overridenPrice = prItemDTO.getOverrideRegPrice();
				final Double currentUnitPrice = prItemDTO.getRegPrice();
				if (recRegUnitPrice > 0.0 && recRegUnitPrice != null && !recRegUnitPrice.equals(CurRegPrice) && currentUnitPrice>0)
				{
					double recRetail = prItemDTO.getRecommendedRegPrice().getUnitPrice();
					if (overridenPrice > 0.0) {
						recRetail = overridenPrice;
					}
					if (CurRegPrice != 0.0) {
						currRetailTot += CurRegPrice;
						baseMovTotal += movement;
					}
					reccRetailTot += recRegUnitPrice;
					newRetMovTotal += movement;
					if (comp1UnitPrice != null && comp1UnitPrice > 0.0) {
						final double comp1Price = prItemDTO.getComp1Retail().getUnitPrice();
						CompStr1Tot += comp1UnitPrice;
						CompStr1Mov += movement;
						reccRetailTotforCompStr1 += recRegUnitPrice;
						newRetMovTotalforCompStr1 += movement;
						auditReportDTO.setCompStrID1(String.valueOf(prItemDTO.getComp1StrId()));
						if (recRegUnitPrice > 0.0 && recRegUnitPrice != null) {
							final int value = this.checkRangeandApplyvalue(comp1Price, recRetail);
							if (value == 1) {
								CompStr1EqualCount++;
							}  else if (recRetail > comp1Price) {
								CompStr1LesserCount++;
							} else if (recRetail < comp1Price) {
								CompStr1GreaterCount++;
							}
						}
						currRetailTotforCompStr1 += CurRegPrice;
						curRetMovTotalforCompStr1 += movement;
						final int value2 = this.checkRangeandApplyvalue(comp1Price, currentUnitPrice);
						if (value2 == 1) {
							CompStr1VsCurrEqualCount++;
						} else if (currentUnitPrice > comp1Price) {
							++CompStr1VsCurrLesserCount;
						} else if (currentUnitPrice < comp1Price) {
							CompStr1VsCurrGreaterCount++;
						}
					}
					if (comp2UnitPrice != null && comp2UnitPrice > 0.0) {
						final double comp2Price = prItemDTO.getComp2Retail().getUnitPrice();
						auditReportDTO.setCompStrID2(String.valueOf(prItemDTO.getComp2StrId()));
						CompStr2Mov += movement;
						CompStr2Tot += comp2UnitPrice;
						reccRetailTotforCompStr2 += recRegUnitPrice;
						newRetMovTotalforCompStr2 += movement;
						if (recRegUnitPrice > 0.0 && recRegUnitPrice != null) {
							final int value = this.checkRangeandApplyvalue(comp2Price, recRetail);
							if (value == 1) {
								CompStr2EqualCount++;
							} else if (recRetail > comp2Price) {
								CompStr2LesserCount++;
							} else if (recRetail < comp2Price) {
								CompStr2GreaterCount++;
							}
						}
						currRetailTotforCompStr2 += CurRegPrice;
						curRetMovTotalforCompStr2 += movement;
						final int value2 = this.checkRangeandApplyvalue(comp2Price, currentUnitPrice);
						if (value2 == 1) {
							CompStr2VsCurrEqualCount++;
						} else if (currentUnitPrice > comp2Price) {
							CompStr2VsCurrLesserCount++;
						} else if (currentUnitPrice < comp2Price) {
							CompStr2VsCurrGreaterCount++;
						}
					}
					if (comp3UnitPrice != null && comp3UnitPrice > 0.0) {
						final double comp3Price = prItemDTO.getComp3Retail().getUnitPrice();
						auditReportDTO.setCompStrID3(String.valueOf(prItemDTO.getComp3StrId()));
						CompStr3Mov += movement;
						CompStr3Tot += comp3UnitPrice;
						reccRetailTotforCompStr3 += recRegUnitPrice;
						newRetMovTotalforCompStr3 += movement;
						if (recRegUnitPrice > 0.0 && recRegUnitPrice != null) {
							final int value = this.checkRangeandApplyvalue(comp3Price, recRetail);
							if (value == 1) {
								CompStr3EqualCount++;
							} else if (recRetail > comp3Price) {
								CompStr3LesserCount++;
							} else if (recRetail < comp3Price) {
								CompStr3GreaterCount++;
							}
						}
						
						currRetailTotforCompStr3 += CurRegPrice;
						curRetMovTotalforCompStr3 += movement;
						final int value2 = this.checkRangeandApplyvalue(comp3Price, currentUnitPrice);
						if (value2 == 1) {
							CompStr3VsCurrEqualCount++;
						} else if (currentUnitPrice > comp3Price) {
							CompStr3VsCurrLesserCount++;
						} else if (currentUnitPrice < comp3Price) {
							CompStr3VsCurrGreaterCount++;
						}
					}
					if (comp4UnitPrice != null && comp4UnitPrice > 0.0) {
						final double comp4Price = prItemDTO.getComp4Retail().getUnitPrice();
						auditReportDTO.setCompStrID4(String.valueOf(prItemDTO.getComp4StrId()));
						CompStr4Tot += comp4UnitPrice;
						CompStr4Mov += movement;
						reccRetailTotforCompStr4 += recRegUnitPrice;
						newRetMovTotalforCompStr4 += movement;
						if (recRegUnitPrice > 0.0 && recRegUnitPrice != null) {
							final int value = this.checkRangeandApplyvalue(comp4Price, recRetail);
							if (value == 1) {
								CompStr4EqualCount++;
							} else if (recRetail > comp4Price) {
								CompStr4LesserCount++;
							} else if (recRetail < comp4Price) {
								CompStr4GreaterCount++;
							}
						}
						currRetailTotforCompStr4 += CurRegPrice;
						curRetMovTotalforCompStr4 += movement;
						final int value2 = this.checkRangeandApplyvalue(comp4Price, currentUnitPrice);
						if (value2 == 1) {
							CompStr4VsCurrEqualCount++;
						} else if (currentUnitPrice > comp4Price) {
							CompStr4VsCurrLesserCount++;
						} else if (currentUnitPrice < comp4Price) {
							CompStr4VsCurrGreaterCount++;
						}
					}
					if (comp5UnitPrice != null && comp5UnitPrice > 0.0) {
						final double comp5Price = prItemDTO.getComp5Retail().getUnitPrice();
						auditReportDTO.setCompStrID5(String.valueOf(prItemDTO.getComp5StrId()));
						CompStr5Tot += comp5UnitPrice;
						CompStr5Mov += movement;
						reccRetailTotforCompStr5 += recRegUnitPrice;
						newRetMovTotalforCompStr5 += movement;
						if (recRegUnitPrice > 0.0 && recRegUnitPrice != null) {
							final int value = this.checkRangeandApplyvalue(comp5Price, recRetail);
							if (value == 1) {
								CompStr5EqualCount++;
							} else if (recRetail > comp5Price) {
								CompStr5LesserCount++;
							} else if (recRetail < comp5Price) {
								CompStr5GreaterCount++;
							}
						}
						
						currRetailTotforCompStr5 += CurRegPrice;
						curRetMovTotalforCompStr5 += movement;
						final int value2 = this.checkRangeandApplyvalue(comp5Price, currentUnitPrice);
						if (value2 == 1) {
							CompStr5VsCurrEqualCount++;
						} else if (currentUnitPrice > comp5Price) {
							CompStr5VsCurrLesserCount++;
						} else if (currentUnitPrice < comp5Price) {
							CompStr5VsCurrGreaterCount++;
						}
					}
					if (comp6UnitPrice != null && comp6UnitPrice > 0.0) {
						final double comp6Price = prItemDTO.getCompPrice().getUnitPrice();
						auditReportDTO.setCompStrID6(String.valueOf(prItemDTO.getComp6StrId()));
						CompStr6Tot += comp6UnitPrice;
						CompStr6Mov += movement;
						reccRetailTotforCompStr6 += recRegUnitPrice;
						newRetMovTotalforCompStr6 += movement;
						if (recRegUnitPrice > 0.0 && recRegUnitPrice != null) {
							final int value = this.checkRangeandApplyvalue(comp6Price, recRetail);
							if (value == 1) {
								CompStr6EqualCount++;
							} else if (recRetail > comp6Price) {
								CompStr6LesserCount++;
							} else if (recRetail < comp6Price) {
								CompStr6GreaterCount++;
							}
						}
						currRetailTotforCompStr6 += CurRegPrice;
						curRetMovTotalforCompStr6 += movement;
						
						final int value2 = this.checkRangeandApplyvalue(comp6Price, currentUnitPrice);
						if (value2 == 1) {
							CompStr6VsCurrEqualCount++;
						} else if (currentUnitPrice > comp6Price) {
							CompStr6VsCurrLesserCount++;
						}  else if (currentUnitPrice < comp6Price) {
							CompStr6VsCurrGreaterCount++;
						}
					}
				}
				
			}
		
		}
		auditReportDTO.setProjectedMovementVol(projectedVolume);
		auditReportDTO.setMovementVol(currentVolume);
		auditReportDTO.setBaseRetAUR(round(currRetailTot / baseMovTotal, 2));
		auditReportDTO.setBaseRecRetAUR(round(reccRetailTot / newRetMovTotal, 2));
		if (CompStr1Tot > 0.0) {
			auditReportDTO.setCompStr1AUR(round(CompStr1Tot / CompStr1Mov, 2));
		} else {
			auditReportDTO.setCompStr1AUR(0.0);
		}
		if (CompStr2Tot > 0.0) {
			auditReportDTO.setCompStr2AUR(round(CompStr2Tot / CompStr2Mov, 2));
		} else {
			auditReportDTO.setCompStr2AUR(0.0);
		}
		if (CompStr3Tot > 0.0) {
			auditReportDTO.setCompStr3AUR(round(CompStr3Tot / CompStr3Mov, 2));
		} else {
			auditReportDTO.setCompStr3AUR(0.0);
		}
		if (CompStr4Tot > 0.0) {
			auditReportDTO.setCompStr4AUR(round(CompStr4Tot / CompStr4Mov, 2));
		} else {
			auditReportDTO.setCompStr4AUR(0.0);
		}
		if (CompStr5Tot > 0.0) {
			auditReportDTO.setCompStr5AUR(round(CompStr5Tot / CompStr5Mov, 2));
		} else {
			auditReportDTO.setCompStr5AUR(0.0);
		}
		if (CompStr6Tot > 0.0) {
			auditReportDTO.setCompStr6AUR(round(CompStr6Tot / CompStr6Mov, 2));
		} else {
			auditReportDTO.setCompStr6AUR(0.0);
		}
		
		auditReportDTO.setRetlessComp1(CompStr1GreaterCount);
		auditReportDTO.setRetequalComp1(CompStr1EqualCount);
		auditReportDTO.setRetgreaterComp1(CompStr1LesserCount);
		auditReportDTO.setRetlessComp2(CompStr2GreaterCount);
		auditReportDTO.setRetequalComp2(CompStr2EqualCount);
		auditReportDTO.setRetgreaterComp2(CompStr2LesserCount);
		auditReportDTO.setRetlessComp3(CompStr3GreaterCount);
		auditReportDTO.setRetequalComp3(CompStr3EqualCount);
		auditReportDTO.setRetgreaterComp3(CompStr3LesserCount);
		auditReportDTO.setRetlessComp4(CompStr4GreaterCount);
		auditReportDTO.setRetequalComp4(CompStr4EqualCount);
		auditReportDTO.setRetgreaterComp4(CompStr4LesserCount);
		auditReportDTO.setRetlessComp5(CompStr5GreaterCount);
		auditReportDTO.setRetequalComp5(CompStr5EqualCount);
		auditReportDTO.setRetgreaterComp5(CompStr5LesserCount);
		auditReportDTO.setRetlessComp6(CompStr6GreaterCount);
		auditReportDTO.setRetequalComp6(CompStr6EqualCount);
		auditReportDTO.setRetgreaterComp6(CompStr6LesserCount);
		auditReportDTO.setRetailChanges(auditReportDTO.getRetailIncreased() + auditReportDTO.getRetailDecreased());
		auditReportDTO
				.setRetailChangesUnq(auditReportDTO.getRetailIncreasedUnq() + auditReportDTO.getRetailDecreasedUnq());
		if (newRetMovTotalforCompStr1 != 0.0) {
			auditReportDTO.setBaseReccVsCompAUR1(round(reccRetailTotforCompStr1 / newRetMovTotalforCompStr1, 2));
		}
		if (newRetMovTotalforCompStr2 != 0.0) {
			auditReportDTO.setBaseReccVsCompAUR2(round(reccRetailTotforCompStr2 / newRetMovTotalforCompStr2, 2));
		}
		if (newRetMovTotalforCompStr3 != 0.0) {
			auditReportDTO.setBaseReccVsCompAUR3(round(reccRetailTotforCompStr3 / newRetMovTotalforCompStr3, 2));
		}
		if (newRetMovTotalforCompStr4 != 0.0) {
			auditReportDTO.setBaseReccVsCompAUR4(round(reccRetailTotforCompStr4 / newRetMovTotalforCompStr4, 2));
		}
		if (newRetMovTotalforCompStr5 != 0.0) {
			auditReportDTO.setBaseReccVsCompAUR5(round(reccRetailTotforCompStr5 / newRetMovTotalforCompStr5, 2));
		}
		if (newRetMovTotalforCompStr6 != 0.0) {
			auditReportDTO.setBaseReccVsCompAUR6(round(reccRetailTotforCompStr6 / newRetMovTotalforCompStr6, 2));
		}
		if (curRetMovTotalforCompStr1 != 0.0) {
			auditReportDTO.setBaseCurrRetailVsCompAUR1(round(currRetailTotforCompStr1 / curRetMovTotalforCompStr1, 2));
		}
		if (curRetMovTotalforCompStr2 != 0.0) {
			auditReportDTO.setBaseCurrRetailVsCompAUR2(round(currRetailTotforCompStr2 / curRetMovTotalforCompStr2, 2));
		}
		if (curRetMovTotalforCompStr3 != 0.0) {
			auditReportDTO.setBaseCurrRetailVsCompAUR3(round(currRetailTotforCompStr3 / curRetMovTotalforCompStr3, 2));
		}
		if (curRetMovTotalforCompStr4 != 0.0) {
			auditReportDTO.setBaseCurrRetailVsCompAUR4(round(currRetailTotforCompStr4 / curRetMovTotalforCompStr4, 2));
		}
		if (curRetMovTotalforCompStr5 != 0.0) {
			auditReportDTO.setBaseCurrRetailVsCompAUR5(round(currRetailTotforCompStr5 / curRetMovTotalforCompStr5, 2));
		}
		if (curRetMovTotalforCompStr6 != 0.0) {
			auditReportDTO.setBaseCurrRetailVsCompAUR6(round(currRetailTotforCompStr6 / curRetMovTotalforCompStr6, 2));
		}
		auditReportDTO.setCurRetgreaterComp1(CompStr1VsCurrLesserCount);
		auditReportDTO.setCurRetequalComp1(CompStr1VsCurrEqualCount);
		auditReportDTO.setCurRetlessComp1(CompStr1VsCurrGreaterCount);
		auditReportDTO.setCurRetgreaterComp2(CompStr2VsCurrLesserCount);
		auditReportDTO.setCurRetequalComp2(CompStr2VsCurrEqualCount);
		auditReportDTO.setCurRetlessComp2(CompStr2VsCurrGreaterCount);
		auditReportDTO.setCurRetgreaterComp3(CompStr3VsCurrLesserCount);
		auditReportDTO.setCurRetequalComp3(CompStr3VsCurrEqualCount);
		auditReportDTO.setCurRetlessComp3(CompStr3VsCurrGreaterCount);
		auditReportDTO.setCurRetgreaterComp4(CompStr4VsCurrLesserCount);
		auditReportDTO.setCurRetequalComp4(CompStr4VsCurrEqualCount);
		auditReportDTO.setCurRetlessComp4(CompStr4VsCurrGreaterCount);
		auditReportDTO.setCurRetgreaterComp5(CompStr5VsCurrLesserCount);
		auditReportDTO.setCurRetequalComp5(CompStr5VsCurrEqualCount);
		auditReportDTO.setCurRetlessComp5(CompStr5VsCurrGreaterCount);
		auditReportDTO.setCurRetgreaterComp6(CompStr6VsCurrLesserCount);
		auditReportDTO.setCurRetequalComp6(CompStr6VsCurrEqualCount);
		auditReportDTO.setCurRetlessComp6(CompStr6VsCurrGreaterCount);
	}
	
	/**
	 * 
	 * @param outOfNormList
	 * @param ligList
	 * @return count of out of norm unique items
	 */
	private int findOutOfNormLig(List<PRItemDTO> outOfNormList, List<PRItemDTO> ligList){
		List<PRItemDTO> OutOfNormLig = new ArrayList<PRItemDTO>(); 
		for(PRItemDTO prItemDTO: outOfNormList){
			for(PRItemDTO prItemDTOLig: ligList){
				if(prItemDTO.getRetLirId() > 0 && prItemDTO.getRetLirId() == prItemDTOLig.getRetLirId()){
					if(!OutOfNormLig.contains(prItemDTOLig)){
						OutOfNormLig.add(prItemDTOLig);
					}
					else if(prItemDTO.getRetLirId() == 0){
						if(!OutOfNormLig.contains(prItemDTO)){
							OutOfNormLig.add(prItemDTO);
						}
					}
				}
			}
			if(prItemDTO.getRetLirId() == 0){
				if(!OutOfNormLig.contains(prItemDTO)){
					OutOfNormLig.add(prItemDTO);
				}
			}
			}
		return OutOfNormLig.size();
	}
	
	/**
	 * 
	 * @param value
	 * @return value multiplied with prediction duration.
	 */
	/*private double getQaurterLevelValue(double value){
		double predictionDuration = Double.parseDouble(PropertyManager.getProperty("PREDICTION_DURATION", "13"));
		double retValue = round(value * predictionDuration, 0);
		return retValue ;
	}*/
	
	/**
	 * adds out of norm items
	 * @param outOfNormList
	 * @param prItemDTO
	 */
	private void addOutOfNorm(List<PRItemDTO> outOfNormList, PRItemDTO prItemDTO){
		if(!outOfNormList.contains(prItemDTO)){
			outOfNormList.add(prItemDTO);
		}
	}
	
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    long factor = (long) Math.pow(10, places);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}
	
	/**
	 * 
	 * @param revenue
	 * @param margin
	 * @return gives margin %
	 */
	@SuppressWarnings("unused")
	private double findMarginPCT(double revenue, double margin) {
		if (revenue != 0)
			return (margin / revenue) * 100;
		else
			return 0;
	}

	/**
	 * 
	 * @param uomName
	 * @param itemSize
	 * @param movementByUnits
	 * @return volume in LB
	 */
	private double convertMovementToVolume(String uomName, double itemSize, double movementByUnits) {
		double movementByVolume = 0;

		if (itemSize == 0)
			itemSize = 1;

		if (uomName.equals("LB")) // Pound LB = 16* oz
			movementByVolume = movementByUnits * itemSize * 16;
		else if (uomName.equals("OZ")) // Ounce 16 OZ = 1LB
			movementByVolume = movementByUnits * itemSize * 1;
		else if (uomName.equals("ML")) // Milli liter 1 ml = 0.0338140225589 OZ
			movementByVolume = movementByUnits * itemSize * 0.0338140225589;
		else if (uomName.equals("QT")) // Quart Q = 32OZ
			movementByVolume = movementByUnits * itemSize * 32;
		else if (uomName.equals("GA")) // Gallon G = 128 OZ
			movementByVolume = movementByUnits * itemSize * 128;
		else if (uomName.equals("PT")) // Pint = 16 oz
			movementByVolume = movementByUnits * itemSize * 16;
		else if (uomName.equals("GR")) // Gram 1 Gram = 0.0352739619 OZ
			movementByVolume = movementByUnits * itemSize * 0.0352739619;
		else if (uomName.equals("LT")) // Litre = 33.8140227
			movementByVolume = movementByUnits * itemSize * 33.8140227;
		else if (uomName.equals("FZ")) // Fluid ounces FZ = OZ
			movementByVolume = movementByUnits * itemSize * 1;
		else if (uomName.equals("EA")) // Fluid ounces FZ = OZ
			movementByVolume = movementByUnits * itemSize * 1;
		else
			movementByVolume = 0;

		if (movementByVolume > 0)
			return convertOZToLB(movementByVolume);
		else
			return movementByVolume;
	}

	private double convertOZToLB(double movementByOZ) {
		double movementByLB = 0;

		if (movementByOZ != 0)
			movementByLB = movementByOZ / 16;

		return movementByLB;
	}

	/**
	 * @param uniqueMap
	 * @param prItemDTO
	 * @param filterType
	 */
	private void addUniqueItems(HashMap<Integer, List<PRItemDTO>> uniqueMap, PRItemDTO prItemDTO, int filterType) {
		if (uniqueMap.get(filterType) == null) {
			List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
			itemList.add(prItemDTO);
			uniqueMap.put(filterType, itemList);
		} else {
			List<PRItemDTO> itemList = uniqueMap.get(filterType);
			boolean canBeAdded = true;
			for (PRItemDTO itemDTO : itemList) {
				if (itemDTO.getRetLirId() == prItemDTO.getRetLirId()) {
					canBeAdded = false;
				}
			}
			if (canBeAdded || prItemDTO.getRetLirId() == 0) {
				itemList.add(prItemDTO);
				uniqueMap.put(filterType, itemList);
			}
		}
	}

	/**
	 * adds items into metric types
	 * 
	 * @param itemMap
	 * @param prItemDTO
	 * @param filterType
	 */
	private void addItems(HashMap<Integer, List<PRItemDTO>> itemMap, PRItemDTO prItemDTO, int filterType) {
		if (itemMap.get(filterType) == null) {
			List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
			itemList.add(prItemDTO);
			itemMap.put(filterType, itemList);
		} else {
			List<PRItemDTO> itemList = itemMap.get(filterType);
			itemList.add(prItemDTO);
			itemMap.put(filterType, itemList);
		}
	}

	/**
	 * populates unique items count based on metric type
	 * 
	 * @param uniqueMap
	 * @param auditReportDTO
	 */
	private void setUniqueItemsCount(HashMap<Integer, List<PRItemDTO>> uniqueMap, AuditReportDTO auditReportDTO) {
		for (Map.Entry<Integer, List<PRItemDTO>> entry : uniqueMap.entrySet()) {
			int key = entry.getKey();
			/*if (key == AuditFilterTypes.RETAILS_CHANGED.getAuditParamId()) {
				auditReportDTO.setRetailChangesUnq(entry.getValue().size());
			} else*/ if (key == AuditFilterTypes.RETAIL_DECREASED.getAuditParamId()) {
				auditReportDTO.setRetailDecreasedUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAIL_INCREASED.getAuditParamId()) {
				auditReportDTO.setRetailIncreasedUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_OVERIDDEN.getAuditParamId()) {
				auditReportDTO.setRetailOverriddenUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.MANAGER_REVIEW.getAuditParamId()) {
				auditReportDTO.setRetailMarkedUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.LOWER_SIZES_NOT_PRICED_LOWER.getAuditParamId()) {
				auditReportDTO.setLowerSizeHigherPriceUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.SIZE_REL_VIOL.getAuditParamId()) {
				auditReportDTO.setLowerHigherPriceVariationUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_LT_LC.getAuditParamId()) {
				auditReportDTO.setRetailBelowListCostUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_LT_LC_NO_PROMO.getAuditParamId()) {
				auditReportDTO.setRetailBelowListCostNoVIPUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_LT_PROMO.getAuditParamId()) {
				auditReportDTO.setRetailBelowVIPCostUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.MARGIN_LT_X_PCT.getAuditParamId()) {
				auditReportDTO.setMarginLTPCTUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.MARGIN_RT_X_PCT.getAuditParamId()) {
				auditReportDTO.setMarginGTPCTUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.MARGIN_VIOLATION.getAuditParamId()) {
				auditReportDTO.setMarginViolationUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.BRAND_VIOLATION.getAuditParamId()) {
				auditReportDTO.setBrandViolationUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.SIZE_VIOLATION.getAuditParamId()) {
				auditReportDTO.setSizeViolationUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAIL_CHANGE_LT_X_PCT.getAuditParamId()) {
				auditReportDTO.setRetailChangedLTPCTUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAIL_CHANGE_GT_X_PCT.getAuditParamId()) {
				auditReportDTO.setRetailChangedGTPCTUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_CHANGE_VIOLATION.getAuditParamId()) {
				auditReportDTO.setRetailChangedViolationUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_TO_REDUCE_MAR_OPP.getAuditParamId()) {
				auditReportDTO.setReducedRetailsToIncreaseMarginOppUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_LT_X_PCT_COMP.getAuditParamId()) {
				auditReportDTO.setRetailsLTPCTOfPrimaryCompUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_GT_X_PCT_COMP.getAuditParamId()) {
				auditReportDTO.setRetailsGTPCTOfPrimaryCompUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_COMP_VIOLATION.getAuditParamId()) {
				auditReportDTO.setRetailsPrimaryCompViolationUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.ZERO_COST.getAuditParamId()) {
				auditReportDTO.setZeroCostUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.ZERO_CURR_RETAILS.getAuditParamId()) {
				auditReportDTO.setZeroCurrRetailsUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.COST_CAHNGE_GT_X_PCT.getAuditParamId()) {
				auditReportDTO.setListCostChangedGTPCTUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAIL_CHANGE_X_TIMES.getAuditParamId()) {
				auditReportDTO.setRetailChangedNtimesUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.KVI_WITH_COMP_X_MONTHS_OLD.getAuditParamId()) {
				auditReportDTO.setKVICompPriceOldUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.KVI_WITH_NO_COMP.getAuditParamId()) {
				auditReportDTO.setKVIWithNoCompPriceCountUnq(entry.getValue().size());
			} else if (key == AuditFilterTypes.ENDING_DIGIT_VIOL.getAuditParamId()) {
				auditReportDTO.setRetailChangedRoundingViolUnq(entry.getValue().size());
			}
		}
	}

	/**
	 * populates items count based on metric type
	 * 
	 * @param itemMap
	 * @param auditReportDTO
	 */
	private void setItemsCount(HashMap<Integer, List<PRItemDTO>> itemMap, AuditReportDTO auditReportDTO) {
		for (Map.Entry<Integer, List<PRItemDTO>> entry : itemMap.entrySet()) {
			int key = entry.getKey();
			/*
			 * if (key == AuditFilterTypes.RETAILS_CHANGED.getAuditParamId()) {
			 * auditReportDTO.setRetailChanges(entry.getValue().size()); } else
			 */if (key == AuditFilterTypes.RETAIL_DECREASED.getAuditParamId()) {
				auditReportDTO.setRetailDecreased(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAIL_INCREASED.getAuditParamId()) {
				auditReportDTO.setRetailIncreased(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_OVERIDDEN.getAuditParamId()) {
				auditReportDTO.setRetailOverridden(entry.getValue().size());
			} else if (key == AuditFilterTypes.MANAGER_REVIEW.getAuditParamId()) {
				auditReportDTO.setRetailMarked(entry.getValue().size());
			} else if (key == AuditFilterTypes.LOWER_SIZES_NOT_PRICED_LOWER.getAuditParamId()) {
				auditReportDTO.setLowerSizeHigherPrice(entry.getValue().size());
			} else if (key == AuditFilterTypes.SIZE_REL_VIOL.getAuditParamId()) {
				auditReportDTO.setLowerHigherPriceVariation(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_LT_LC.getAuditParamId()) {
				auditReportDTO.setRetailBelowListCost(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_LT_LC_NO_PROMO.getAuditParamId()) {
				auditReportDTO.setRetailBelowListCostNoVIP(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_LT_PROMO.getAuditParamId()) {
				auditReportDTO.setRetailBelowVIPCost(entry.getValue().size());
			} else if (key == AuditFilterTypes.MARGIN_LT_X_PCT.getAuditParamId()) {
				auditReportDTO.setMarginLTPCT(entry.getValue().size());
			} else if (key == AuditFilterTypes.MARGIN_RT_X_PCT.getAuditParamId()) {
				auditReportDTO.setMarginGTPCT(entry.getValue().size());
			} else if (key == AuditFilterTypes.MARGIN_VIOLATION.getAuditParamId()) {
				auditReportDTO.setMarginViolation(entry.getValue().size());
			} else if (key == AuditFilterTypes.BRAND_VIOLATION.getAuditParamId()) {
				auditReportDTO.setBrandViolation(entry.getValue().size());
			} else if (key == AuditFilterTypes.SIZE_VIOLATION.getAuditParamId()) {
				auditReportDTO.setSizeViolation(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAIL_CHANGE_LT_X_PCT.getAuditParamId()) {
				auditReportDTO.setRetailChangedLTPCT(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAIL_CHANGE_GT_X_PCT.getAuditParamId()) {
				auditReportDTO.setRetailChangedGTPCT(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_CHANGE_VIOLATION.getAuditParamId()) {
				auditReportDTO.setRetailChangedViolation(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_TO_REDUCE_MAR_OPP.getAuditParamId()) {
				auditReportDTO.setReducedRetailsToIncreaseMarginOpp(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_LT_X_PCT_COMP.getAuditParamId()) {
				auditReportDTO.setRetailsLTPCTOfPrimaryComp(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_GT_X_PCT_COMP.getAuditParamId()) {
				auditReportDTO.setRetailsGTPCTOfPrimaryComp(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAILS_COMP_VIOLATION.getAuditParamId()) {
				auditReportDTO.setRetailsPrimaryCompViolation(entry.getValue().size());
			} else if (key == AuditFilterTypes.ZERO_COST.getAuditParamId()) {
				auditReportDTO.setZeroCost(entry.getValue().size());
			} else if (key == AuditFilterTypes.ZERO_CURR_RETAILS.getAuditParamId()) {
				auditReportDTO.setZeroCurrRetails(entry.getValue().size());
			} else if (key == AuditFilterTypes.COST_CAHNGE_GT_X_PCT.getAuditParamId()) {
				auditReportDTO.setListCostChangedGTPCT(entry.getValue().size());
			} else if (key == AuditFilterTypes.RETAIL_CHANGE_X_TIMES.getAuditParamId()) {
				auditReportDTO.setRetailChangedNtimes(entry.getValue().size());
			} else if (key == AuditFilterTypes.KVI_WITH_COMP_X_MONTHS_OLD.getAuditParamId()) {
				auditReportDTO.setKVICompPriceOld(entry.getValue().size());
			} else if (key == AuditFilterTypes.KVI_WITH_NO_COMP.getAuditParamId()) {
				auditReportDTO.setKVIWithNoCompPriceCount(entry.getValue().size());
			} else if (key == AuditFilterTypes.ENDING_DIGIT_VIOL.getAuditParamId()) {
				auditReportDTO.setRetailChangedRoundingViol(entry.getValue().size());
			}
		}
	}

	/**
	 * Initializes connection. Used when program is accessed through webservice
	 */
	protected void initializeForWS() {
		setConnection(getDSConnection());
		System.out.println("Connection : " + getConnection());
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
	
	/**
	 * Initializes connection
	 */
	protected void initialize(){
		setConnection();
	}
	
	/**
	 * This function will check if the diff btwn comp and recretail <= threshold and consider it as equal
	 * This is added to handle rounding for AZ 
	 * @param compValue
	 * @param recRetail
	 * @return
	 */
	public int checkRangeandApplyvalue(double compValue, double recRetail) {
		int value = 0;

		if (compValue > 0 && compValue < 11) {
			double threshold = 0.1;
			double absDiff = round(Math.abs(compValue - recRetail),2);
			if (absDiff <= threshold) {
				value = 1;
			}

		} else if (compValue >= 11 && compValue < 100) {
			double threshold = 0.49;
			double absDiff = round(Math.abs(compValue - recRetail),2);
			if (absDiff <= threshold) {
				value = 1;
			}

		} else if (compValue >= 100 && compValue <= 10000) {
			double threshold = 0.99;
			double absDiff = round(Math.abs(compValue - recRetail),2);;
			if (absDiff <= threshold) {
				value = 1;
			}
		}

		return value;
	}

	public int getBatchLocationLevelId() {
		return batchLocationLevelId;
	}
	public void setBatchLocationLevelId(int batchLocationLevelId) {
		this.batchLocationLevelId = batchLocationLevelId;
	}
	public int getBatchLocationId() {
		return batchLocationId;
	}
	public void setBatchLocationId(int batchLocationId) {
		this.batchLocationId = batchLocationId;
	}
	public int getBatchProductLevelId() {
		return batchProductLevelId;
	}
	public void setBatchProductLevelId(int batchProductLevelId) {
		this.batchProductLevelId = batchProductLevelId;
	}
	public int getBatchProductId() {
		return batchProductId;
	}
	public void setBatchProductId(int batchProductId) {
		this.batchProductId = batchProductId;
	}
	public int getBatchStrategyId() {
		return batchStrategyId;
	}
	public void setBatchStrategyId(int batchStrategyId) {
		this.batchStrategyId = batchStrategyId;
	}
	public String getBatchInputDate() {
		return batchInputDate;
	}
	public void setBatchInputDate(String batchInputDate) {
		this.batchInputDate = batchInputDate;
	}

	public String getChainId() {
		return chainId;
	}

	public void setChainId(String chainId) {
		this.chainId = chainId;
	}

	public int getDivisionId() {
		return divisionId;
	}

	public void setDivisionId(int divisionId) {
		this.divisionId = divisionId;
	}

}
