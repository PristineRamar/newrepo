package com.pristine.service.offermgmt;


import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.AuditEngineDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dataload.offermgmt.AuditFilterTypes;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.audittool.AuditDashboardDTO;
import com.pristine.dto.offermgmt.audittool.AuditParameterDTO;
import com.pristine.dto.offermgmt.audittool.AuditParameterHeaderDTO;
import com.pristine.dto.offermgmt.audittool.AuditReportDTO;
import com.pristine.dto.offermgmt.audittool.AuditReportHeaderDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.webservice.AuditFilter;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

/**
 * 
 * @author Pradeepkumar
 *
 */
public class AuditService {
	private static Logger logger = Logger.getLogger("AuditService");
	private Connection conn = null;
	private boolean isOnline;
	private Context initContext;
	private Context envContext;
	private DataSource ds;
	public AuditService(){
		String runType = PropertyManager.getProperty("PR_RUN_TYPE");
//		logger.debug("Run Type - " + runType);
		if(runType != null && PRConstants.RUN_TYPE_ONLINE == runType.charAt(0)){
			isOnline = true;
		}else
			isOnline = false;
	}
	
	public void filterAuditItems(long runId, long reportId, int filterType, AuditFilter auditFilter) throws GeneralException{
		List<String> filteredItems = null;
		
		AuditEngineDAO auditEngineDAO = new AuditEngineDAO();
		try{
			if(isOnline)
				initializeForWS();
			else{
				initialize();
			}
			
			AuditReportHeaderDTO auditReportHeaderDTO = new AuditReportHeaderDTO();
			auditReportHeaderDTO.setRunId(runId);
			SimpleDateFormat formatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
			logger.info("Getting recommendation info...");
			List<PRItemDTO> recommendedItems = auditEngineDAO.getAuditDetailFromRecommendation(getConnection(), auditReportHeaderDTO);
			AuditDashboardDTO auditDashboardDTO = auditEngineDAO.getAuditDashboardData(getConnection(), reportId);
			logger.info("Getting recommendation is completed.");
			logger.info("Getting audit parameters...");
			AuditParameterHeaderDTO auditParameterHeaderDTO = auditEngineDAO.getAuditParamHeaderForReportId(getConnection(), reportId);
			if(auditParameterHeaderDTO == null){
				throw new GeneralException("Unable to proceed without audit parameters.");
			}
			List<AuditParameterDTO> auditParams = auditEngineDAO.getAuditParameters(getConnection(),
					auditParameterHeaderDTO.getParamHeaderId(), auditParameterHeaderDTO.getApVerId());
			logger.info("Getting audit parameters is completed.");
			
			Date startDate = auditEngineDAO.getStartDateFromRecommendation(getConnection(), runId);
			if (startDate == null) {
				logger.error("Error -- runAuditing() - Unable to get startDate of recommendation");
				throw new GeneralException(
						PRConstants.MESSAGE_AUDIT_ERROR + " - Unable to get startDate of recommendation");
			}
			String startDateStr = formatter.format(startDate);
			
			logger.info("Filtering items...");
			if(filterType == AuditFilterTypes.RETAILS_CHANGED.getAuditParamId()){
				filteredItems = getRetailsChangedItems(recommendedItems);
			}
			else if(filterType == AuditFilterTypes.RETAIL_DECREASED.getAuditParamId()){
				filteredItems = getRetailsDecreasedItems(recommendedItems);
			}
			else if(filterType == AuditFilterTypes.RETAIL_INCREASED.getAuditParamId()){
				filteredItems = getRetailsIncreasedItems(recommendedItems);
			}
			else if(filterType == AuditFilterTypes.RETAILS_OVERIDDEN.getAuditParamId()){
				filteredItems = getRetailsOverriddenItems(recommendedItems);
			}
			else if(filterType == AuditFilterTypes.MANAGER_REVIEW.getAuditParamId()){
				filteredItems = getMarkedItems(recommendedItems);
			}
			else if(filterType == AuditFilterTypes.OUT_OF_NORM.getAuditParamId()){
				filteredItems = getOutOfNormItems(recommendedItems, auditParams, auditFilter);
			}
			/*else if(filterType == AuditParameters.LOWER_SIZES_NOT_PRICED_LOWER.getAuditParamId()){
				
			}
			else if(filterType == AuditParameters.SIZE_REL_VIOL.getAuditParamId()){
				
			}*/
			else if(filterType == AuditFilterTypes.RETAILS_LT_LC.getAuditParamId()){
				filteredItems = getRetailsLTListCost(recommendedItems);
			}
			else if(filterType == AuditFilterTypes.RETAILS_LT_LC_NO_PROMO.getAuditParamId()){
				filteredItems = getRetailsLTListCostNoPromo(recommendedItems);
			}
			else if(filterType == AuditFilterTypes.RETAILS_LT_PROMO.getAuditParamId()){
				filteredItems = getRetailsLTPromoCost(recommendedItems);
			}
			else if(filterType == AuditFilterTypes.MARGIN_LT_X_PCT.getAuditParamId()){
				filteredItems = getMarginLTXPctItems(recommendedItems, auditParams, auditFilter);
			}
			else if(filterType == AuditFilterTypes.MARGIN_RT_X_PCT.getAuditParamId()){
				filteredItems = getMarginGTXPctItems(recommendedItems, auditParams, auditFilter);
			}
			else if(filterType == AuditFilterTypes.MARGIN_VIOLATION.getAuditParamId()){
				filteredItems = getMarginViolation(recommendedItems, auditParams, auditFilter);
			}
			
			else if(filterType == AuditFilterTypes.BRAND_VIOLATION.getAuditParamId()){
				filteredItems = getBrandViolation(recommendedItems, auditFilter);
			}
			else if(filterType == AuditFilterTypes.SIZE_VIOLATION.getAuditParamId()){
				filteredItems = getSizeViolation(recommendedItems, auditFilter);
			}			
			
			else if(filterType == AuditFilterTypes.RETAILS_LT_X_PCT_COMP.getAuditParamId()){
				filteredItems = getRetailBelowXPctOfPriComp(recommendedItems, auditParams, auditFilter);
			}
			else if(filterType == AuditFilterTypes.RETAILS_GT_X_PCT_COMP.getAuditParamId()){
				filteredItems = getRetailAboveXPctOfPriComp(recommendedItems, auditParams, auditFilter);
			}
			else if(filterType == AuditFilterTypes.RETAILS_COMP_VIOLATION.getAuditParamId()){
				filteredItems = getRetailPriCompViolation(recommendedItems, auditParams, auditFilter);
			}
			else if(filterType == AuditFilterTypes.RETAIL_CHANGE_LT_X_PCT.getAuditParamId()){
				filteredItems = getRetailChangeLTXPct(recommendedItems, auditParams, auditFilter);
			}
			else if(filterType == AuditFilterTypes.RETAIL_CHANGE_GT_X_PCT.getAuditParamId()){
				filteredItems = getRetailChangeGTXPct(recommendedItems, auditParams, auditFilter);
			}
			else if(filterType == AuditFilterTypes.RETAILS_CHANGE_VIOLATION.getAuditParamId()){
				filteredItems = getRetailChangeViolation(recommendedItems, auditParams, auditFilter);
			}
			else if(filterType == AuditFilterTypes.RETAILS_TO_REDUCE_MAR_OPP.getAuditParamId()){
				filteredItems = getRetailsToReduceForMarginOpp(recommendedItems);
			}
			else if(filterType == AuditFilterTypes.ZERO_COST.getAuditParamId()){
				filteredItems = getZeroCost(recommendedItems);
			}
			else if(filterType == AuditFilterTypes.ZERO_CURR_RETAILS.getAuditParamId()){
				filteredItems = getZeroCurrRetails(recommendedItems);
			}
			else if(filterType == AuditFilterTypes.RETAIL_CHANGE_X_TIMES.getAuditParamId()){
				filteredItems = getRetailsChangedXTimes(recommendedItems, auditParams, auditFilter, runId, auditDashboardDTO.getLocationId());
			}
			else if(filterType == AuditFilterTypes.COST_CAHNGE_GT_X_PCT.getAuditParamId()){
				filteredItems = getCostChangedGTXPct(recommendedItems, auditParams, auditFilter, runId, auditDashboardDTO.getLocationId());
			}
			else if(filterType == AuditFilterTypes.LINE_PRICE_VIOLATIONS.getAuditParamId()){
				filteredItems = getLinePriceViolations(recommendedItems);
			}
			else if(filterType == AuditFilterTypes.KVI_WITH_COMP_X_MONTHS_OLD.getAuditParamId()){
				filteredItems = getKviItemWithCompPriceXmonthsOld(auditDashboardDTO, recommendedItems, startDateStr, startDate);
			}
			else if(filterType == AuditFilterTypes.KVI_WITH_NO_COMP.getAuditParamId()){
				filteredItems = getKviItemWithCompNoComp(auditDashboardDTO, recommendedItems, startDateStr);
			}
			else if(filterType == AuditFilterTypes.ENDING_DIGIT_VIOL.getAuditParamId()){
				filteredItems = getEndingDigitViolations(recommendedItems, auditParams, auditFilter);
			}
			logger.info(filteredItems.size() + " items filtered.");
			if(filteredItems.size() == 1){
				auditFilter.filteredItem = filteredItems.get(0);
			}
			else {
				auditFilter.filteredItems = filteredItems;
			}
		}
		catch(Exception e){
			logger.error("Error -- filterAuditItems() " + e.toString());
			throw new GeneralException("Error -- filterAuditItems()", e);
		}
		finally{
			PristineDBUtil.close(getConnection());
		}
	}
	
/**
 * 	
 * @param itemList
 * @return retail changed items
 */
	private List<String> getRetailsChangedItems(List<PRItemDTO> itemList){
		List<String> filteredItems = new ArrayList<String>();
		for(PRItemDTO prItemDTO: itemList){
			boolean isItemToBeAdded = checkRetailChanged(prItemDTO);
			addItems(prItemDTO, filteredItems, isItemToBeAdded);
		}
		return filteredItems;
	}

/**
 * 
 * @param itemList
 * @return retail decreased items
 */
	private List<String> getRetailsDecreasedItems(List<PRItemDTO> itemList){
		List<String> filteredItems = new ArrayList<String>();
		for(PRItemDTO prItemDTO: itemList){
			boolean isItemToBeAdded = checkRetailDecreased(prItemDTO);
			addItems(prItemDTO, filteredItems, isItemToBeAdded);
		}
		return filteredItems;
	}
	
/**
 * 	
 * @param itemList
 * @return retail increased items
 */
	private List<String> getRetailsIncreasedItems(List<PRItemDTO> itemList){
		List<String> filteredItems = new ArrayList<String>();
		for(PRItemDTO prItemDTO: itemList){
			boolean isItemToBeAdded = checkRetailIncreased(prItemDTO);
			addItems(prItemDTO, filteredItems, isItemToBeAdded);
		}
		return filteredItems;
	}
	
	/**
	 * 
	 * @param itemList
	 * @return retail overridden items
	 */
	private List<String> getRetailsOverriddenItems(List<PRItemDTO> itemList){
		List<String> filteredItems = new ArrayList<String>();
		for(PRItemDTO prItemDTO: itemList){
			boolean isRetailOverridden = checkRetailOverridden(prItemDTO);
			addItems(prItemDTO, filteredItems, isRetailOverridden);
		}
		return filteredItems;
	}
	
	/**
	 * 
	 * @param itemList
	 * @return marked manager review items
	 */
	private List<String> getMarkedItems(List<PRItemDTO> itemList){
		List<String> filteredItems = new ArrayList<String>();
		for(PRItemDTO prItemDTO: itemList){
			boolean isItemMarked = checkItemMarked(prItemDTO);
			addItems(prItemDTO, filteredItems, isItemMarked);
		}
		return filteredItems;
	}
	
	
	/**
	 * 
	 * @param itemList
	 * @param auditParameters
	 * @return all the out of norm items
	 */
	private List<String> getOutOfNormItems(List<PRItemDTO> itemList, List<AuditParameterDTO> auditParameters, AuditFilter auditFilter){
		List<String> filteredItems = new ArrayList<String>();
		filteredItems.addAll(getRetailChangeGTXPct(itemList, auditParameters, auditFilter));
		filteredItems.addAll(getRetailChangeLTXPct(itemList, auditParameters, auditFilter));
		filteredItems.addAll(getRetailsLTListCost(itemList));
		filteredItems.addAll(getRetailsLTListCostNoPromo(itemList));
		filteredItems.addAll(getRetailsLTPromoCost(itemList));
		filteredItems.addAll(getMarginGTXPctItems(itemList, auditParameters, auditFilter));
		filteredItems.addAll(getMarginLTXPctItems(itemList, auditParameters, auditFilter));
		filteredItems.addAll(getRetailAboveXPctOfPriComp(itemList, auditParameters, auditFilter));
		filteredItems.addAll(getRetailBelowXPctOfPriComp(itemList, auditParameters, auditFilter));
		filteredItems.addAll(getZeroCost(itemList));
		filteredItems.addAll(getZeroCurrRetails(itemList));
		auditFilter.auditSetting = 0;
		return filteredItems;
	}
	
	/**
	 * 
	 * @param itemList
	 * @return retails below list cost
	 */
	private List<String> getRetailsLTListCost(List<PRItemDTO> itemList){
		List<String> filteredItems = new ArrayList<String>();
		for(PRItemDTO prItemDTO: itemList){
			boolean isRetailBelowListCost = checkRetailsBelowListCost(prItemDTO);
			addItems(prItemDTO, filteredItems, isRetailBelowListCost);
		}
		return filteredItems;
	}
	
	/**
	 * 
	 * @param itemList
	 * @return retails below list cost when no promo
	 */
	private List<String> getRetailsLTListCostNoPromo(List<PRItemDTO> itemList){
		List<String> filteredItems = new ArrayList<String>();
		for(PRItemDTO prItemDTO: itemList){
			boolean isRetailBelowListCostNoPromo = checkRetailsBelowListCostNoPromo(prItemDTO);
			addItems(prItemDTO, filteredItems, isRetailBelowListCostNoPromo);
		}
		return filteredItems;
	}
	
	
	/**
	 * 
	 * @param itemList
	 * @return retails below promo cost
	 */
	private List<String> getRetailsLTPromoCost(List<PRItemDTO> itemList){
		List<String> filteredItems = new ArrayList<String>();
		for(PRItemDTO prItemDTO: itemList){
			boolean isRetailBelowPromoCost = checkRetailsBelowPromoCost(prItemDTO);
			addItems(prItemDTO, filteredItems, isRetailBelowPromoCost);
		}
		return filteredItems;
	}
	
	/**
	 * 
	 * @param itemList
	 * @param auditParameters
	 * @return margin above x pct 
	 */
	private List<String> getMarginGTXPctItems(List<PRItemDTO> itemList, List<AuditParameterDTO> auditParameters, AuditFilter auditFilter){
		List<String> filteredItems = new ArrayList<String>();
		double marginGTPct = 0;
		for(AuditParameterDTO auditParameterDTO: auditParameters){
			 if(auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.MARGIN_GT_PCT))){
					marginGTPct = auditParameterDTO.getParameterValue();
					auditFilter.auditSetting = marginGTPct;
			}
		}
		for(PRItemDTO prItemDTO: itemList){
			boolean isMarginAboveXPct = checkMarginGTXPct(prItemDTO, marginGTPct);
			addItems(prItemDTO, filteredItems, isMarginAboveXPct);
		}
		return filteredItems;
	}
	

	/**
	 * 
	 * @param itemList
	 * @param auditParameters
	 * @return margin below x %
	 */
	private List<String> getMarginLTXPctItems(List<PRItemDTO> itemList, List<AuditParameterDTO> auditParameters, AuditFilter auditFilter){
		List<String> filteredItems = new ArrayList<String>();
		double marginLTPct = 0;
		for(AuditParameterDTO auditParameterDTO: auditParameters){
			 if(auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.MARGIN_LT_PCT))){
					marginLTPct = auditParameterDTO.getParameterValue();
					auditFilter.auditSetting = marginLTPct;
			}
		}
		for(PRItemDTO prItemDTO: itemList){
			boolean isMarginBelowXPct = checkMarginLTXPct(prItemDTO, marginLTPct);
			addItems(prItemDTO, filteredItems, isMarginBelowXPct);
		}
		return filteredItems;
	}
	
	
	/**
	 * 
	 * @param itemList
	 * @param auditParameters
	 * @return margin below x %
	 */
	private List<String> getMarginViolation(List<PRItemDTO> itemList, List<AuditParameterDTO> auditParameters, AuditFilter auditFilter){
		List<String> filteredItems = new ArrayList<String>();
		double marginLTPct = 0;
		double marginGTPct = 0;
		for(AuditParameterDTO auditParameterDTO: auditParameters){
			 if(auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.MARGIN_LT_PCT))){
					marginLTPct = auditParameterDTO.getParameterValue();
			}
			 if(auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.MARGIN_GT_PCT))){
					marginGTPct = auditParameterDTO.getParameterValue();
			}
		}
		auditFilter.auditSettingMessage = "Margin Rate <" + marginLTPct + "% or >" + marginGTPct + "% with current retail";
		for(PRItemDTO prItemDTO: itemList){
			boolean isMarginBelowXPct = checkMarginViolation(prItemDTO, marginLTPct, marginGTPct);
			addItems(prItemDTO, filteredItems, isMarginBelowXPct);
		}
		return filteredItems;
	}	
	
	/*
	 * @param itemList
	 * @return filtered items
	 */
	private List<String> getBrandViolation(List<PRItemDTO> itemList, AuditFilter auditFilter){
		List<String> filteredItems = new ArrayList<String>();

		auditFilter.auditSettingMessage = "Brand violation";
		for(PRItemDTO prItemDTO: itemList){
			boolean isBrandViolation = checkBrandViol(prItemDTO);
			addItems(prItemDTO, filteredItems, isBrandViolation);
		}
		return filteredItems;
	}	
	
	/*
	 * @param itemList
	 * @return filtered items
	 */
	private List<String> getSizeViolation(List<PRItemDTO> itemList, AuditFilter auditFilter){
		List<String> filteredItems = new ArrayList<String>();

		auditFilter.auditSettingMessage = "Size violation";
		for(PRItemDTO prItemDTO: itemList){
			boolean isSizeViolation = checkSizeViol(prItemDTO);
			addItems(prItemDTO, filteredItems, isSizeViolation);
		}
		
		return filteredItems;
	}	
	
	
	/**
	 * 
	 * @param itemList
	 * @param auditParameters
	 * @return retail change below x %
	 */
	private List<String> getRetailChangeLTXPct(List<PRItemDTO> itemList, List<AuditParameterDTO> auditParameters, AuditFilter auditFilter){
		List<String> filteredItems = new ArrayList<String>();
		double retailChangedXPct = 0;
		for(AuditParameterDTO auditParameterDTO: auditParameters){
			 if(auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.RETAIL_CHANGED_LT_PCT))){
				 retailChangedXPct = auditParameterDTO.getParameterValue();
				 auditFilter.auditSetting = retailChangedXPct;
			}
		}
		for(PRItemDTO prItemDTO: itemList){
			boolean isRetailChangeBelowXPct = checkRetailChangeLTXPct(prItemDTO, retailChangedXPct);
			addItems(prItemDTO, filteredItems, isRetailChangeBelowXPct);
		}
		return filteredItems;
	}
	
	/**
	 * 
	 * @param itemList
	 * @param auditParameters
	 * @return retail change above x %
	 */
	
	private List<String> getRetailChangeGTXPct(List<PRItemDTO> itemList, List<AuditParameterDTO> auditParameters, AuditFilter auditFilter){
		List<String> filteredItems = new ArrayList<String>();
		double retailChangedXPct = 0;
		for(AuditParameterDTO auditParameterDTO: auditParameters){
			 if(auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.RETAIL_CHANGED_GT_PCT))){
				 retailChangedXPct = auditParameterDTO.getParameterValue();
				 auditFilter.auditSetting = retailChangedXPct;
			}
		}
		for(PRItemDTO prItemDTO: itemList){
			boolean isRetailChangeAboveXPct = checkRetailChangeGTXPct(prItemDTO, retailChangedXPct);
			addItems(prItemDTO, filteredItems, isRetailChangeAboveXPct);
		}
		return filteredItems;
	}
	
	/**
	 * 
	 * @param itemList
	 * @param auditParameters
	 * @return retail change above x %
	 */
	
	private List<String> getRetailChangeViolation(List<PRItemDTO> itemList, List<AuditParameterDTO> auditParameters, AuditFilter auditFilter){
		List<String> filteredItems = new ArrayList<String>();
		double retailChangedGTXPct = 0;
		double retailChangedLTXPct = 0;
		for(AuditParameterDTO auditParameterDTO: auditParameters){
			 if(auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.RETAIL_CHANGED_LT_PCT))){
				 retailChangedLTXPct = auditParameterDTO.getParameterValue();
			}			
			 if(auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.RETAIL_CHANGED_GT_PCT))){
				 retailChangedGTXPct = auditParameterDTO.getParameterValue();
			}
		}
		auditFilter.auditSettingMessage = "Retail change <" + retailChangedLTXPct + "% or >" + retailChangedGTXPct + "% with current retail";
		for(PRItemDTO prItemDTO: itemList){
			boolean isRetailChangeAboveXPct = checkRetailChangeViolation(prItemDTO, retailChangedLTXPct, retailChangedGTXPct);
			addItems(prItemDTO, filteredItems, isRetailChangeAboveXPct);
		}
		return filteredItems;
	}	
	
	/**
	 * 
	 * @param itemList
	 * @return retails to reduce for margin opportunity
	 */
	
	private List<String> getRetailsToReduceForMarginOpp(List<PRItemDTO> itemList){
		List<String> filteredItems = new ArrayList<String>();
		for(PRItemDTO prItemDTO: itemList){
			boolean isRetailToReduceForMarginOpp = checkRetailsToReduceForMarginOpp(prItemDTO);
			addItems(prItemDTO, filteredItems, isRetailToReduceForMarginOpp);
		}
		return filteredItems;
	}
	
	
	/**
	 * 
	 * @param itemList
	 * @param auditParameters
	 * @param auditFilter
	 * @return retails below xPct of primary comp
	 */
	private List<String> getRetailBelowXPctOfPriComp(List<PRItemDTO> itemList,List<AuditParameterDTO> auditParameters, AuditFilter auditFilter ){
		List<String> filteredItems = new ArrayList<String>();
		double compLTPct = 0;
		for(AuditParameterDTO auditParameterDTO: auditParameters){
			 if(auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.RETAIL_LT_PRIME_COMP_PCT))){
				 compLTPct = auditParameterDTO.getParameterValue();
				 auditFilter.auditSetting = compLTPct;
			}
		}
		for(PRItemDTO prItemDTO: itemList){
			boolean isRetailBelowXPctOfPriComp = checkRetailBelowXPctOfPriComp(prItemDTO, compLTPct);
			addItems(prItemDTO, filteredItems, isRetailBelowXPctOfPriComp);
		}
		return filteredItems;
	}
	
	
	
	/**
	 * 
	 * @param itemList
	 * @param auditParameters
	 * @param auditFilter
	 * @return retails below xPct of primary comp
	 */
	private List<String> getRetailAboveXPctOfPriComp(List<PRItemDTO> itemList,List<AuditParameterDTO> auditParameters, AuditFilter auditFilter ){
		List<String> filteredItems = new ArrayList<String>();
		double compGTPct = 0;
		for(AuditParameterDTO auditParameterDTO: auditParameters){
			 if(auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.RETAIL_GT_PRIME_COMP_PCT))){
				 compGTPct = auditParameterDTO.getParameterValue();
				 auditFilter.auditSetting = compGTPct;
			}
		}
		for(PRItemDTO prItemDTO: itemList){
			boolean isRetailBelowXPctOfPriComp = checkRetailAboveXPctOfPriComp(prItemDTO, compGTPct);
			addItems(prItemDTO, filteredItems, isRetailBelowXPctOfPriComp);
		}
		return filteredItems;
	}

	/**
	 * 
	 * @param itemList
	 * @param auditParameters
	 * @param auditFilter
	 * @return retails below xPct of primary comp
	 */
	private List<String> getRetailPriCompViolation(List<PRItemDTO> itemList,List<AuditParameterDTO> auditParameters, AuditFilter auditFilter ){
		List<String> filteredItems = new ArrayList<String>();
		double compGTPct = 0;
		double compLTPct = 0;
		for(AuditParameterDTO auditParameterDTO: auditParameters){
			if(auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.RETAIL_LT_PRIME_COMP_PCT))){
				 compLTPct = auditParameterDTO.getParameterValue();
			}
			 
			if(auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.RETAIL_GT_PRIME_COMP_PCT))){
				 compGTPct = auditParameterDTO.getParameterValue();
			}
		}
		
		auditFilter.auditSettingMessage = "Retail <" + compLTPct + "% or >" + compGTPct + "% with primary Comp";
		for(PRItemDTO prItemDTO: itemList){
			boolean isRetailBelowXPctOfPriComp = checkRetailPriCompViolation(prItemDTO, compLTPct, compGTPct);
			addItems(prItemDTO, filteredItems, isRetailBelowXPctOfPriComp);
		}
		return filteredItems;
	}	
	
	
	/**
	 * 	
	 * @param itemList
	 * @return current retail zero
	 */
		private List<String> getZeroCurrRetails(List<PRItemDTO> itemList){
			List<String> filteredItems = new ArrayList<String>();
			for(PRItemDTO prItemDTO: itemList){
				boolean isItemToBeAdded = checkZeroCurrentRetail(prItemDTO);
				addItems(prItemDTO, filteredItems, isItemToBeAdded);
			}
			return filteredItems;
		}
		
		/**
		 * 	
		 * @param itemList
		 * @return current retail zero
		 */
			private List<String> getZeroCost(List<PRItemDTO> itemList){
				List<String> filteredItems = new ArrayList<String>();
				for(PRItemDTO prItemDTO: itemList){
					boolean isItemToBeAdded = checkZeroCost(prItemDTO);
					addItems(prItemDTO, filteredItems, isItemToBeAdded);
				}
				return filteredItems;
			}
	
			
	/**
	 * 		
	 * @param conn
	 * @param startDate
	 * @param endDate
	 * @param locationId
	 * @param runId
	 * @return list of items for which the retail changed x times in last x months
	 * @throws GeneralException
	 */
	public Set<Integer> getRetailsChangedXtimesInLastXmonths(Connection conn, String startDate, String endDate,
			int locationId, long runId, int retailChangeNTimes) throws GeneralException {
		AuditEngineDAO auditEngineDAO = new AuditEngineDAO();
		return auditEngineDAO.getRetailChangedXTimesInLastXMonths(conn, startDate, endDate, locationId, runId, retailChangeNTimes);
	}
	
	
	
/**
 * 
 * @param itemList
 * @param auditParameters
 * @param auditFilter
 * @param runId
 * @param locationId
 * @return  items for which the retail changed x times in last x months
 * @throws GeneralException
 */
	private List<String> getRetailsChangedXTimes(List<PRItemDTO> itemList, List<AuditParameterDTO> auditParameters,
			AuditFilter auditFilter, long runId, int locationId) throws GeneralException {
		List<String> filteredItems = new ArrayList<String>();
		AuditEngineDAO auditEngineDAO = new AuditEngineDAO();
		SimpleDateFormat formatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		Date startDate = auditEngineDAO.getStartDateFromRecommendation(getConnection(), runId);
		if (startDate == null) {
			logger.error("Error -- runAuditing() - Unable to get startDate of recommendation");
			throw new GeneralException(
					PRConstants.MESSAGE_AUDIT_ERROR + " - Unable to get startDate of recommendation");
		}
		int retailChangedXTimes = 0;
		for (AuditParameterDTO auditParam : auditParameters) {
			if (auditParam.getParamsType().trim().equals(String.valueOf(PRConstants.RETAIL_CHANGES_X_TIMES))) {
				retailChangedXTimes = (int) auditParam.getParameterValue();
			}
		}
		auditFilter.auditSetting = retailChangedXTimes;
		int retailChangesXMonths = Integer.parseInt(PropertyManager.getProperty("AUDIT_RETAIL_CHANGES_IN_X_MONTHS"));
		
		Date endDate = DateUtil.incrementDate(startDate, -(retailChangesXMonths * 30));
		String startDateStr = formatter.format(startDate);
		String endDateStr = formatter.format(endDate);

		Set<Integer> retailChangeSet = getRetailsChangedXtimesInLastXmonths(getConnection(), startDateStr, endDateStr,
				locationId, runId, retailChangedXTimes);
		for (PRItemDTO prItemDTO : itemList) {
			boolean isRetailsChangedXTimes = checkRetailChangedXTimesInLastXmonths(prItemDTO, retailChangeSet);
			addItems(prItemDTO, filteredItems, isRetailsChangedXTimes);
		}
		return filteredItems;
	}
	
	
	/**
	 * 
	 * @param itemList
	 * @param auditParameters
	 * @param auditFilter
	 * @param runId
	 * @param locationId
	 * @return  items for which the cost change is greater than x% in last x months
	 * @throws GeneralException
	 */
		private List<String> getCostChangedGTXPct(List<PRItemDTO> itemList, List<AuditParameterDTO> auditParameters,
				AuditFilter auditFilter, long runId, int locationId) throws GeneralException {
			List<String> filteredItems = new ArrayList<String>();
			AuditEngineDAO auditEngineDAO = new AuditEngineDAO();
			SimpleDateFormat formatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
			double listCostChangeGTPct = 0;
			for (AuditParameterDTO auditParam : auditParameters) {
				if (auditParam.getParamsType().trim().equals(String.valueOf(PRConstants.LIST_COST_CHANGE_GT_PCT))) {
					listCostChangeGTPct = auditParam.getParameterValue();
				}
			}
			
			auditFilter.auditSetting = listCostChangeGTPct;
			Date startDate = auditEngineDAO.getStartDateFromRecommendation(getConnection(), runId);
			if (startDate == null) {
				logger.error("Error -- runAuditing() - Unable to get startDate of recommendation");
				throw new GeneralException(
						PRConstants.MESSAGE_AUDIT_ERROR + " - Unable to get startDate of recommendation");
			}
			int costChangesXMonths = Integer.parseInt(PropertyManager.getProperty("AUDIT_COST_CHANGES_IN_X_MONTHS"));
			
			Date endDate = DateUtil.incrementDate(startDate, -(costChangesXMonths * 30));
			String startDateStr = formatter.format(startDate);
			String endDateStr = formatter.format(endDate);

			Set<Integer> costChangeSet = getCostChangGtXPctInLastXmonths(getConnection(), startDateStr, endDateStr, locationId, runId, listCostChangeGTPct);
			for (PRItemDTO prItemDTO : itemList) {
				boolean isCostChangedXPctInLastXmonths = checkCostChangedXPctInLastXmonths(prItemDTO, costChangeSet);
				addItems(prItemDTO, filteredItems, isCostChangedXPctInLastXmonths);
			}
			return filteredItems;
		}
	
	
	/**
	 * 		
	 * @param conn
	 * @param startDate
	 * @param endDate
	 * @param locationId
	 * @param runId
	 * @return  list of items for which the cost change is greater than x% in last x months
	 * @throws GeneralException
	 */
	public Set<Integer> getCostChangGtXPctInLastXmonths(Connection conn, String startDate, String endDate,
			int locationId, long runId, double xPct) throws GeneralException {
		AuditEngineDAO auditEngineDAO = new AuditEngineDAO();
		return auditEngineDAO.getCostChangedXPctInLastXMonths(conn, startDate, endDate, locationId, runId, xPct);
	}
			
	/**
	 * adds filtered items
	 * @param prItemDTO
	 * @param filteredItems
	 * @param isItemToBeAdded
	 */
	private void addItems(PRItemDTO prItemDTO, List<String> filteredItems, boolean isItemToBeAdded){
		if(isItemToBeAdded)
			filteredItems.add(String.valueOf(prItemDTO.getItemCode()));
	}
	
	
	/**
	 * 
	 * @param prItemDTO
	 * @return item needs to be added or not
	 */
	public boolean checkRetailChanged(PRItemDTO prItemDTO){
		boolean isItemToBeAdded = false;
//		double recommendedUnitPrice = prItemDTO.getRecommendedRegPrice() / prItemDTO.getRecommendedRegMultiple();
		double recommendedUnitPrice = PRCommonUtil.getUnitPrice(prItemDTO.getRecommendedRegPrice(), true);
		if(prItemDTO.getIsNewPriceRecommended() == 1 && !prItemDTO.isLir()){
			if(recommendedUnitPrice > 0){
				isItemToBeAdded = true;
			}
		}
		return isItemToBeAdded;
	}
	
	/**
	 * 
	 * @param prItemDTO
	 * @return item needs to be added or not
	 */
	
	public boolean checkRetailIncreased(PRItemDTO prItemDTO){
		boolean isItemToBeAdded = false;
		double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
		double currUnitPrice = prItemDTO.getRegPrice();
		if(prItemDTO.getRegMPack() > 1){
			currUnitPrice = prItemDTO.getRegPrice() / prItemDTO.getRegMPack();
		}
		if(!prItemDTO.isLir()){
			if(recommendedUnitPrice > 0 && currUnitPrice > 0){
					//Retail increased
					if(currUnitPrice < recommendedUnitPrice){
						isItemToBeAdded = true;
					}
				}
			}
		return isItemToBeAdded;
	}
	
	/**
	 * 
	 * @param prItemDTO
	 * @return item needs to be added or not
	 */
	
	public boolean checkRetailDecreased(PRItemDTO prItemDTO){
		boolean isItemToBeAdded = false;
		double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
		double currUnitPrice = prItemDTO.getRegPrice();
		if(prItemDTO.getRegMPack() > 1){
			currUnitPrice = prItemDTO.getRegPrice() / prItemDTO.getRegMPack();
		}
		if(!prItemDTO.isLir()){
			if(recommendedUnitPrice > 0 && currUnitPrice > 0){
					//Retail decreased
					if(currUnitPrice > recommendedUnitPrice){
						isItemToBeAdded = true;
					}
				}
			}
		return isItemToBeAdded;
	}
	
	
	/**
	 * 
	 * @param prItemDTO
	 * @return item needs to be added or not
	 */
	public boolean checkRetailOverridden(PRItemDTO prItemDTO){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			double overridePrice = 0;
			if(prItemDTO.getOverrideRegMultiple() > 0){
				overridePrice = prItemDTO.getOverrideRegPrice() / prItemDTO.getOverrideRegMultiple();	
			}
			else{
				overridePrice = prItemDTO.getOverrideRegPrice();
			}
			//Retail overridden
			if(overridePrice > 0){
					isItemToBeAdded = true;
			}
		}
		return isItemToBeAdded;
	}
	
	
	/**
	 * 
	 * @param prItemDTO
	 * @return item needs to be added or not
	 */
	public boolean checkItemMarked(PRItemDTO prItemDTO){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			String markedIndicator = prItemDTO.getIsMarkedForReview() == null ? "" : prItemDTO.getIsMarkedForReview();
				//Retail marked
				if(markedIndicator.equals("Y")){
					isItemToBeAdded = true;
			}
		}
		return isItemToBeAdded;
	}

	/**
	 * 
	 * @param prItemDTO
	 * @return item needs to be added or not
	 */
	public boolean checkRetailsBelowListCost(PRItemDTO prItemDTO){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
			double listCost = prItemDTO.getListCost();
			if(recommendedUnitPrice > 0){
				if(recommendedUnitPrice < listCost){
					isItemToBeAdded = true;
				}
			}
		}
		return isItemToBeAdded;
	}

	
	/**
	 * 
	 * @param prItemDTO
	 * @return item needs to be added or not
	 */
	public boolean checkRetailsBelowListCostNoPromo(PRItemDTO prItemDTO){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
			double listCost = prItemDTO.getListCost();
			//double vipCost = prItemDTO.getVipCost();
			if(recommendedUnitPrice > 0){
			//	if(recommendedUnitPrice < listCost && vipCost == 0){
				if(recommendedUnitPrice < listCost){
					isItemToBeAdded = true;
				}
			}
		}
		return isItemToBeAdded;
	}
	
	/**
	 * 
	 * @param prItemDTO
	 * @return item needs to be added or not
	 */
	public boolean checkRetailsBelowPromoCost(PRItemDTO prItemDTO){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
			double vipCost = prItemDTO.getVipCost();
			if(recommendedUnitPrice > 0){
				if(recommendedUnitPrice < vipCost){
						isItemToBeAdded = true;
				}
			}
		}
		return isItemToBeAdded;
	}
	
	/**
	 * 
	 * @param prItemDTO
	 * @param marginGTPct
	 * @return item needs to be added or not
	 */
	public boolean checkMarginGTXPct(PRItemDTO prItemDTO, double marginGTPct){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			double marginPCT = 0;
			double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
			if(recommendedUnitPrice > 0){
				double listCost = prItemDTO.getListCost();
				//double vipCost = prItemDTO.getVipCost();
				if(listCost > 0){
					/*
					 * if(vipCost > 0) marginPCT = ((recommendedUnitPrice - vipCost) /
					 * recommendedUnitPrice) * 100; else
					 */
						marginPCT = ((recommendedUnitPrice - listCost) / recommendedUnitPrice) * 100;
				}
				if(marginPCT > marginGTPct){
					isItemToBeAdded = true;
				}
			}
		}
		return isItemToBeAdded; 
		
	}
	
	/**
	 * 
	 * @param prItemDTO
	 * @param marginLTPct
	 * @param marginGTPct
	 * @return item needs to be added or not
	 */
	public boolean checkMarginViolation(PRItemDTO prItemDTO, double marginLTPct, double marginGTPct){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			double marginPCT = 0;
			double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
			if(recommendedUnitPrice > 0){
				double listCost = prItemDTO.getListCost();
				//double vipCost = prItemDTO.getVipCost();
				if(listCost > 0){
					/*
					 * if(vipCost > 0) marginPCT = ((recommendedUnitPrice - vipCost) /
					 * recommendedUnitPrice) * 100; else
					 */
						marginPCT = ((recommendedUnitPrice - listCost) / recommendedUnitPrice) * 100;
				}
				
				if(marginPCT < marginLTPct && marginPCT != 0){
					isItemToBeAdded = true;
				}
				
				if(marginPCT > marginGTPct){
					isItemToBeAdded = true;
				}
			}
		}
		return isItemToBeAdded; 
		
	}
	
	/**
	 * 
	 * @param prItemDTO
	 * @param marginLTPct
	 * @return item needs to be added or not
	 */
	public boolean checkMarginLTXPct(PRItemDTO prItemDTO, double marginLTPct){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			double marginPCT = 0;
			double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
			if(recommendedUnitPrice > 0){
				double listCost = prItemDTO.getListCost();
				//double vipCost = prItemDTO.getVipCost();
				if(listCost > 0){
					/*
					 * if(vipCost > 0) marginPCT = ((recommendedUnitPrice - vipCost) /
					 * recommendedUnitPrice) * 100; else
					 */
						marginPCT = ((recommendedUnitPrice - listCost) / recommendedUnitPrice) * 100;
				}
				if(marginPCT < marginLTPct && marginPCT != 0){
					isItemToBeAdded = true;
				}
			}
		}
		return isItemToBeAdded; 
		
	}
	
	
	/**
	 * 
	 * @param prItemDTO
	 * @param retailChangedXPct
	 * @return item needs to be added or not
	 */
	public boolean checkRetailChangeLTXPct(PRItemDTO prItemDTO, double retailChangedXPct){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			double currUnitPrice = prItemDTO.getRegPrice();
			if(prItemDTO.getRegMPack() > 1){
				currUnitPrice = prItemDTO.getRegPrice() / prItemDTO.getRegMPack();
			}
			double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
			if(recommendedUnitPrice > 0){
				double retailDiffPct = 0;
				if(currUnitPrice > 0){
					retailDiffPct = ((recommendedUnitPrice - currUnitPrice) / currUnitPrice) * 100;
					if(Math.abs(retailDiffPct) > 0){
						if(Math.abs(retailDiffPct) < retailChangedXPct){
							isItemToBeAdded = true;
						}
					}
				}	
			}
		}
		return isItemToBeAdded;
	}
	
	/**
	 * 
	 * @param prItemDTO
	 * @param retailChangedXPct
	 * @return item needs to be added or not
	 */
	public boolean checkRetailChangeGTXPct(PRItemDTO prItemDTO, double retailChangedXPct){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			double currUnitPrice = prItemDTO.getRegPrice();
			if(prItemDTO.getRegMPack() > 1){
				currUnitPrice = prItemDTO.getRegPrice() / prItemDTO.getRegMPack();
			}
			double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
			if(recommendedUnitPrice > 0){
				double retailDiffPct = 0;
				if(currUnitPrice > 0){
					retailDiffPct = ((recommendedUnitPrice - currUnitPrice) / currUnitPrice) * 100;
					if(Math.abs(retailDiffPct) > 0){
						if(Math.abs(retailDiffPct) > retailChangedXPct){
							isItemToBeAdded = true;
						}
					}
				}	
			}
		}
		return isItemToBeAdded;
	}

	/**
	 * 
	 * @param prItemDTO
	 * @param retailChangedLTXPct
	 * @param retailChangedGTXPct
	 * @return item needs to be added or not
	 */
	public boolean checkRetailChangeViolation(PRItemDTO prItemDTO, double retailChangedLTXPct, double retailChangedGTXPct){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			double currUnitPrice = prItemDTO.getRegPrice();
			if(prItemDTO.getRegMPack() > 1){
				currUnitPrice = prItemDTO.getRegPrice() / prItemDTO.getRegMPack();
			}
			double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
			if(recommendedUnitPrice > 0){
				double retailDiffPct = 0;
				if(currUnitPrice > 0){
					retailDiffPct = ((recommendedUnitPrice - currUnitPrice) / currUnitPrice) * 100;
					if(Math.abs(retailDiffPct) > 0){
						if(Math.abs(retailDiffPct) < retailChangedLTXPct){
							isItemToBeAdded = true;
						}
						
						if(Math.abs(retailDiffPct) > retailChangedGTXPct){
							isItemToBeAdded = true;
						}
					}
				}	
			}
		}
		return isItemToBeAdded;
	}
	
	
	/**
	 * 
	 * @param prItemDTO
	 * @return item needs to be added or not
	 */
	public boolean checkRetailsToReduceForMarginOpp(PRItemDTO prItemDTO){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			double opportunityPrice = 0; 
			String oppIndicator = prItemDTO.getIsOppurtunity() == null ? "" : prItemDTO.getIsOppurtunity();
			double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
			if(oppIndicator.equals("Y")){
				if(prItemDTO.getOppurtunityPrice() > 0 && prItemDTO.getOppurtunityQty() > 0){
					opportunityPrice = prItemDTO.getOppurtunityPrice() / prItemDTO.getOppurtunityQty();
				}
				else if(prItemDTO.getOppurtunityPrice() > 0){
					opportunityPrice = prItemDTO.getOppurtunityPrice();
				}
			}
			
			if(opportunityPrice > 0 && opportunityPrice < recommendedUnitPrice){
				isItemToBeAdded = true;
			}
		}
		return isItemToBeAdded;
	}

	
	
	/**
	 * 
	 * @param prItemDTO
	 * @param belowXPctComp
	 * @return checks diff % of comp and rec price with given % 
	 */
	public boolean checkRetailAboveXPctOfPriComp(PRItemDTO prItemDTO, double aboveXPctComp) {
		boolean isItemToBeAdded = false;
		if (!prItemDTO.isLir()) {
			if (prItemDTO.getCompPrice() != null) {
				MultiplePrice compPrice = prItemDTO.getCompPrice();
				double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
				double compUnitPrice = 0;
				if (compPrice.multiple > 1) {
					compUnitPrice = compPrice.price / compPrice.multiple;
				} else {
					compUnitPrice = compPrice.price;
				}

				if (compUnitPrice > 0 && recommendedUnitPrice > 0 
						&& recommendedUnitPrice > compUnitPrice) {
					double priceDiff = recommendedUnitPrice - compUnitPrice;
					if(priceDiff > 0){
						double priceDiffPct = (priceDiff / compUnitPrice) * 100;
						if (priceDiffPct > aboveXPctComp) {
							isItemToBeAdded = true;
						}
					}
				}
			}
		}
		return isItemToBeAdded;
	}
	
	/**
	 * 
	 * @param prItemDTO
	 * @param belowXPctComp
	 * @return checks diff % of comp and rec price with given % 
	 */
	public boolean checkRetailBelowXPctOfPriComp(PRItemDTO prItemDTO, double belowXPctComp) {
		boolean isItemToBeAdded = false;
		if (!prItemDTO.isLir()) {
			if (prItemDTO.getCompPrice() != null) {
				MultiplePrice compPrice = prItemDTO.getCompPrice();
				double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
				double compUnitPrice = 0;
				if (compPrice.multiple > 1) {
					compUnitPrice = compPrice.price / compPrice.multiple;
				} else {
					compUnitPrice = compPrice.price;
				}

				if (compUnitPrice > 0 && recommendedUnitPrice > 0 
						&& recommendedUnitPrice < compUnitPrice) {
					double priceDiff = recommendedUnitPrice - compUnitPrice;
					if(priceDiff > 0){
						double priceDiffPct = (priceDiff / compUnitPrice) * 100;
						if (priceDiffPct < belowXPctComp) {
							isItemToBeAdded = true;
						}
					}
				}
			}
		}
		return isItemToBeAdded;
	}
	
	/**
	 * 
	 * @param prItemDTO
	 * @param belowXPctComp
	 * @param aboveXPctComp
	 * @return checks diff % of comp and rec price with given % 
	 */
	public boolean checkRetailPriCompViolation(PRItemDTO prItemDTO, double belowXPctComp, double aboveXPctComp) {
		boolean isItemToBeAdded = false;
		if (!prItemDTO.isLir()) {
			if (prItemDTO.getCompPrice() != null) {
				MultiplePrice compPrice = prItemDTO.getCompPrice();
				double recommendedUnitPrice = checkOverrideAndReturnPrice(prItemDTO);
				double compUnitPrice = 0;
				if (compPrice.multiple > 1) {
					compUnitPrice = compPrice.price / compPrice.multiple;
				} else {
					compUnitPrice = compPrice.price;
				}

				if (compUnitPrice > 0 && recommendedUnitPrice > 0 
						&& recommendedUnitPrice < compUnitPrice) {
					double priceDiff = recommendedUnitPrice - compUnitPrice;
					if(priceDiff > 0){
						double priceDiffPct = (priceDiff / compUnitPrice) * 100;
						if (priceDiffPct < belowXPctComp) {
							isItemToBeAdded = true;
						}
					}
				}
				
				if (compUnitPrice > 0 && recommendedUnitPrice > 0 
						&& recommendedUnitPrice > compUnitPrice) {
					double priceDiff = recommendedUnitPrice - compUnitPrice;
					if(priceDiff > 0){
						double priceDiffPct = (priceDiff / compUnitPrice) * 100;
						if (priceDiffPct > aboveXPctComp) {
							isItemToBeAdded = true;
						}
					}
				}				
				
			}
		}
		return isItemToBeAdded;
	}	
	
	/**
	 * 
	 * @param prItemDTO
	 * @return check cost is zero
	 */
	public boolean checkZeroCost(PRItemDTO prItemDTO){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			double currentCost = prItemDTO.getCost();
			if(currentCost == 0){
				isItemToBeAdded = true;
			}
		}
		return isItemToBeAdded;
	}
	
	
	/**
	 * 
	 * @param prItemDTO
	 * @return check current price is zero
	 */
	public boolean checkZeroCurrentRetail(PRItemDTO prItemDTO){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			double currUnitPrice = prItemDTO.getRegPrice();
			if(prItemDTO.getRegMPack() > 1 ){
				currUnitPrice = prItemDTO.getRegPrice() / prItemDTO.getRegMPack();
			}
			if(currUnitPrice == 0){
				isItemToBeAdded = true;
			}
		}
		return isItemToBeAdded;
	}
	
	
	
	/**
	 * 
	 * @param prItemDTO
	 * @return checks if items cost is changed gt x pct in last x months
	 */
	public boolean checkCostChangedXPctInLastXmonths(PRItemDTO prItemDTO, Set<Integer> costChangeSet){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			if(costChangeSet.contains(prItemDTO.getItemCode())){
				isItemToBeAdded = true;
			}
		}
		return isItemToBeAdded;
	}
	
	
	/**
	 * 
	 * @param prItemDTO
	 * @return checks if items retail is changed gt x times in last x months
	 */
	public boolean checkRetailChangedXTimesInLastXmonths(PRItemDTO prItemDTO, Set<Integer> priceChangeSet){
		boolean isItemToBeAdded = false;
		if(!prItemDTO.isLir()){
			if(priceChangeSet.contains(prItemDTO.getItemCode())){
				isItemToBeAdded = true;
			}
		}
		return isItemToBeAdded;
	}
	
	
	/**
	 * Groups items into corresponding brands
	 * @param prItemDTO
	 * @param brandMap
	 */
	public void groupBrandItems(PRItemDTO prItemDTO, HashMap<Integer, List<PRItemDTO>> brandMap){
		if(!prItemDTO.isLir()){
			int brandId = prItemDTO.getBrandId();
			if(brandId > 0){
				if(brandMap.get(brandId) == null){
					List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
					itemList.add(prItemDTO);	
					brandMap.put(brandId, itemList);
				}
				else{
					List<PRItemDTO> itemList = brandMap.get(brandId);
					itemList.add(prItemDTO);
					brandMap.put(brandId, itemList);
				}
			}
		}
	}
	
	
	public void findLinePriceViolations(HashMap<Integer, List<PRItemDTO>> ligMap, AuditReportDTO auditReportDTO){
		Set<Integer> violatedMembers = new HashSet<>();
		Set<Integer> violatedLig = new HashSet<>();
		for(Map.Entry<Integer, List<PRItemDTO>> entry: ligMap.entrySet()){
			List<Double> lineItemPriceList = new ArrayList<>();
			boolean isPriceViolated = false;
			for(PRItemDTO itemDTO: entry.getValue()){
				if(isLinePriceViolation(itemDTO, lineItemPriceList)){
					violatedMembers.add(itemDTO.getItemCode());
				}
			}
			if(isPriceViolated){
				violatedLig.add(entry.getKey());
			}
		}
		auditReportDTO.setRetailLirLinePriceViol(violatedMembers.size());
		auditReportDTO.setRetailLirLinePriceViolUnq(violatedLig.size());
	}
	
	
	private boolean isLinePriceViolation(PRItemDTO itemDTO, List<Double> lineItemPriceList){
		boolean isPriceViolated = false;
		double recommendedUnitPrice = checkOverrideAndReturnPrice(itemDTO);
		if(lineItemPriceList.size() > 1 && !lineItemPriceList.contains(recommendedUnitPrice)){
			isPriceViolated = true; 
		}
			lineItemPriceList.add(recommendedUnitPrice);
		return isPriceViolated;
	}
	
	
	public void groupLigItems(PRItemDTO itemDTO, HashMap<Integer, List<PRItemDTO>> ligMap){
		if(!itemDTO.isLir() && itemDTO.getRetLirId() != 0){
			if(ligMap.get(itemDTO.getRetLirId()) == null){
				List<PRItemDTO> memeberList = new ArrayList<>();
				memeberList.add(itemDTO);
				ligMap.put(itemDTO.getRetLirId(), memeberList);
			}
			else{
				List<PRItemDTO> memeberList = ligMap.get(itemDTO.getRetLirId());
				memeberList.add(itemDTO);
				ligMap.put(itemDTO.getRetLirId(), memeberList);
			}
		}
	}
	
	
	private List<String> getLinePriceViolations(List<PRItemDTO> itemList){
		List<String> filteredItems = new ArrayList<>();
		HashMap<Integer, List<PRItemDTO>> ligMap = new HashMap<>();
		for(PRItemDTO itemDTO: itemList){
			groupLigItems(itemDTO, ligMap);
		}
		for(Map.Entry<Integer, List<PRItemDTO>> entry: ligMap.entrySet()){
			List<Double> lineItemPriceList = new ArrayList<>();
			for(PRItemDTO itemDTO: entry.getValue()){
				addItems(itemDTO, filteredItems, isLinePriceViolation(itemDTO, lineItemPriceList));
			}
		}
		return filteredItems;
	}
	
	
	private List<String> getKviItemWithCompPriceXmonthsOld(AuditDashboardDTO auditDashboardDTO, 
			List<PRItemDTO> itemList, String weekStartDateStr, Date weekStartDate) throws GeneralException, ParseException{
		List<String> filteredItems = new ArrayList<>();
		HashMap<Integer, CompetitiveDataDTO> compData = getCompDataHistory(getConnection(), auditDashboardDTO, weekStartDateStr);
		for(PRItemDTO prItemDTO: itemList){
			boolean isKviItemWithCompPriceXmonthsOld = checkKviItemWithCompPriceXmonthsOld(compData, prItemDTO, weekStartDate);
			addItems(prItemDTO, filteredItems, isKviItemWithCompPriceXmonthsOld);
		}
		return filteredItems;
	}
	
	
	private List<String> getKviItemWithCompNoComp(AuditDashboardDTO auditDashboardDTO, 
			List<PRItemDTO> itemList, String weekStartDateStr) throws GeneralException{
		List<String> filteredItems = new ArrayList<>();
		for(PRItemDTO prItemDTO: itemList){
			boolean isKviItemWithCompNoComp = checkKviItemWithNoComp(prItemDTO);
			addItems(prItemDTO, filteredItems, isKviItemWithCompNoComp);
		}
		return filteredItems;
	}
	
	
	public HashMap<Integer, CompetitiveDataDTO> getCompDataHistory(Connection conn, AuditDashboardDTO auditDashboardDTO,
			String weekStartDate) throws GeneralException {
		RetailPriceZoneDAO retailPriceZoneDAO = new RetailPriceZoneDAO();
		int primaryCompId = retailPriceZoneDAO.getPrimaryCompetitor(conn, auditDashboardDTO.getLocationId());
		PRStrategyDTO prStrategyDTO = new PRStrategyDTO();
		prStrategyDTO.setProductId(auditDashboardDTO.getProductId());
		prStrategyDTO.setProductLevelId(auditDashboardDTO.getProductLevelId());
		prStrategyDTO.setLocationId(primaryCompId);
		prStrategyDTO.setLocationLevelId(Constants.STORE_LEVEL_ID);
		int compHistory = Integer.parseInt(PropertyManager.getProperty("AUDIT_KVI_COMP_HISTORY"));
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
//		HashMap<Integer, CompetitiveDataDTO> compHistoryMap = pricingEngineDAO.getCompPriceData(conn,
//				prStrategyDTO, weekStartDate, compHistory  * 7);
		HashMap<Integer, CompetitiveDataDTO> compHistoryMap = pricingEngineDAO.getLatestCompPriceData(conn,
				prStrategyDTO, weekStartDate, compHistory  * 7);
		return compHistoryMap;
	}
	
	
	
	public boolean checkKviItemWithCompPriceXmonthsOld(HashMap<Integer, CompetitiveDataDTO> compData, PRItemDTO prItemDTO, Date weekStartDate) throws ParseException{
		boolean isKviItemWithCompXMonthsOld = false;
		SimpleDateFormat formatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		if(!prItemDTO.isLir()){
			if(prItemDTO.getPriceCheckListId() > 0 && prItemDTO.getPriceCheckListTypeId() > 0){
				CompetitiveDataDTO competitiveDataDTO = compData.get(prItemDTO.getItemCode());
				if(competitiveDataDTO != null){
					String checkDateStr = competitiveDataDTO.checkDate;
					Date checkDate = formatter.parse(checkDateStr);
					int compHistory = Integer.parseInt(PropertyManager.getProperty("AUDIT_KVI_WITH_X_MONTHS_OLD_COMP"));
					Date dateLimit = DateUtil.incrementDate(weekStartDate, -(compHistory * 30));
					if(checkDate.before(dateLimit)){
						 isKviItemWithCompXMonthsOld = true;
					}
				}
			}
		}
		return isKviItemWithCompXMonthsOld;
	}
	
	public boolean checkKviItemWithNoComp(PRItemDTO prItemDTO) {
		boolean isKviItemWithCompXMonthsOld = false;
		if(!prItemDTO.isLir()){
			if(prItemDTO.getPriceCheckListId() > 0 && prItemDTO.getPriceCheckListTypeId() > 0){
				MultiplePrice compPrice = prItemDTO.getCompPrice();
				if(compPrice == null){
					isKviItemWithCompXMonthsOld = true;
				} else {
					double compUnitPrice = 0;
					if (compPrice.multiple > 1) {
						compUnitPrice = compPrice.price / compPrice.multiple;
					} else {
						compUnitPrice = compPrice.price;
					}
					
					if(compUnitPrice == 0){
						isKviItemWithCompXMonthsOld = true;
					}
				}
				
				
			}
		}
		return isKviItemWithCompXMonthsOld;
	}
	
	public boolean checkEndDigitViol(PRItemDTO prItemDTO, int endingDigit) {
		boolean isEndDigitViol = false;
		if(!prItemDTO.isLir()){
			double recommendedPrice = 0;
			if(prItemDTO.getOverrideRegPrice() > 0){
				recommendedPrice = prItemDTO.getOverrideRegPrice();
			}
			else{
//				recommendedPrice = prItemDTO.getRecommendedRegPrice();
				recommendedPrice = prItemDTO.getRecommendedRegPrice().price;
			}
			
			if(recommendedPrice > 0){
				String recommendedPriceStr = String.valueOf(recommendedPrice);
				char recEndingDigitChar = recommendedPriceStr.charAt(recommendedPriceStr.length() - 1);
				int recEndingDigit = Integer.parseInt(String.valueOf(recEndingDigitChar));
				
				if(recEndingDigit != endingDigit){
					isEndDigitViol = true;
				}
			}
		}
		return isEndDigitViol;
	}
	
	public boolean checkBrandViol(PRItemDTO prItemDTO) {
		boolean isBrandViol = false;
		
		//Check log object is null or not
		if (prItemDTO.getExplainLog() != null){
			PRExplainLog  explainLog = prItemDTO.getExplainLog();
			List<PRGuidelineAndConstraintLog> guidelineListObj =  explainLog.getGuidelineAndConstraintLogs();

			//Check is there any guideline or constraints
			if (guidelineListObj != null && guidelineListObj.size() > 0){

				//Iterate for each guideline/constraints				
				for (int i=0; i < guidelineListObj.size(); i++){
					PRGuidelineAndConstraintLog guidelineObj = guidelineListObj.get(i);  
					
					//If the guideline is brand and it has conflict
					if (guidelineObj.getGuidelineTypeId() == 4 && guidelineObj.getIsConflict())
						isBrandViol = true;
				}
			}
		}		

		return isBrandViol;
	}	

	//Method to check is any size conflict in the recommendation
	public boolean checkSizeViol(PRItemDTO prItemDTO) {
		boolean isSizeViol = false;

		//Check log object is null or not
		if (prItemDTO.getExplainLog() != null){
			PRExplainLog  explainLog = prItemDTO.getExplainLog();
			List<PRGuidelineAndConstraintLog> guidelineListObj =  explainLog.getGuidelineAndConstraintLogs();

			//Check is there any guideline or constraints
			if (guidelineListObj != null && guidelineListObj.size() > 0){

				//Iterate for each guideline/constraints				
				for (int i=0; i < guidelineListObj.size(); i++){
					PRGuidelineAndConstraintLog guidelineObj = guidelineListObj.get(i);  
					
					//If the guideline is size and it has conflict
					if (guidelineObj.getGuidelineTypeId() == 5 && guidelineObj.getIsConflict())
						isSizeViol = true;
				}
			}
		}

		return isSizeViol;
	}		
	
	
	private List<String> getEndingDigitViolations(List<PRItemDTO> itemList,List<AuditParameterDTO> auditParameters, AuditFilter auditFilter ){
		List<String> filteredItems = new ArrayList<String>();
		int endingDigit = 0;
		for(AuditParameterDTO auditParameterDTO: auditParameters){
			 if(auditParameterDTO.getParamsType().trim().equals(String.valueOf(PRConstants.END_DIGIT_VIOL))){
				 endingDigit = (int) auditParameterDTO.getParameterValue();
				 auditFilter.auditSetting = endingDigit;
			}
		}
		for(PRItemDTO prItemDTO: itemList){
			boolean isEndingDigitViol = checkEndDigitViol(prItemDTO, endingDigit);
			addItems(prItemDTO, filteredItems, isEndingDigitViol);
		}
		return filteredItems;
	}
	
	/**
	 * 
	 * @param prItemDTO
	 * @return returns unit retail of recommended or overridden.
	 */
	private double checkOverrideAndReturnPrice(PRItemDTO prItemDTO){
//		double recommendedUnitPrice = prItemDTO.getRecommendedRegPrice() 
//				/ prItemDTO.getRecommendedRegMultiple();
		double recommendedUnitPrice = PRCommonUtil.getUnitPrice(prItemDTO.getRecommendedRegPrice(), true);
		if(prItemDTO.getOverrideRegPrice() > 0){
			double overridePrice = 0;
			if(prItemDTO.getOverrideRegMultiple() > 0){
				overridePrice = prItemDTO.getOverrideRegPrice() / prItemDTO.getOverrideRegMultiple();	
			}
			else{
				overridePrice = prItemDTO.getOverrideRegPrice();
			}
			recommendedUnitPrice = overridePrice;
		}
		
		return recommendedUnitPrice;
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
	
}
