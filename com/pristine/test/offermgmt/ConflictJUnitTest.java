package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

//import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dto.offermgmt.MultiplePrice;
//import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.service.offermgmt.PricingEngineService;
//import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class ConflictJUnitTest {
	ObjectMapper mapper = new ObjectMapper();
	
	@Before
    public void init() {
//		PropertyConfigurator.configure("log4j-pricing-engine.properties");		 
    }
	
	@Test
	public void testGenericConflict() {
		PRItemDTO itemInfo = new PRItemDTO();
		PRExplainLog expExplainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;
		PricingEngineService pricingEngineService = new PricingEngineService();
//		itemInfo.setRecommendedRegPrice(5.69);
		
		itemInfo.setRecommendedRegPrice(new MultiplePrice(1, 5.69));
		
		//No Margin (No both ends)
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.MARGIN.getGuidelineTypeId());
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		//Price Index(only End Range) Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.PRICE_INDEX.getGuidelineTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(4.69);
		guidelineAndConstraintLog.setIsConflict(true);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		//Multi Comp (Only Start Range) Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.COMPETITION.getGuidelineTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(6.29);
		guidelineAndConstraintLog.setIsConflict(true);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		// Brand (Both Range) Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.BRAND.getGuidelineTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(5.70);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(7.29);
		guidelineAndConstraintLog.setOperatorText(PRConstants.PRICE_GROUP_EXPR_ABOVE);
		guidelineAndConstraintLog.setIsConflict(true);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		// Brand (Both Range) Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.BRAND.getGuidelineTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(5.55);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(5.55);
		guidelineAndConstraintLog.setOperatorText(PRConstants.PRICE_GROUP_EXPR_ABOVE);
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		// Brand (Both Range) Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.BRAND.getGuidelineTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(5.69);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(5.69);
		guidelineAndConstraintLog.setOperatorText(PRConstants.PRICE_GROUP_EXPR_ABOVE);
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		// Brand (Both Range) Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.BRAND.getGuidelineTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(4.69);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(4.69);
		guidelineAndConstraintLog.setOperatorText(PRConstants.PRICE_GROUP_EXPR_BELOW);
		guidelineAndConstraintLog.setIsConflict(true);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		// Brand (Both Range) Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.BRAND.getGuidelineTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(7.69);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(7.69);
		guidelineAndConstraintLog.setOperatorText(PRConstants.PRICE_GROUP_EXPR_BELOW);
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		// Threshold (both range) Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.THRESHOLD.getConstraintTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(5.70);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(6.29);
		guidelineAndConstraintLog.setIsConflict(true);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		PRExplainLog actExplainLog = new PRExplainLog();
		
		copyExplainLog(expExplainLog, actExplainLog);
		itemInfo.setExplainLog(actExplainLog);
		
		pricingEngineService.updateConflicts(itemInfo);
		
		try {			 
			assertEquals("JSON Not Matching", mapper.writeValueAsString(expExplainLog), 
					mapper.writeValueAsString(itemInfo.getExplainLog()));
		} catch (JsonProcessingException e) {			 
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testGenericNoConflict() {
		PRItemDTO itemInfo = new PRItemDTO();
		PRExplainLog expExplainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;
//		itemInfo.setRecommendedRegPrice(5.70);
		itemInfo.setRecommendedRegPrice(new MultiplePrice(1, 5.70));
		//No Margin (No both ends)
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.MARGIN.getGuidelineTypeId());
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		//Price Index(only End Range) No Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.PRICE_INDEX.getGuidelineTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(8.69);
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		//Multi Comp (Only Start Range) No Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.COMPETITION.getGuidelineTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(3.29);
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		// Brand (Both Range) No Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.BRAND.getGuidelineTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(5.70);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(7.29);
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		// Threshold (both range) No Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.THRESHOLD.getConstraintTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(5.70);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(6.29);
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		PRExplainLog actExplainLog = new PRExplainLog();
		
		copyExplainLog(expExplainLog, actExplainLog);
		itemInfo.setExplainLog(actExplainLog);
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.updateConflicts(itemInfo);
		
		try {			 
			assertEquals("JSON Not Matching", mapper.writeValueAsString(expExplainLog), 
					mapper.writeValueAsString(itemInfo.getExplainLog()));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testGenericNoConflict1() {
		PRItemDTO itemInfo = new PRItemDTO();
		PRExplainLog expExplainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;
//		itemInfo.setRecommendedRegPrice(3.59);
		itemInfo.setRecommendedRegPrice(new MultiplePrice(1, 3.59));
		
		//No Margin (No both ends)
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.MARGIN.getGuidelineTypeId());
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		// Brand (Both Range) No Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.BRAND.getGuidelineTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(3.59);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(3.59);
		guidelineAndConstraintLog.setOperatorText(PRConstants.PRICE_GROUP_EXPR_ABOVE);
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		// Multi Comp (Only Start Range) No Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.COMPETITION.getGuidelineTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(3.67);
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	 
		
		// Threshold (both range) No Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.THRESHOLD.getConstraintTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(3.23);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(3.95);
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		PRExplainLog actExplainLog = new PRExplainLog();
		
		copyExplainLog(expExplainLog, actExplainLog);
		itemInfo.setExplainLog(actExplainLog);
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.updateConflicts(itemInfo);
		
		try {			 
			assertEquals("JSON Not Matching", mapper.writeValueAsString(expExplainLog), 
					mapper.writeValueAsString(itemInfo.getExplainLog()));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testConflictThreshTwoRange1() {
		PRItemDTO itemInfo = new PRItemDTO();
		PRExplainLog expExplainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;
//		itemInfo.setRecommendedRegPrice(5.70);			 
		itemInfo.setRecommendedRegPrice(new MultiplePrice(1, 5.70));
		
		// Threshold (both range) No Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.THRESHOLD.getConstraintTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(5.80);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(6.29);
		
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange2().setStartVal(3.60);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange2().setEndVal(5.60);
		
		guidelineAndConstraintLog.setIsConflict(true);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		PRExplainLog actExplainLog = new PRExplainLog();
		
		copyExplainLog(expExplainLog, actExplainLog);
		itemInfo.setExplainLog(actExplainLog);
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.updateConflicts(itemInfo);
		
		try {			 
			assertEquals("JSON Not Matching", mapper.writeValueAsString(expExplainLog), 
					mapper.writeValueAsString(itemInfo.getExplainLog()));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	@Test
	public void testNoConflictThreshTwoRange1() {
		PRItemDTO itemInfo = new PRItemDTO();
		PRExplainLog expExplainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;
//		itemInfo.setRecommendedRegPrice(5.89);			 
		itemInfo.setRecommendedRegPrice(new MultiplePrice(1, 5.89));
		
		// Threshold (both range) No Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.THRESHOLD.getConstraintTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(5.80);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(6.29);
		
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange2().setStartVal(3.60);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange2().setEndVal(5.60);
		
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		PRExplainLog actExplainLog = new PRExplainLog();
		
		copyExplainLog(expExplainLog, actExplainLog);
		itemInfo.setExplainLog(actExplainLog);
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.updateConflicts(itemInfo);
		
		try {			 
			assertEquals("JSON Not Matching", mapper.writeValueAsString(expExplainLog), 
					mapper.writeValueAsString(itemInfo.getExplainLog()));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testNoConflictThreshTwoRange2() {
		PRItemDTO itemInfo = new PRItemDTO();
		PRExplainLog expExplainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;
//		itemInfo.setRecommendedRegPrice(5.60);		
		itemInfo.setRecommendedRegPrice(new MultiplePrice(1, 5.60));
		
		// Threshold (both range) No Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.THRESHOLD.getConstraintTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(5.80);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(6.29);
		
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange2().setStartVal(3.60);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange2().setEndVal(5.60);
		
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		PRExplainLog actExplainLog = new PRExplainLog();
		
		copyExplainLog(expExplainLog, actExplainLog);
		itemInfo.setExplainLog(actExplainLog);
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.updateConflicts(itemInfo);
		
		try {			 
			assertEquals("JSON Not Matching", mapper.writeValueAsString(expExplainLog), 
					mapper.writeValueAsString(itemInfo.getExplainLog()));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void copyExplainLog(PRExplainLog orgExplainLog, PRExplainLog cloneExplainLog) {
		for (PRGuidelineAndConstraintLog orgLog : orgExplainLog.getGuidelineAndConstraintLogs()) {
			PRGuidelineAndConstraintLog cloneLog = new PRGuidelineAndConstraintLog();
			cloneLog.setConstraintTypeId(orgLog.getConstraintTypeId());
			cloneLog.setGuidelineTypeId(orgLog.getGuidelineTypeId());
			cloneLog.setIsConflict(false);
			cloneLog.setGuidelineOrConstraintPriceRange1(orgLog.getGuidelineOrConstraintPriceRange1());
			cloneLog.setGuidelineOrConstraintPriceRange2(orgLog.getGuidelineOrConstraintPriceRange2());
			cloneLog.setOperatorText(orgLog.getOperatorText());
			cloneExplainLog.getGuidelineAndConstraintLogs().add(cloneLog);
		}
	}
	
	
	@Test
	public void testIndexAndRounding() {
		PRItemDTO itemInfo = new PRItemDTO();
		PRExplainLog expExplainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;
//		itemInfo.setRecommendedRegPrice(5.89);			 
		itemInfo.setRecommendedRegPrice(new MultiplePrice(1, 5.89));
		
		// Threshold (both range) No Conflict
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.ROUNDING.getConstraintTypeId());
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setStartVal(1.1);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().setEndVal(6.29);
		
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange2().setStartVal(3.60);
		guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange2().setEndVal(5.60);
		
		guidelineAndConstraintLog.setIsConflict(false);
		expExplainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		PRExplainLog actExplainLog = new PRExplainLog();
		
		copyExplainLog(expExplainLog, actExplainLog);
		itemInfo.setExplainLog(actExplainLog);
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.updateConflicts(itemInfo);
		
		try {			 
			assertEquals("JSON Not Matching", mapper.writeValueAsString(expExplainLog), 
					mapper.writeValueAsString(itemInfo.getExplainLog()));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
