package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRGuidelineComp;
import com.pristine.dto.offermgmt.PRGuidelineCompDetail;
import com.pristine.dto.offermgmt.PRGuidelinesDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.guideline.MultiCompGuideline;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRConstants;

public class MultiCompGuidelineJUnitTestCases {
	
	@Test
	public void test1EqualToMinPriceOfTheGroup() throws Exception{
		PRRange priceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
		PRExplainLog explainLog = new PRExplainLog();
		PRItemDTO itemInfo = new PRItemDTO();
		PRStrategyDTO strategyDTO = new PRStrategyDTO();
		PRGuidelinesDTO guidelines = new PRGuidelinesDTO();
		PRGuidelineComp guidelineComp = new PRGuidelineComp();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		List<PRGuidelineCompDetail> compDetails = new ArrayList<PRGuidelineCompDetail>();
		//PRGuidelineComp guidelineComp =  null;
		
		//guidelineComp.setGroupPriceType(PRConstants.GROUP_PRICE_TYPE_MIN);
		guidelineComp.setGroupPriceType(PRConstants.GROUP_PRICE_TYPE_MIN);
		guidelineComp.setLatestPriceObservationDays(90);
		guidelineComp.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(978);		
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(952);		
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(968);		
		compDetails.add(guidelineCompDetail);
		
		guidelineComp.setCompetitorDetails(compDetails);
		guidelines.setCompGuideline(guidelineComp);
		
		strategyDTO.setGuidelines(guidelines);
		strategyDTO.setStartDate("01/20/2020");
		
		itemInfo.setItemCode(1234);
		itemInfo.setStrategyDTO(strategyDTO);
		
		LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 978);
		MultiplePrice compPrice = new MultiplePrice(1, 2.59);
		String checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 952);
		compPrice = new MultiplePrice(1, 2.69);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 968);
		compPrice = new MultiplePrice(1, 2.49);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		MultiCompGuideline multiCompGuideline = new MultiCompGuideline(itemInfo, priceRange, explainLog, "03/01/2020");
		guidelineAndConstraintOutputLocal = multiCompGuideline.applyMultipleCompGuideline();
		
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getStartVal() - 2.49), 0);
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getEndVal() - 2.49), 0);
	}
	
	
	@Test
	public void test2EqualToAvgPriceOfTheGroup() throws Exception{
		PRRange priceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
		PRExplainLog explainLog = new PRExplainLog();
		PRItemDTO itemInfo = new PRItemDTO();
		PRStrategyDTO strategyDTO = new PRStrategyDTO();
		PRGuidelinesDTO guidelines = new PRGuidelinesDTO();
		PRGuidelineComp guidelineComp = new PRGuidelineComp();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		List<PRGuidelineCompDetail> compDetails = new ArrayList<PRGuidelineCompDetail>();
		//PRGuidelineComp guidelineComp =  null;
		
		guidelineComp.setGroupPriceType(PRConstants.GROUP_PRICE_TYPE_AVG);
		guidelineComp.setLatestPriceObservationDays(90);
		guidelineComp.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(978);		
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(952);		
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(968);		
		compDetails.add(guidelineCompDetail);
		
		guidelineComp.setCompetitorDetails(compDetails);
		guidelines.setCompGuideline(guidelineComp);
		
		strategyDTO.setGuidelines(guidelines);
		strategyDTO.setStartDate("01/20/2020");
		
		itemInfo.setItemCode(1234);
		itemInfo.setStrategyDTO(strategyDTO);
		
		LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 978);
		MultiplePrice compPrice = new MultiplePrice(1, 2.59);
		String checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 952);
		compPrice = new MultiplePrice(1, 2.69);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 968);
		compPrice = new MultiplePrice(1, 2.49);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		MultiCompGuideline multiCompGuideline = new MultiCompGuideline(itemInfo, priceRange, explainLog, "03/01/2020");
		guidelineAndConstraintOutputLocal = multiCompGuideline.applyMultipleCompGuideline();
		
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getStartVal() - 2.59), 0);
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getEndVal() - 2.59), 0);
		
	}
	
	
	@Test
	public void test3LessThanEqualToAvgPriceOfTheGroup() throws Exception{
		PRRange priceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
		PRExplainLog explainLog = new PRExplainLog();
		PRItemDTO itemInfo = new PRItemDTO();
		PRStrategyDTO strategyDTO = new PRStrategyDTO();
		PRGuidelinesDTO guidelines = new PRGuidelinesDTO();
		PRGuidelineComp guidelineComp = new PRGuidelineComp();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		List<PRGuidelineCompDetail> compDetails = new ArrayList<PRGuidelineCompDetail>();
		//PRGuidelineComp guidelineComp =  null;
		
		guidelineComp.setGroupPriceType(PRConstants.GROUP_PRICE_TYPE_AVG);
		guidelineComp.setLatestPriceObservationDays(90);
		guidelineComp.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(978);		
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(952);		
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(968);		
		compDetails.add(guidelineCompDetail);
		
		guidelineComp.setCompetitorDetails(compDetails);
		guidelines.setCompGuideline(guidelineComp);
		
		strategyDTO.setGuidelines(guidelines);
		strategyDTO.setStartDate("01/20/2020");
		
		itemInfo.setItemCode(1234);
		itemInfo.setStrategyDTO(strategyDTO);
		
		LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 978);
		MultiplePrice compPrice = new MultiplePrice(1, 2.59);
		String checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 952);
		compPrice = new MultiplePrice(1, 2.69);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 968);
		compPrice = new MultiplePrice(1, 2.49);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		MultiCompGuideline multiCompGuideline = new MultiCompGuideline(itemInfo, priceRange, explainLog, "03/01/2020");
		guidelineAndConstraintOutputLocal = multiCompGuideline.applyMultipleCompGuideline();
		
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getEndVal() - 2.59), 0);
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getStartVal()), Constants.DEFAULT_NA);
		
	}
	
	@Test
	public void test4LessThanAvgPriceOfTheGroup() throws Exception{
		PRRange priceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
		PRExplainLog explainLog = new PRExplainLog();
		PRItemDTO itemInfo = new PRItemDTO();
		PRStrategyDTO strategyDTO = new PRStrategyDTO();
		PRGuidelinesDTO guidelines = new PRGuidelinesDTO();
		PRGuidelineComp guidelineComp = new PRGuidelineComp();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		List<PRGuidelineCompDetail> compDetails = new ArrayList<PRGuidelineCompDetail>();
		//PRGuidelineComp guidelineComp =  null;
		
		guidelineComp.setGroupPriceType(PRConstants.GROUP_PRICE_TYPE_AVG);
		guidelineComp.setLatestPriceObservationDays(90);
		guidelineComp.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_LESSER_SYM);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(978);		
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(952);		
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(968);		
		compDetails.add(guidelineCompDetail);
		
		guidelineComp.setCompetitorDetails(compDetails);
		guidelines.setCompGuideline(guidelineComp);
		
		strategyDTO.setGuidelines(guidelines);
		strategyDTO.setStartDate("01/20/2020");
		
		itemInfo.setItemCode(1234);
		itemInfo.setStrategyDTO(strategyDTO);
		
		LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 978);
		MultiplePrice compPrice = new MultiplePrice(1, 2.59);
		String checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 952);
		compPrice = new MultiplePrice(1, 2.69);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 968);
		compPrice = new MultiplePrice(1, 2.49);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		MultiCompGuideline multiCompGuideline = new MultiCompGuideline(itemInfo, priceRange, explainLog, "03/01/2020");
		guidelineAndConstraintOutputLocal = multiCompGuideline.applyMultipleCompGuideline();
		
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getEndVal() - 2.58), 0);
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getStartVal()), Constants.DEFAULT_NA);
		
	}
	
	
	@Test
	public void test5LessThanMinPriceOfTheGroup() throws Exception{
		PRRange priceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
		PRExplainLog explainLog = new PRExplainLog();
		PRItemDTO itemInfo = new PRItemDTO();
		PRStrategyDTO strategyDTO = new PRStrategyDTO();
		PRGuidelinesDTO guidelines = new PRGuidelinesDTO();
		PRGuidelineComp guidelineComp = new PRGuidelineComp();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		List<PRGuidelineCompDetail> compDetails = new ArrayList<PRGuidelineCompDetail>();
		//PRGuidelineComp guidelineComp =  null;
		
		guidelineComp.setGroupPriceType(PRConstants.GROUP_PRICE_TYPE_MIN);
		guidelineComp.setLatestPriceObservationDays(90);
		guidelineComp.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_LESSER_SYM);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(978);		
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(952);		
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(968);		
		compDetails.add(guidelineCompDetail);
		
		guidelineComp.setCompetitorDetails(compDetails);
		guidelines.setCompGuideline(guidelineComp);
		
		strategyDTO.setGuidelines(guidelines);
		strategyDTO.setStartDate("01/20/2020");
		
		itemInfo.setItemCode(1234);
		itemInfo.setStrategyDTO(strategyDTO);
		
		LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 978);
		MultiplePrice compPrice = new MultiplePrice(1, 2.59);
		String checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 952);
		compPrice = new MultiplePrice(1, 2.69);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 968);
		compPrice = new MultiplePrice(1, 2.49);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		MultiCompGuideline multiCompGuideline = new MultiCompGuideline(itemInfo, priceRange, explainLog, "03/01/2020");
		guidelineAndConstraintOutputLocal = multiCompGuideline.applyMultipleCompGuideline();
		
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getEndVal() - 2.48), 0);
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getStartVal()), Constants.DEFAULT_NA);
		
	}
	
	@Test
	public void test6LessThanOrEqualToMinPriceOfTheGroup() throws Exception{
		PRRange priceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
		PRExplainLog explainLog = new PRExplainLog();
		PRItemDTO itemInfo = new PRItemDTO();
		PRStrategyDTO strategyDTO = new PRStrategyDTO();
		PRGuidelinesDTO guidelines = new PRGuidelinesDTO();
		PRGuidelineComp guidelineComp = new PRGuidelineComp();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		List<PRGuidelineCompDetail> compDetails = new ArrayList<PRGuidelineCompDetail>();
		//PRGuidelineComp guidelineComp =  null;
		
		guidelineComp.setGroupPriceType(PRConstants.GROUP_PRICE_TYPE_MIN);
		guidelineComp.setLatestPriceObservationDays(90);
		guidelineComp.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(978);		
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(952);		
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(968);		
		compDetails.add(guidelineCompDetail);
		
		guidelineComp.setCompetitorDetails(compDetails);
		guidelines.setCompGuideline(guidelineComp);
		
		strategyDTO.setGuidelines(guidelines);
		strategyDTO.setStartDate("01/20/2020");
		
		itemInfo.setItemCode(1234);
		itemInfo.setStrategyDTO(strategyDTO);
		
		LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 978);
		MultiplePrice compPrice = new MultiplePrice(1, 2.59);
		String checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 952);
		compPrice = new MultiplePrice(1, 2.69);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 968);
		compPrice = new MultiplePrice(1, 2.49);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		MultiCompGuideline multiCompGuideline = new MultiCompGuideline(itemInfo, priceRange, explainLog, "03/01/2020");
		guidelineAndConstraintOutputLocal = multiCompGuideline.applyMultipleCompGuideline();
		
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getEndVal() - 2.49), 0);
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getStartVal()), Constants.DEFAULT_NA);
		
	}
	
	@Test
	public void test7LessThanOrEqualToMinPriceOfTheGroupWithMaxRetail() throws Exception{
		PRRange priceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
		PRExplainLog explainLog = new PRExplainLog();
		PRItemDTO itemInfo = new PRItemDTO();
		PRStrategyDTO strategyDTO = new PRStrategyDTO();
		PRGuidelinesDTO guidelines = new PRGuidelinesDTO();
		PRGuidelineComp guidelineComp = new PRGuidelineComp();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		List<PRGuidelineCompDetail> compDetails = new ArrayList<PRGuidelineCompDetail>();
		//PRGuidelineComp guidelineComp =  null;
		
		guidelineComp.setGroupPriceType(PRConstants.GROUP_PRICE_TYPE_MIN);
		guidelineComp.setLatestPriceObservationDays(90);
		guidelineComp.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(978);		
		guidelineCompDetail.setMinValue(1);
		guidelineCompDetail.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_GREATER_SYM);
		guidelineCompDetail.setValueType(PRConstants.VALUE_TYPE_$);
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(952);
		guidelineCompDetail.setMinValue(1);
		guidelineCompDetail.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_GREATER_SYM);
		guidelineCompDetail.setValueType(PRConstants.VALUE_TYPE_$);
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(968);	
		guidelineCompDetail.setMinValue(1);
		guidelineCompDetail.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_GREATER_SYM);
		guidelineCompDetail.setValueType(PRConstants.VALUE_TYPE_$);
		compDetails.add(guidelineCompDetail);
		
		guidelineComp.setCompetitorDetails(compDetails);
		guidelines.setCompGuideline(guidelineComp);
		
		strategyDTO.setGuidelines(guidelines);
		strategyDTO.setStartDate("01/20/2020");
		
		itemInfo.setItemCode(1234);
		itemInfo.setStrategyDTO(strategyDTO);
		
		LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 978);
		MultiplePrice compPrice = new MultiplePrice(1, 2.59);
		String checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 952);
		compPrice = new MultiplePrice(1, 2.69);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 968);
		compPrice = new MultiplePrice(1, 2.49);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		MultiCompGuideline multiCompGuideline = new MultiCompGuideline(itemInfo, priceRange, explainLog, "03/01/2020");
		guidelineAndConstraintOutputLocal = multiCompGuideline.applyMultipleCompGuideline();
		
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getEndVal() - 3.49), 0);
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getStartVal()), Constants.DEFAULT_NA);
		
	}
	
	
	@Test
	public void test8LessThanMinPriceOfTheGroupWithMaxRetail() throws Exception{
		PRRange priceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
		PRExplainLog explainLog = new PRExplainLog();
		PRItemDTO itemInfo = new PRItemDTO();
		PRStrategyDTO strategyDTO = new PRStrategyDTO();
		PRGuidelinesDTO guidelines = new PRGuidelinesDTO();
		PRGuidelineComp guidelineComp = new PRGuidelineComp();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		List<PRGuidelineCompDetail> compDetails = new ArrayList<PRGuidelineCompDetail>();
		//PRGuidelineComp guidelineComp =  null;
		
		guidelineComp.setGroupPriceType(PRConstants.GROUP_PRICE_TYPE_MIN);
		guidelineComp.setLatestPriceObservationDays(90);
		guidelineComp.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_LESSER_SYM);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(978);		
		guidelineCompDetail.setMinValue(1);
		guidelineCompDetail.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_GREATER_SYM);
		guidelineCompDetail.setValueType(PRConstants.VALUE_TYPE_$);
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(952);
		guidelineCompDetail.setMinValue(1);
		guidelineCompDetail.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_GREATER_SYM);
		guidelineCompDetail.setValueType(PRConstants.VALUE_TYPE_$);
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(968);	
		guidelineCompDetail.setMinValue(1);
		guidelineCompDetail.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_GREATER_SYM);
		guidelineCompDetail.setValueType(PRConstants.VALUE_TYPE_$);
		compDetails.add(guidelineCompDetail);
		
		guidelineComp.setCompetitorDetails(compDetails);
		guidelines.setCompGuideline(guidelineComp);
		
		strategyDTO.setGuidelines(guidelines);
		strategyDTO.setStartDate("01/20/2020");
		
		itemInfo.setItemCode(1234);
		itemInfo.setStrategyDTO(strategyDTO);
		
		LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 978);
		MultiplePrice compPrice = new MultiplePrice(1, 2.59);
		String checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 952);
		compPrice = new MultiplePrice(1, 2.69);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 968);
		compPrice = new MultiplePrice(1, 2.49);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		MultiCompGuideline multiCompGuideline = new MultiCompGuideline(itemInfo, priceRange, explainLog, "03/01/2020");
		guidelineAndConstraintOutputLocal = multiCompGuideline.applyMultipleCompGuideline();
		
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getEndVal() - 3.48), 0);
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getStartVal()), Constants.DEFAULT_NA);
		
	}
	
	@Test
	public void test9LessThanAvgPriceOfTheGroupWithMaxRetail() throws Exception{
		PRRange priceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
		PRExplainLog explainLog = new PRExplainLog();
		PRItemDTO itemInfo = new PRItemDTO();
		PRStrategyDTO strategyDTO = new PRStrategyDTO();
		PRGuidelinesDTO guidelines = new PRGuidelinesDTO();
		PRGuidelineComp guidelineComp = new PRGuidelineComp();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		List<PRGuidelineCompDetail> compDetails = new ArrayList<PRGuidelineCompDetail>();
		//PRGuidelineComp guidelineComp =  null;
		
		guidelineComp.setGroupPriceType(PRConstants.GROUP_PRICE_TYPE_AVG);
		guidelineComp.setLatestPriceObservationDays(90);
		guidelineComp.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_LESSER_SYM);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(978);		
		guidelineCompDetail.setMinValue(1);
		guidelineCompDetail.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_GREATER_SYM);
		guidelineCompDetail.setValueType(PRConstants.VALUE_TYPE_$);
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(952);
		guidelineCompDetail.setMinValue(1);
		guidelineCompDetail.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_GREATER_SYM);
		guidelineCompDetail.setValueType(PRConstants.VALUE_TYPE_$);
		compDetails.add(guidelineCompDetail);
		
		guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(968);	
		guidelineCompDetail.setMinValue(1);
		guidelineCompDetail.setRelationalOperatorText(PRConstants.PRICE_GROUP_EXPR_GREATER_SYM);
		guidelineCompDetail.setValueType(PRConstants.VALUE_TYPE_$);
		compDetails.add(guidelineCompDetail);
		
		guidelineComp.setCompetitorDetails(compDetails);
		guidelines.setCompGuideline(guidelineComp);
		
		strategyDTO.setGuidelines(guidelines);
		strategyDTO.setStartDate("01/20/2020");
		
		itemInfo.setItemCode(1234);
		itemInfo.setStrategyDTO(strategyDTO);
		
		LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 978);
		MultiplePrice compPrice = new MultiplePrice(1, 2.59);
		String checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 952);
		compPrice = new MultiplePrice(1, 2.69);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		locationKey = new LocationKey(Constants.STORE_LEVEL_ID, 968);
		compPrice = new MultiplePrice(1, 2.49);
		checkDate = "02/01/2020";
		itemInfo.addAllCompPrice(locationKey, compPrice);
		itemInfo.addAllCompPriceCheckDate(locationKey, checkDate);
		
		MultiCompGuideline multiCompGuideline = new MultiCompGuideline(itemInfo, priceRange, explainLog, "03/01/2020");
		guidelineAndConstraintOutputLocal = multiCompGuideline.applyMultipleCompGuideline();
		
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getEndVal() - 3.58), 0);
		assertEquals("Mismatch", (int) (guidelineAndConstraintOutputLocal.outputPriceRange.getStartVal()), Constants.DEFAULT_NA);
		
	}
}
