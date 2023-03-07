package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//import org.apache.log4j.PropertyConfigurator;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.pristine.dao.offermgmt.prediction.PredictionDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRSubstituteItem;
import com.pristine.dto.offermgmt.prediction.PredictionDetailDTO;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.substitute.SubjstituteAdjRMainItemDTO;
import com.pristine.dto.offermgmt.substitute.SubstituteAdjRInputDTO;
import com.pristine.dto.offermgmt.substitute.SubstituteAdjROutputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.prediction.PredictionDetailKey;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.service.offermgmt.substitute.SubstituteAdjustmentRService;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class SubstituteJUnitTest {
	private int locationLevelId = Constants.ZONE_LEVEL_ID;
	private int locationId = 15;
	private int productLevelId = Constants.CATEGORYLEVELID;
	private int productId = 1203;
	private int recWeekCalendarId = 0;

	com.pristine.service.offermgmt.substitute.SubstituteService susbstituteService;
	SubstituteAdjustmentRService substituteAdjustmentRServiceMock;
	PredictionDAO predictionDAOMock;
	PredictionInputDTO predictionInputDTOMock;

	@Before
	public void init() {
//		PropertyConfigurator.configure("log4j-pricing-engine.properties");
		PropertyManager.initialize("recommendation.properties");

		susbstituteService = new com.pristine.service.offermgmt.substitute.SubstituteService(false);
		substituteAdjustmentRServiceMock = EasyMock.createMock(SubstituteAdjustmentRService.class);
		susbstituteService.setSubstituteAdjustmentRService(substituteAdjustmentRServiceMock);
		predictionDAOMock = EasyMock.createMock(PredictionDAO.class);
		susbstituteService.setPredictionDAO(predictionDAOMock);
		predictionInputDTOMock = EasyMock.createMock(PredictionInputDTO.class);
	}

	/**
	 * Check if main item cur and rec pred is adjusted
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
//	@SuppressWarnings("unchecked")
//	@Test
//	public void testRecPredAdjustment() throws Exception, GeneralException {
//		List<PRItemDTO> recommendedItems = new ArrayList<PRItemDTO>();
//		PredictionDetailKey pdk = null;
//		PredictionDetailDTO pd = null;
//		
//		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails("10/02/2016", "");
//		PRRecommendationRunHeader recommendationRunHeader = TestHelper.getRecommendationRunHeader("10/09/2016");
//
//		HashMap<ItemKey, List<PRSubstituteItem>> substituteItemMap = new HashMap<ItemKey, List<PRSubstituteItem>>();
//		List<PRSubstituteItem> substituteItems = new ArrayList<PRSubstituteItem>();
//
//		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetailMap = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
//		List<PRItemSaleInfoDTO> saleItems = new ArrayList<PRItemSaleInfoDTO>();
//
//		int MAIN_ITEM_RET_LIR_ID = 14388;
//		int SUBS_ITEM_RET_LIR_ID = 14389;
//
//		substituteItems.add(TestHelper.getSubstituteItem(Constants.PRODUCT_LEVEL_ID_LIG, MAIN_ITEM_RET_LIR_ID, Constants.PRODUCT_LEVEL_ID_LIG,
//				SUBS_ITEM_RET_LIR_ID, 0.6368d));
//		substituteItemMap.put(new ItemKey(MAIN_ITEM_RET_LIR_ID, PRConstants.LIG_ITEM_INDICATOR), substituteItems);
//
//		/* Sale info of subs items */
//		PRItemSaleInfoDTO saleInfoDTO = TestHelper.getItemSaleInfoDTO(1, 2.79, PromoTypeLookup.STANDARD.getPromoTypeId(), "10/02/2016", "10/08/2016",
//				"10/02/2016");
//		saleItems.add(saleInfoDTO);
//		saleInfoDTO = TestHelper.getItemSaleInfoDTO(1, 2.69, PromoTypeLookup.STANDARD.getPromoTypeId(), "10/09/2016", "10/15/2016", "10/09/2016");
//		saleItems.add(saleInfoDTO);
//		saleDetailMap.put(35328, saleItems);
//
//		/* Main Item */
//		PRItemDTO mainItemDTO = TestHelper.getTestItem1(828396, 1, 4.19, null, null, null, 0, 0, null, null, 0, 0, null, null, "",
//				MAIN_ITEM_RET_LIR_ID, false, 0, 100d, 0, 90d, 1, 4.29, null);
//		recommendedItems.add(mainItemDTO);
//
//		/* Main Item LIR */
//		PRItemDTO mainItemLIRDTO = TestHelper.getTestItem1(MAIN_ITEM_RET_LIR_ID, 1, 4.19, null, null, null, 0, 0, null, null, 0, 0, null, null, "",
//				MAIN_ITEM_RET_LIR_ID, true, 0, 120d, 0, 100d, 1, 4.29, null);
//		recommendedItems.add(mainItemLIRDTO);
//
//		/* Subs Item LIR */
//		PRItemDTO subsItemDTO = TestHelper.getTestItem1(SUBS_ITEM_RET_LIR_ID, 1, 4.19, null, null, null, 0, 0, null, null, 0, 0, null, null, "",
//				SUBS_ITEM_RET_LIR_ID, true, 0, 0, 0, 0, 1, 4.29, null);
//		recommendedItems.add(subsItemDTO);
//
//		/* Expected output */
//		SubstituteAdjRInputDTO subsAdjRInputDTO = new SubstituteAdjRInputDTO();
//		subsAdjRInputDTO.setMainItems(new ArrayList<SubjstituteAdjRMainItemDTO>());
//		List<SubstituteAdjROutputDTO> rOutput = new ArrayList<SubstituteAdjROutputDTO>();
//		SubstituteAdjROutputDTO rOutputDTO = new SubstituteAdjROutputDTO();
//		rOutputDTO.setAdjRegMov(110d);
//		rOutputDTO.setItemKey(new ItemKey(828396, PRConstants.NON_LIG_ITEM_INDICATOR));
//		rOutput.add(rOutputDTO);
//		
//		EasyMock.expect(substituteAdjustmentRServiceMock.getSubsAdjustedMov(EasyMock.anyObject())).andReturn(rOutput).anyTimes();
////		EasyMock.replay(substituteAdjustmentRServiceMock);
//		
//		HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetailMock = new HashMap<PredictionDetailKey, PredictionDetailDTO>();
//		pdk = TestHelper.getPredictionDetailKey(locationLevelId, locationId, 828396, 1, 4.29d, 0, 0d, 0, 0, 0, 0);
//		pd = TestHelper.getPredictionDetailDTO(0, 0, locationLevelId, locationId, 828396, 1, 4.29d, 0, 0d, 0, 0, 0, 0, 110, 0, 0);
//		predictionDetailMock.put(pdk, pd);
//		
////		pdk = TestHelper.getPredictionDetailKey(locationLevelId, locationId, 828396, 1, 4.19d, 1, 4.29d, 0, 0, 0, 0);
////		pd = TestHelper.getPredictionDetailDTO(0, 0, locationLevelId, locationId, 828396, 1, 4.19d, 0, 0d, 0, 0, 0, 0, 110, 0, 0);
////		predictionDetailMock.put(pdk, pd);
//		
//		EasyMock.expect(predictionDAOMock.getAllPredictedMovement(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt(), EasyMock.anyInt(),
//				EasyMock.anyInt(), EasyMock.anyObject(List.class), EasyMock.anyBoolean())).andReturn(predictionDetailMock).anyTimes();
//		EasyMock.replay(substituteAdjustmentRServiceMock, predictionDAOMock);
//		
//		susbstituteService.adjustPredWithSubsEffect(null, locationLevelId, locationId, productLevelId, productId, recWeekCalendarId,
//				substituteItemMap, recommendedItems, saleDetailMap, curCalDTO, recommendationRunHeader);
//
//		/* Check item movement adjustment */
//		assertEquals("Main Item rec adjustment not matching", mainItemDTO.getPredictedMovement(), (Double) rOutput.get(0).getAdjRegMov());
//		/* Check lig movement adjustment */
//	}
}
