package com.pristine.test.offermgmt.promotion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.offermgmt.promotion.BlockDetail;
import com.pristine.dto.offermgmt.promotion.PageBlockNoKey;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.service.offermgmt.promotion.AdFinalizationService;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;

public class AdFinalizeItemJunitTest {
	AdFinalizationService adFinalizationService = null;
	int PAGE_NO_1 = 1, BLOCK_NO_1 = 1, DEPT_ID_1 = 1, LIG_1=1, LIG_2 = 2, LIG_3 = 3, LIG_4 = 4;
	int PPG_GROUP_ID_1 = 1;
	String DEPT_NAME_1 = "Dept1";
	
	HashMap<PageBlockNoKey, BlockDetail> actualAdBlockDetails =  null;
	HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsLIGNonLigSummary =  null;
	List<PromoItemDTO> indItemSummary =  null;
	List<PromoItemDTO> ppgAndItsItemSummary  =  null;
	HashMap<Integer, String> deptDetail = null;
	List<PromoItemDTO> itemList = null;
	PageBlockNoKey pageBlockNoKey = null;
	PromoItemDTO promoItemDTO = null;
	
	@Before
	public void init() {
		PropertyManager.initialize("recommendation.properties");
	}

	
	/***
	 * Number of actual blocks: 1, 
	 * Ind or PPG items not present in actual block
	 * 1 Ind LIG Item, 1 PPG with 2 LIG's
	 * Ind gives better results
	 */
	@Test
	public void testCase1() {
		adFinalizationService = new AdFinalizationService();
		actualAdBlockDetails = new HashMap<PageBlockNoKey, BlockDetail>();
		actualAdItemsLIGNonLigSummary = new HashMap<PageBlockNoKey, List<PromoItemDTO>>();
		indItemSummary = new ArrayList<PromoItemDTO>();
		ppgAndItsItemSummary = new ArrayList<PromoItemDTO>();
		
		// Actual ad block details
		deptDetail = new HashMap<Integer, String>();
		deptDetail.put(DEPT_ID_1, DEPT_NAME_1);
		pageBlockNoKey = new PageBlockNoKey(PAGE_NO_1, BLOCK_NO_1);
		BlockDetail blockDetail = TestHelper.setBlockDetail(PAGE_NO_1, BLOCK_NO_1, deptDetail, 1500);
		actualAdBlockDetails.put(pageBlockNoKey, blockDetail);
		
		// Actual ad LIG and Non Lig Summary
		itemList = new ArrayList<PromoItemDTO>();
		promoItemDTO = TestHelper.setBasicPromoItemDetail(Constants.PRODUCT_LEVEL_ID_LIG, LIG_1, DEPT_ID_1, false, 0);
		TestHelper.updatePromoItemPredictions(promoItemDTO, 1000, 1500, 200, 200, 300, 250, 1250);
		itemList.add(promoItemDTO);
		actualAdItemsLIGNonLigSummary.put(pageBlockNoKey, itemList);
		
		// Ind Item Summary
		promoItemDTO = TestHelper.setBasicPromoItemDetail(Constants.PRODUCT_LEVEL_ID_LIG, LIG_2, DEPT_ID_1, false, 0);
		TestHelper.updatePromoItemPredictions(promoItemDTO, 1200, 2500, 300, 150, 400, 200, 1400);
		indItemSummary.add(promoItemDTO);
		
		// PPG level Summary
		promoItemDTO = TestHelper.setBasicPromoItemDetail(0, 0, DEPT_ID_1, true, PPG_GROUP_ID_1);
		TestHelper.updatePromoItemPredictions(promoItemDTO, 1100, 2200, 250, 150, 400, 200, 1600);
		ppgAndItsItemSummary.add(promoItemDTO);
		
		// PPG Items
		promoItemDTO = TestHelper.setBasicPromoItemDetail(Constants.PRODUCT_LEVEL_ID_LIG, LIG_3, DEPT_ID_1, false, PPG_GROUP_ID_1);
		TestHelper.updatePromoItemPredictions(promoItemDTO, 600, 1200, 150, 150, 250, 100, 800);
		ppgAndItsItemSummary.add(promoItemDTO);
		promoItemDTO = TestHelper.setBasicPromoItemDetail(Constants.PRODUCT_LEVEL_ID_LIG, LIG_4, DEPT_ID_1, false, PPG_GROUP_ID_1);
		TestHelper.updatePromoItemPredictions(promoItemDTO, 500, 1000, 100, 20, 150, 100, 800);
		ppgAndItsItemSummary.add(promoItemDTO);
		
		
		HashMap<PageBlockNoKey, List<PromoItemDTO>> recAdItemsLIGNonLigSummary = adFinalizationService.finalizeItems(actualAdBlockDetails,
				actualAdItemsLIGNonLigSummary, indItemSummary, ppgAndItsItemSummary, 10);
		
		
		
	}
	
	
}
