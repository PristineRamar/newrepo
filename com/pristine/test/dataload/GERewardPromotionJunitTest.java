package com.pristine.test.dataload;

import static org.junit.Assert.assertEquals;

import java.util.*;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.pristine.dataload.gianteagle.RewardPromotionSetup;
import com.pristine.dto.fileformatter.gianteagle.GERewardPromotionDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.lookup.offermgmt.promotion.GEPromoCodeLookup;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.test.offermgmt.promotion.TestHelper;

public class GERewardPromotionJunitTest {
	private static Logger logger = Logger.getLogger("ItemFileFormatterGEJUnit");
	RewardPromotionSetup rewardPromotionSetup = new RewardPromotionSetup();;
	
	/**
	 * Get set of locations which has same items and regular price
	 */
	@Test
	public void testCase1() {
		
		List<GERewardPromotionDTO> rewardPromoSet = new ArrayList<>();

		// Location 1
		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 12,"12",rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(124, 1, 3.99, 12,"12", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(125, 1, 4.99, 12,"12", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(126, 1, 5.99, 12,"12", rewardPromoSet);

		// Location 2
		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 13,"13", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(124, 1, 3.99, 13,"14", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(125, 1, 4.99, 13,"15", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(126, 1, 5.99, 13,"16", rewardPromoSet);
		
		// Location 3
		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 14,"22", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(124, 1, 3.99, 14,"34", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(125, 1, 4.99, 14,"45", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(126, 1, 5.99, 14,"54", rewardPromoSet);

		HashMap<List<GERewardPromotionDTO>, Set<LocationKey>> promoItemsAndLocation = rewardPromotionSetup.getLocationBasedOnItemWithPrice(rewardPromoSet);
		
		assertEquals("HashMap values is not matching", 1,promoItemsAndLocation.size());
		promoItemsAndLocation.forEach((key,value)->{
			assertEquals("HashMap values is not matching", 3,value.size());
		});
	}
	
	
	/**
	 * Get set of locations with different Regular price
	 */
	@Test
	public void testCase2() {
		
		List<GERewardPromotionDTO> rewardPromoSet = new ArrayList<>();

		// Location 1
		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 12,"12",rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(124, 1, 3.99, 12,"12", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(125, 1, 4.99, 12,"12", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(126, 1, 5.99, 12,"12", rewardPromoSet);

		// Location 2
		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 13,"13", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(124, 1, 3.99, 13,"14", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(125, 1, 4.99, 13,"15", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(126, 1, 5.99, 13,"16", rewardPromoSet);
		
		// Location 3
		TestHelper.setGERewardPromotionDetail(123, 1, 4.99, 14,"22", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(124, 1, 3.99, 14,"34", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(125, 1, 4.99, 14,"45", rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(126, 1, 5.99, 14,"54", rewardPromoSet);

		HashMap<List<GERewardPromotionDTO>, Set<LocationKey>> promoItemsAndLocation = rewardPromotionSetup.getLocationBasedOnItemWithPrice(rewardPromoSet);
		
		assertEquals("HashMap values is not matching", 2,promoItemsAndLocation.size());
//		promoItemsAndLocation.forEach((key,value)->{
//			assertEquals("HashMap values is not matching", 3,value.size());
//		});
	}
	
	
	/**
	 * Promo type TRUE BOGO
	 */
	@Test
	public void testCase3() {
		
		List<GERewardPromotionDTO> rewardPromoSet = new ArrayList<>();
		HashMap<String, List<GERewardPromotionDTO>> inputMap = new HashMap<>();

		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 12,"12",String.valueOf(GEPromoCodeLookup.TRUE_BOGO.getPromoCode()), rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 13,"13",String.valueOf(GEPromoCodeLookup.TRUE_BOGO.getPromoCode()), rewardPromoSet);
		inputMap.put("123", rewardPromoSet);
		
		int expectedPromoTypeId = PromoTypeLookup.BOGO.getPromoTypeId();
		
		inputMap.forEach((key, value) ->{
			value.forEach(promoDTO -> {
				assertEquals("Promo type not matching", expectedPromoTypeId, promoDTO.getPromoTypeId());
			});
		});
	}
	
	
	/**
	 * Promo type AMOUNT OFF Promo
	 */
	@Test
	public void testCase4() {
		
		List<GERewardPromotionDTO> rewardPromoSet = new ArrayList<>();
		HashMap<String, List<GERewardPromotionDTO>> inputMap = new HashMap<>();

		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 12,"12",String.valueOf(GEPromoCodeLookup.AMOUNT_OFF.getPromoCode()), rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 13,"13",String.valueOf(GEPromoCodeLookup.AMOUNT_OFF.getPromoCode()), rewardPromoSet);
		inputMap.put("123", rewardPromoSet);
		
		int expectedPromoTypeId = PromoTypeLookup.AMOUNT_OFF.getPromoTypeId();
		
		inputMap.forEach((key, value) ->{
			value.forEach(promoDTO -> {
				assertEquals("Promo type not matching", expectedPromoTypeId, promoDTO.getPromoTypeId());
			});
		});
	}
	
	
	/**
	 * Promo type BUY X GET Y (FREE ITEM)
	 */
	@Test
	public void testCase5() {
		
		List<GERewardPromotionDTO> rewardPromoSet = new ArrayList<>();
		HashMap<String, List<GERewardPromotionDTO>> inputMap = new HashMap<>();

		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 12,"12",String.valueOf(GEPromoCodeLookup.FREE_ITEM.getPromoCode()), rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 13,"13",String.valueOf(GEPromoCodeLookup.FREE_ITEM.getPromoCode()), rewardPromoSet);
		inputMap.put("123", rewardPromoSet);
		
		int expectedPromoTypeId = PromoTypeLookup.BUX_X_GET_Y_DIFF.getPromoTypeId();
		
		inputMap.forEach((key, value) ->{
			value.forEach(promoDTO -> {
				assertEquals("Promo type not matching", expectedPromoTypeId, promoDTO.getPromoTypeId());
			});
		});
	}
	
	
	/**
	 * Promo type BUY X GET Y (FREE ITEM)
	 */
	@Test
	public void testCase6() {
		
		List<GERewardPromotionDTO> rewardPromoSet = new ArrayList<>();
		HashMap<String, List<GERewardPromotionDTO>> inputMap = new HashMap<>();

		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 12,"12",String.valueOf(GEPromoCodeLookup.GEAC.getPromoCode()), rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 13,"13",String.valueOf(GEPromoCodeLookup.GEAC.getPromoCode()), rewardPromoSet);
		inputMap.put("123", rewardPromoSet);
		
		int expectedPromoTypeId = PromoTypeLookup.MUST_BUY.getPromoTypeId();
		
		inputMap.forEach((key, value) ->{
			value.forEach(promoDTO -> {
				assertEquals("Promo type not matching", expectedPromoTypeId, promoDTO.getPromoTypeId());
			});
		});
	}
	
	
	/**
	 * Promo type PICK 5 promotions
	 */
	@Test
	public void testCase7() {
		
		List<GERewardPromotionDTO> rewardPromoSet = new ArrayList<>();
		HashMap<String, List<GERewardPromotionDTO>> inputMap = new HashMap<>();

		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 12,"12",String.valueOf(GEPromoCodeLookup.PICK_5.getPromoCode()), rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 13,"13",String.valueOf(GEPromoCodeLookup.PICK_5.getPromoCode()), rewardPromoSet);
		inputMap.put("123", rewardPromoSet);
		
		int expectedPromoTypeId = PromoTypeLookup.PICK_5.getPromoTypeId();
		
		inputMap.forEach((key, value) ->{
			value.forEach(promoDTO -> {
				assertEquals("Promo type not matching", expectedPromoTypeId, promoDTO.getPromoTypeId());
			});
		});
	}
	
	
	/**
	 * Promo type PICK 5 promotions
	 */
	@Test
	public void testCase8() {
		
		List<GERewardPromotionDTO> rewardPromoSet = new ArrayList<>();
		HashMap<String, List<GERewardPromotionDTO>> inputMap = new HashMap<>();

		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 12,"12",String.valueOf(GEPromoCodeLookup.PICK_5.getPromoCode()), rewardPromoSet);
		TestHelper.setGERewardPromotionDetail(123, 1, 2.99, 13,"13",String.valueOf(GEPromoCodeLookup.PICK_5.getPromoCode()), rewardPromoSet);
		inputMap.put("123", rewardPromoSet);
		
		int expectedPromoTypeId = PromoTypeLookup.PICK_5.getPromoTypeId();
		
		inputMap.forEach((key, value) ->{
			value.forEach(promoDTO -> {
				assertEquals("Promo type not matching", expectedPromoTypeId, promoDTO.getPromoTypeId());
			});
		});
	}
	
}
