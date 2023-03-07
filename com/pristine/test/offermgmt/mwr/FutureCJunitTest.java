package com.pristine.test.offermgmt.mwr;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.PriceReactionTimeLookup;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;

public class FutureCJunitTest {

	PRItemDTO itemDTO;
	String recWeekStartDate = "";

	@Before
	public void init() {
		PropertyManager.initialize("com/pristine/test/offermgmt/AllClients.properties");
	}

	@Test
	public void Case1NonPromo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("07/10/2021");

		int priceflag = PriceReactionTimeLookup.IMMEDIATE.getTypeId();

		PricingEngineService pr = new PricingEngineService();

		String effectiveDate = pr.setEffectiveDateNonpromo(priceflag, futureCostEffDate);

		String expectedDate = "";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	public void Case2NonPromo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("06/22/2021");

		int priceflag = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();

		PricingEngineService pr = new PricingEngineService();

		String effectiveDate = pr.setEffectiveDateNonpromo(priceflag, futureCostEffDate);

		String expectedDate = "06/16/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	public void Case3NonPromo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("07/10/2021");

		int priceflag = PriceReactionTimeLookup.WEEK_AFTER.getTypeId();

		PricingEngineService pr = new PricingEngineService();

		String effectiveDate = pr.setEffectiveDateNonpromo(priceflag, futureCostEffDate);

		String expectedDate = "07/14/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	public void Case4NonPromo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("07/07/2021");

		int priceflag = PriceReactionTimeLookup.WEEK_AFTER.getTypeId();

		PricingEngineService pr = new PricingEngineService();

		String effectiveDate = pr.setEffectiveDateNonpromo(priceflag, futureCostEffDate);

		String expectedDate = "07/14/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	public void Case5NonPromo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("07/07/2021");

		int priceflag = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();

		PricingEngineService pr = new PricingEngineService();

		String effectiveDate = pr.setEffectiveDateNonpromo(priceflag, futureCostEffDate);

		String expectedDate = "06/30/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	public void Case6NonPromo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("07/11/2021");

		int priceflag = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();

		PricingEngineService pr = new PricingEngineService();

		String effectiveDate = pr.setEffectiveDateNonpromo(priceflag, futureCostEffDate);

		String expectedDate = "07/07/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	public void Case7NonPromo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("07/11/2021");

		int priceflag = PriceReactionTimeLookup.WEEK_AFTER.getTypeId();

		PricingEngineService pr = new PricingEngineService();

		String effectiveDate = pr.setEffectiveDateNonpromo(priceflag, futureCostEffDate);

		String expectedDate = "07/14/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	/*
	 * Cost effective date is before promotion starts and its eff in between week
	 * (Tue)
	 */
	public void Case1Promo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("06/22/2021");
		Date promoStart = DateUtil.toDate("07/04/2021");
		Date promoEnd = DateUtil.toDate("07/11/2021");
		itemDTO = new PRItemDTO();
		PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();
		saleInfoDTO.setSaleStartDate("07/04/2021");
		saleInfoDTO.setSaleEndDate("07/11/20211");
		itemDTO.setFutWeekSaleInfo(saleInfoDTO);
		recWeekStartDate = "06/27/2021";
		int priceflag = PriceReactionTimeLookup.IMMEDIATE.getTypeId();
		PricingEngineService pr = new PricingEngineService();
		String effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		String expectedDate = "";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "06/16/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_AFTER.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "06/30/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	/**
	 * cost eff date is before promotion starts and its effective on week end (Sun)
	 * 
	 * @throws GeneralException
	 */
	public void Case2Promo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("06/20/2021");
		Date promoStart = DateUtil.toDate("04/07/2021");
		Date promoEnd = DateUtil.toDate("04/11/2021");
		itemDTO = new PRItemDTO();
		PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();
		saleInfoDTO.setSaleStartDate("07/04/2021");
		saleInfoDTO.setSaleEndDate("07/11/2021");
		itemDTO.setFutWeekSaleInfo(saleInfoDTO);
		recWeekStartDate = "06/27/2021";
		int priceflag = PriceReactionTimeLookup.IMMEDIATE.getTypeId();
		PricingEngineService pr = new PricingEngineService();
		String effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		String expectedDate = "";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "06/16/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_AFTER.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "06/23/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	/**
	 * cost eff date is before promotion starts and its effective on Wednesday (Sun)
	 * 
	 * @throws GeneralException
	 */
	public void Case3Promo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("06/23/2021");
		Date promoStart = DateUtil.toDate("07/04/2021");
		Date promoEnd = DateUtil.toDate("07/11/2021");
		itemDTO = new PRItemDTO();
		PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();
		saleInfoDTO.setSaleStartDate("07/04/2021");
		saleInfoDTO.setSaleEndDate("07/11/2021");
		itemDTO.setFutWeekSaleInfo(saleInfoDTO);
		recWeekStartDate = "06/27/2021";
		int priceflag = PriceReactionTimeLookup.IMMEDIATE.getTypeId();
		PricingEngineService pr = new PricingEngineService();
		String effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		String expectedDate = "";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "06/16/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_AFTER.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "06/30/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	/**
	 * cost eff date is before promotion starts and its effective on Wednesday (Sun)
	 * 
	 * @throws GeneralException
	 */
	public void Case4Promo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("06/23/2021");
		Date promoStart = DateUtil.toDate("07/04/2021");
		Date promoEnd = DateUtil.toDate("07/11/2021");
		itemDTO = new PRItemDTO();
		PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();
		saleInfoDTO.setSaleStartDate("07/04/2021");
		saleInfoDTO.setSaleEndDate("07/11/2021");
		itemDTO.setFutWeekSaleInfo(saleInfoDTO);
		recWeekStartDate = "06/27/2021";
		int priceflag = PriceReactionTimeLookup.IMMEDIATE.getTypeId();
		PricingEngineService pr = new PricingEngineService();
		String effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		String expectedDate = "";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "06/16/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_AFTER.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "06/30/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	/**
	 * Cost eff date is same as promotion start date and promotion is starting on
	 * Sunday.Promotion can be single week or spans multiple week
	 * 
	 * @throws GeneralException
	 */
	public void Case5Promo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("07/04/2021");
		Date promoStart = DateUtil.toDate("07/04/2021");
		Date promoEnd = DateUtil.toDate("07/11/2021");
		itemDTO = new PRItemDTO();
		PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();
		saleInfoDTO.setSaleStartDate("07/04/2021");
		saleInfoDTO.setSaleEndDate("07/11/2021");
		itemDTO.setCurSaleInfo(saleInfoDTO);
		recWeekStartDate = "07/11/2021";
		int priceflag = PriceReactionTimeLookup.IMMEDIATE.getTypeId();
		PricingEngineService pr = new PricingEngineService();
		String effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		String expectedDate = "";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "06/30/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_AFTER.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "07/14/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	/**
	 * cost eff date is same as promotion start date and its effective mid day
	 * (wed)and promotion spans multiple weeks or is single week
	 * 
	 * @throws GeneralException
	 */
	public void Case6Promo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("07/07/2021");
		Date promoStart = DateUtil.toDate("07/07/2021");
		Date promoEnd = DateUtil.toDate("07/31/2021");
		itemDTO = new PRItemDTO();
		PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();
		saleInfoDTO.setSaleStartDate("07/07/2021");
		saleInfoDTO.setSaleEndDate("07/31/2021");
		itemDTO.setCurSaleInfo(saleInfoDTO);
		recWeekStartDate = "07/11/2021";
		int priceflag = PriceReactionTimeLookup.IMMEDIATE.getTypeId();
		PricingEngineService pr = new PricingEngineService();
		String effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		String expectedDate = "07/18/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "06/30/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_AFTER.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "08/04/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	/**
	 * Cost eff date is between a single week promotion
	 * 
	 * @throws GeneralException
	 */
	public void Case7Promo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("07/08/2021");
		Date promoStart = DateUtil.toDate("07/04/2021");
		Date promoEnd = DateUtil.toDate("07/10/2021");
		itemDTO = new PRItemDTO();
		PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();
		saleInfoDTO.setSaleStartDate("07/04/2021");
		saleInfoDTO.setSaleEndDate("07/10/2021");
		itemDTO.setCurSaleInfo(saleInfoDTO);
		recWeekStartDate = "07/11/2021";
		int priceflag = PriceReactionTimeLookup.IMMEDIATE.getTypeId();
		PricingEngineService pr = new PricingEngineService();
		String effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		String expectedDate = "";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "06/30/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_AFTER.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "07/14/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	/**
	 * cost eff date is in between a promotion which is spanning more than 1 week
	 * and promotion ends on Saturday
	 * 
	 * @throws GeneralException
	 */
	public void Case8Promo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("07/08/2021");
		Date promoStart = DateUtil.toDate("07/04/2021");
		Date promoEnd = DateUtil.toDate("07/31/2021");
		itemDTO = new PRItemDTO();
		PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();
		saleInfoDTO.setSaleStartDate("07/04/2021");
		saleInfoDTO.setSaleEndDate("07/31/2021");
		itemDTO.setCurSaleInfo(saleInfoDTO);
		recWeekStartDate = "07/11/2021";
		int priceflag = PriceReactionTimeLookup.IMMEDIATE.getTypeId();
		PricingEngineService pr = new PricingEngineService();
		String effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		String expectedDate = "07/18/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "08/04/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_AFTER.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "08/04/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	/**
	 * cost eff date is between a promotion which is spanning >1 week and promotion
	 * ends mid week
	 * 
	 * @throws GeneralException
	 */
	public void Case9Promo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("07/08/2021");
		Date promoStart = DateUtil.toDate("07/04/2021");
		Date promoEnd = DateUtil.toDate("07/29/2021");
		itemDTO = new PRItemDTO();
		PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();
		saleInfoDTO.setSaleStartDate("07/04/2021");
		saleInfoDTO.setSaleEndDate("07/31/2021");
		itemDTO.setCurSaleInfo(saleInfoDTO);
		recWeekStartDate = "07/11/2021";
		int priceflag = PriceReactionTimeLookup.IMMEDIATE.getTypeId();
		PricingEngineService pr = new PricingEngineService();
		String effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		String expectedDate = "07/18/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "08/04/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_AFTER.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "08/04/2021";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

	@Test
	/*
	 * when the cost effective date is after promotion starts and there is no
	 * promotion in recommendation week
	 */
	public void Case10Promo() throws GeneralException {

		Date futureCostEffDate = DateUtil.toDate("01/01/2023");
		Date promoStart = DateUtil.toDate("12/12/2022");
		Date promoEnd = DateUtil.toDate("12/18/2022");

		itemDTO = new PRItemDTO();
		PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();
		saleInfoDTO.setSaleStartDate("12/12/2022");
		saleInfoDTO.setSaleEndDate("12/18/2022");
		itemDTO.setFutWeekSaleInfo(saleInfoDTO);
		recWeekStartDate = "04/12/2023";
		int priceflag = PriceReactionTimeLookup.IMMEDIATE.getTypeId();

		PricingEngineService pr = new PricingEngineService();
		String effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		String expectedDate = "";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "12/28/2022";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

		priceflag = PriceReactionTimeLookup.WEEK_AFTER.getTypeId();
		effectiveDate = pr.setEffectiveDateForPromoItems(priceflag, futureCostEffDate, promoEnd, promoStart,
				recWeekStartDate, itemDTO);
		expectedDate = "01/04/2023";
		assertEquals("Price Eff Date not matching!!!", expectedDate, effectiveDate);

	}

}
