package com.pristine.dataload.riteaid;

import static org.junit.Assert.assertEquals;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;
import org.junit.Test;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dataload.service.PromotionService;
import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.dto.offermgmt.promotion.PromoBuyRequirement;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
import com.pristine.dto.offermgmt.promotion.PromoOfferDetail;
import com.pristine.dto.riteaid.RAPromoFileDTO;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRFormatHelper;

/**
 * 
 * @author Pradeepkumar
 *
 */
public class PromoLoaderJunitTestcases {

	ObjectMapper mapper = new ObjectMapper();
	
	@Test
	public void testSimpleStandardPromo(){
		RAPromoFileDTO promoFileDTO = new RAPromoFileDTO();
		promoFileDTO.setRegPrice(5.99);
		promoFileDTO.setSalePrice(3.99);
		promoFileDTO.setSaleQty(1);
		promoFileDTO.setCpnDesc("");
		promoFileDTO.setItemPriceCode("F");
		PromoDefinition expected = new PromoDefinition();
		expected.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
		PromoDefinition actual = new PromoDefinition();
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.identifyAndSetPromotionType(promoFileDTO, actual);
		
		assertEquals("Promo type not matching", expected.getPromoDefnTypeId(), actual.getPromoDefnTypeId());
	}

	@Test
	public void testSimpleStandardPromoWithPctOff(){
		RAPromoFileDTO promoFileDTO = new RAPromoFileDTO();
		promoFileDTO.setRegPrice(5.99);
		promoFileDTO.setSalePrice(50);
		promoFileDTO.setItemPriceCode("P");
		promoFileDTO.setSaleQty(1);
		promoFileDTO.setCpnDesc("");
		promoFileDTO.setGroupPromoType("");
		PromoDefinition expected = new PromoDefinition();
		expected.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
		
		PromoDefinition actual = new PromoDefinition();
		//expected.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.identifyAndSetPromotionType(promoFileDTO, actual);
		
		assertEquals("Promo type not matching", expected.getPromoDefnTypeId(), actual.getPromoDefnTypeId());
	}
	
	@Test
	public void testSimpleStandardPromoWithDollarOff(){
		RAPromoFileDTO promoFileDTO = new RAPromoFileDTO();
		promoFileDTO.setRegPrice(5.99);
		promoFileDTO.setSalePrice(2);
		promoFileDTO.setItemPriceCode("D");
		promoFileDTO.setSaleQty(1);
		promoFileDTO.setCpnDesc("");
		promoFileDTO.setGroupPromoType("");
		PromoDefinition expected = new PromoDefinition();
		expected.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
		
		PromoDefinition actual = new PromoDefinition();
		PromotionService promotionService = new PromotionService();
		
		promotionService.identifyAndSetPromotionType(promoFileDTO, actual);
		
		assertEquals("Promo type not matching", expected.getPromoDefnTypeId(), actual.getPromoDefnTypeId());
	}
	
	@Test
	public void testBOGOPromo(){
		RAPromoFileDTO promoFileDTO = new RAPromoFileDTO();
		promoFileDTO.setRegPrice(5.99);
		promoFileDTO.setSalePrice(0);
		promoFileDTO.setItemPriceCode("F");
		promoFileDTO.setSaleQty(1);
		promoFileDTO.setBuyQty(1);
		promoFileDTO.setGetQty(1);
		
		PromoDefinition expected = new PromoDefinition();
		expected.setPromoDefnTypeId(Constants.PROMO_TYPE_BOGO);
		
		PromoDefinition actual = new PromoDefinition();
		//expected.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.identifyAndSetPromotionType(promoFileDTO, actual);
		
		assertEquals("Promo type not matching", expected.getPromoDefnTypeId(), actual.getPromoDefnTypeId());
	}
	
	@Test
	public void testBOGOWith50PctOff(){
		RAPromoFileDTO promoFileDTO = new RAPromoFileDTO();
		promoFileDTO.setRegPrice(5.99);
		promoFileDTO.setSalePrice(50);
		promoFileDTO.setItemPriceCode("P");
		promoFileDTO.setSaleQty(1);
		promoFileDTO.setBuyQty(1);
		promoFileDTO.setGetQty(1);
		
		PromoDefinition expected = new PromoDefinition();
		expected.setPromoDefnTypeId(Constants.PROMO_TYPE_BOGO);
		
		PromoDefinition actual = new PromoDefinition();
		//expected.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.identifyAndSetPromotionType(promoFileDTO, actual);
		
		assertEquals("Promo type not matching", expected.getPromoDefnTypeId(), actual.getPromoDefnTypeId());
	}

	
	@Test
	public void testMustBuy() throws JsonProcessingException{
		RAPromoFileDTO promoFileDTO = new RAPromoFileDTO();
		promoFileDTO.setRegPrice(5.99);
		promoFileDTO.setSalePrice(7.99);
		promoFileDTO.setItemPriceCode("F");
		promoFileDTO.setSecondItemPrice(4.99);
		promoFileDTO.setSecondItemQty(1);
		promoFileDTO.setSaleQty(2);
		PromoDefinition expected = new PromoDefinition();
		expected.setPromoDefnTypeId(Constants.PROMO_TYPE_MUST_BUY);
		
		PromoDefinition actual = new PromoDefinition();
		//expected.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.identifyAndSetPromotionType(promoFileDTO, actual);
		
		assertEquals("Promo type not matching", expected.getPromoDefnTypeId(), actual.getPromoDefnTypeId());
		
		
		PromoBuyItems buyItemInput = new PromoBuyItems();
			
		promotionService.setSalePrice(buyItemInput, promoFileDTO);
		
		PromoBuyItems buyItemExpected = new PromoBuyItems();
		buyItemExpected.setRegPrice(promoFileDTO.getRegPrice());
		buyItemExpected.setRegQty(1);
		buyItemExpected.setSaleQty(2);
		buyItemExpected.setSalePrice(7.99);
		
		assertEquals("Sale price not matching", new Double(buyItemExpected.getSalePrice()), new Double(buyItemInput.getSalePrice()));
		
	}
	
	@Test
	public void testOverlay(){
		RAPromoFileDTO promoFileDTO = new RAPromoFileDTO();
		promoFileDTO.setRegPrice(5.99);
		promoFileDTO.setSalePrice(4.99);
		promoFileDTO.setItemPriceCode("F");
		promoFileDTO.setSaleQty(1);
		promoFileDTO.setCpnDesc("Buy $20 Get $5");
		promoFileDTO.setThresholdType("Q");
		promoFileDTO.setThresholdValue(20);
		PromoDefinition expected = new PromoDefinition();
		expected.setPromoDefnTypeId(Constants.PROMO_TYPE_EBONUS_COUPON);
		
		PromoDefinition actual = new PromoDefinition();
		//expected.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.identifyAndSetPromotionType(promoFileDTO, actual);
		
		assertEquals("Promo type not matching", expected.getPromoDefnTypeId(), actual.getPromoDefnTypeId());
	}
	
	
	@Test
	public void testInAdCoupon(){
		RAPromoFileDTO promoFileDTO = new RAPromoFileDTO();
		promoFileDTO.setRegPrice(5.99);
		promoFileDTO.setSalePrice(0);
		promoFileDTO.setItemPriceCode("D");
		promoFileDTO.setSaleQty(1);
		promoFileDTO.setCpnDesc("");
		promoFileDTO.setCpnAmount(2);
		PromoDefinition expected = new PromoDefinition();
		expected.setPromoDefnTypeId(Constants.PROMO_TYPE_IN_AD_COUPON);
		
		PromoDefinition actual = new PromoDefinition();
		//expected.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.identifyAndSetPromotionType(promoFileDTO, actual);
		
		assertEquals("Promo type not matching", expected.getPromoDefnTypeId(), actual.getPromoDefnTypeId());
	}

	@Test
	public void testSalePriceForBOGO50Pct() throws JsonProcessingException{
		RAPromoFileDTO promoFileDTO = new RAPromoFileDTO();
		promoFileDTO.setRegPrice(5.99);
		promoFileDTO.setSalePrice(50);
		promoFileDTO.setItemPriceCode("P");
		promoFileDTO.setGroupPromoType("BOGO");
		promoFileDTO.setSaleQty(1);
		promoFileDTO.setCpnDesc("");
		promoFileDTO.setBuyQty(1);
		promoFileDTO.setGetQty(1);
		PromoDefinition expected = new PromoDefinition();
		expected.setPromoDefnTypeId(Constants.PROMO_TYPE_BOGO);
		
		PromoDefinition actual = new PromoDefinition();
		//expected.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.identifyAndSetPromotionType(promoFileDTO, actual);
		
		assertEquals("Promo type not matching", expected.getPromoDefnTypeId(), actual.getPromoDefnTypeId());
		
		PromoBuyItems buyItemInput = new PromoBuyItems();
		
		promotionService.setSalePrice(buyItemInput, promoFileDTO);
		
		PromoBuyItems buyItemExpected = new PromoBuyItems();
		buyItemExpected.setRegPrice(promoFileDTO.getRegPrice());
		buyItemExpected.setRegQty(1);
		
		double discount = (double)(promoFileDTO.getSalePrice() * promoFileDTO.getRegPrice()) / 100;
		double secondItemPrice = promoFileDTO.getRegPrice() - discount;
		buyItemExpected.setSaleQty(2);
		double salePr = promoFileDTO.getRegPrice() + secondItemPrice;
		buyItemExpected.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePr)));
		
		assertEquals("Sale price not matching", new Double(buyItemExpected.getSalePrice()), new Double(buyItemInput.getSalePrice()));
		
	}
	
	
	
	/*
	1.	Buy $20 Get $5
	2.	Buy $20 Earn $5
	3.	Spend $20 Get $5
	4.	Spend $20 Earn $5
	5.	$5 with Purchase of …
	6.	$5 Bonus cash when you buy 2…
	7.	$5 BC with purchase of…
	*/
	@Test
	//Buy $20 Earn $5
	public void testCouponPattern1(){
		
		PromoAndAdLoader adLoader = new PromoAndAdLoader();
		HashMap<Pattern, Integer> compiledPatterns = adLoader.compilePatterns();
		PromoDefinition promoDefinition = new PromoDefinition();
		promoDefinition.setCouponDesc("Buy $20 Earn $5");
		promoDefinition.setThresholdType("D");
		promoDefinition.setThresholdValue(20);
		
		
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		
		PromoBuyRequirement expectedBuyReq = new PromoBuyRequirement();
		expectedBuyReq.setMinAmtReqd(20.0);
		
		PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
		promoOfferDetail.setOfferValue(5.0);
		promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_COUPON);
		promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
		expectedBuyReq.addOfferDetail(promoOfferDetail);
		
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.parseCouponDescAndSetAddlDetails(promoDefinition, buyReq, compiledPatterns, new HashSet<>());
		
		assertEquals("Thresholds not matching", expectedBuyReq.getMinAmtReqd(), buyReq.getMinAmtReqd());
		
		assertEquals("Pattern not found", promoOfferDetail.getOfferValue(), buyReq.getOfferDetail().get(0).getOfferValue());
	}
	
	@Test
	//Buy $20 Get $5
	public void testCouponPattern2(){
		
		PromoAndAdLoader adLoader = new PromoAndAdLoader();
		HashMap<Pattern, Integer> compiledPatterns = adLoader.compilePatterns();
		PromoDefinition promoDefinition = new PromoDefinition();
		promoDefinition.setCouponDesc("Buy $20 Get $5");
		promoDefinition.setThresholdType("D");
		promoDefinition.setThresholdValue(20);
		
		
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		
		PromoBuyRequirement expectedBuyReq = new PromoBuyRequirement();
		expectedBuyReq.setMinAmtReqd(20.0);
		
		PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
		promoOfferDetail.setOfferValue(5.0);
		promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_COUPON);
		promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
		expectedBuyReq.addOfferDetail(promoOfferDetail);
		
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.parseCouponDescAndSetAddlDetails(promoDefinition, buyReq, compiledPatterns, new HashSet<>());
		
		assertEquals("Thresholds not matching", expectedBuyReq.getMinAmtReqd(), buyReq.getMinAmtReqd());
		
		assertEquals("Pattern not found", promoOfferDetail.getOfferValue(), buyReq.getOfferDetail().get(0).getOfferValue());
	}
	
	@Test
	//Spend $20 Get $5
	public void testCouponPattern3(){
		
		PromoAndAdLoader adLoader = new PromoAndAdLoader();
		HashMap<Pattern, Integer> compiledPatterns = adLoader.compilePatterns();
		PromoDefinition promoDefinition = new PromoDefinition();
		promoDefinition.setCouponDesc("Spend $20.00 Get $5.00");
		promoDefinition.setThresholdType("D");
		promoDefinition.setThresholdValue(20);
		
		
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		
		PromoBuyRequirement expectedBuyReq = new PromoBuyRequirement();
		expectedBuyReq.setMinAmtReqd(20.0);
		
		PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
		promoOfferDetail.setOfferValue(5.0);
		promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_COUPON);
		promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
		expectedBuyReq.addOfferDetail(promoOfferDetail);
		
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.parseCouponDescAndSetAddlDetails(promoDefinition, buyReq, compiledPatterns, new HashSet<>());
		
		assertEquals("Thresholds not matching", expectedBuyReq.getMinAmtReqd(), buyReq.getMinAmtReqd());
		
		assertEquals("Pattern not found", promoOfferDetail.getOfferValue(), buyReq.getOfferDetail().get(0).getOfferValue());
	}
	
	@Test
	//$5 with Purchase of
	public void testCouponPattern4(){
		
		PromoAndAdLoader adLoader = new PromoAndAdLoader();
		HashMap<Pattern, Integer> compiledPatterns = adLoader.compilePatterns();
		PromoDefinition promoDefinition = new PromoDefinition();
		promoDefinition.setCouponDesc("$5 with Purchase of");
		promoDefinition.setThresholdType("D");
		promoDefinition.setThresholdValue(20);
		
		
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		
		PromoBuyRequirement expectedBuyReq = new PromoBuyRequirement();
		expectedBuyReq.setMinAmtReqd(20.0);
		
		PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
		promoOfferDetail.setOfferValue(5.0);
		promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_COUPON);
		promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
		expectedBuyReq.addOfferDetail(promoOfferDetail);
		
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.parseCouponDescAndSetAddlDetails(promoDefinition, buyReq, compiledPatterns, new HashSet<>());
		
		assertEquals("Thresholds not matching", expectedBuyReq.getMinAmtReqd(), buyReq.getMinAmtReqd());
		
		assertEquals("Pattern not found", promoOfferDetail.getOfferValue(), buyReq.getOfferDetail().get(0).getOfferValue());
	}
	
	@Test
	//$5 Bonus cash when you buy 2…
	public void testCouponPattern5(){
		
		PromoAndAdLoader adLoader = new PromoAndAdLoader();
		HashMap<Pattern, Integer> compiledPatterns = adLoader.compilePatterns();
		PromoDefinition promoDefinition = new PromoDefinition();
		promoDefinition.setCouponDesc("$5 Bonus cash when you buy 2…");
		promoDefinition.setThresholdType("D");
		promoDefinition.setThresholdValue(20);
		
		
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		
		PromoBuyRequirement expectedBuyReq = new PromoBuyRequirement();
		expectedBuyReq.setMinAmtReqd(20.0);
		
		PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
		promoOfferDetail.setOfferValue(5.0);
		promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_COUPON);
		promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
		expectedBuyReq.addOfferDetail(promoOfferDetail);
		
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.parseCouponDescAndSetAddlDetails(promoDefinition, buyReq, compiledPatterns, new HashSet<>());
		
		assertEquals("Thresholds not matching", expectedBuyReq.getMinAmtReqd(), buyReq.getMinAmtReqd());
		
		assertEquals("Pattern not found", promoOfferDetail.getOfferValue(), buyReq.getOfferDetail().get(0).getOfferValue());
	}
	
	@Test
	//$5 BC with purchase of…
	public void testCouponPattern6(){
		
		PromoAndAdLoader adLoader = new PromoAndAdLoader();
		HashMap<Pattern, Integer> compiledPatterns = adLoader.compilePatterns();
		PromoDefinition promoDefinition = new PromoDefinition();
		promoDefinition.setCouponDesc("$5 BC with purchase of…");
		promoDefinition.setThresholdType("D");
		promoDefinition.setThresholdValue(20);
		
		
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		
		PromoBuyRequirement expectedBuyReq = new PromoBuyRequirement();
		expectedBuyReq.setMinAmtReqd(20.0);
		
		PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
		promoOfferDetail.setOfferValue(5.0);
		promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_COUPON);
		promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
		expectedBuyReq.addOfferDetail(promoOfferDetail);
		
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.parseCouponDescAndSetAddlDetails(promoDefinition, buyReq, compiledPatterns, new HashSet<>());
		
		assertEquals("Thresholds not matching", expectedBuyReq.getMinAmtReqd(), buyReq.getMinAmtReqd());
		
		assertEquals("Pattern not found", promoOfferDetail.getOfferValue(), buyReq.getOfferDetail().get(0).getOfferValue());
	}
	
	@Test
	//input any data from file
	public void testCouponPattern7(){
		
		PromoAndAdLoader adLoader = new PromoAndAdLoader();
		HashMap<Pattern, Integer> compiledPatterns = adLoader.compilePatterns();
		PromoDefinition promoDefinition = new PromoDefinition();
		promoDefinition.setCouponDesc("Reef Apothcare Buy $15 Get $5 BC 3/4-3/31/18");
		promoDefinition.setThresholdType("D");
		promoDefinition.setThresholdValue(20);
		
		
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		
		PromoBuyRequirement expectedBuyReq = new PromoBuyRequirement();
		expectedBuyReq.setMinAmtReqd(20.0);
		
		PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
		promoOfferDetail.setOfferValue(5.0);
		promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_COUPON);
		promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
		expectedBuyReq.addOfferDetail(promoOfferDetail);
		
		PromotionService promotionService = new PromotionService();
		
		promotionService.parseCouponDescAndSetAddlDetails(promoDefinition, buyReq, compiledPatterns, new HashSet<>());
		
		assertEquals("Thresholds not matching", expectedBuyReq.getMinAmtReqd(), buyReq.getMinAmtReqd());
		
		assertEquals("Pattern not found", promoOfferDetail.getOfferValue(), buyReq.getOfferDetail().get(0).getOfferValue());
	}
}
