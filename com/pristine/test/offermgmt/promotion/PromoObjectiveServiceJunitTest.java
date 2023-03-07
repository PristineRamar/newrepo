package com.pristine.test.offermgmt.promotion;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.promotion.BlockDetail;
import com.pristine.dto.offermgmt.promotion.PageBlockNoKey;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.service.offermgmt.DisplayTypeLookup;
import com.pristine.service.offermgmt.promotion.AdFinalizationService;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PromoObjectiveServiceJunitTest {

	
	int productLevelId = 1, productId = 1234, deptId = 5;
	long ppgGroupId = 1;
	
	/*@Before
	public void init() {
		PropertyManager.initialize("recommendation.properties");
	}*/

	
	/*@Test
	public void testCase1() {
		List<PromoItemDTO> promoScenarios = new ArrayList<>();

		// Promo scenario 1
		PromoItemDTO promoItemDTO1 = TestHelper.setBasicPromoItemDetail(productLevelId, productId, deptId, false,
				ppgGroupId);
		TestHelper.updatePromoDetails(promoItemDTO1, 4.99, 1, 3.99, 1, PromoTypeLookup.STANDARD.getPromoTypeId(), 0, 0,
				DisplayTypeLookup.NONE.getDisplayTypeId());
		TestHelper.updatePromoItemPredictions(promoItemDTO1, 500, 2500, 1500, 200, 1000, 500, 255);
		promoScenarios.add(promoItemDTO1);

		// Promo scenario 2
		PromoItemDTO promoItemDTO2 = TestHelper.setBasicPromoItemDetail(productLevelId, productId, deptId, false,
				ppgGroupId);
		TestHelper.updatePromoDetails(promoItemDTO2, 4.99, 1, 3.89, 1, PromoTypeLookup.STANDARD.getPromoTypeId(), 0, 0,
				DisplayTypeLookup.NONE.getDisplayTypeId());
		TestHelper.updatePromoItemPredictions(promoItemDTO2, 600, 2400, 1400, 200, 1000, 500, 255);
		promoScenarios.add(promoItemDTO2);

		// Promo scenario 2
		PromoItemDTO promoItemDTO3 = TestHelper.setBasicPromoItemDetail(productLevelId, productId, deptId, false,
				ppgGroupId);
		TestHelper.updatePromoDetails(promoItemDTO2, 4.99, 1, 3.79, 1, PromoTypeLookup.STANDARD.getPromoTypeId(), 0, 0,
				DisplayTypeLookup.NONE.getDisplayTypeId());
		TestHelper.updatePromoItemPredictions(promoItemDTO2, 800, 2600, 1500, 200, 1000, 500, 255);
		promoScenarios.add(promoItemDTO3);

	}*/

	@Test
	public void testCase2() {
		
		
		double regPrice = 3.99;
		double discount = (regPrice * 25) / 100;
		double newSalePrice = regPrice - discount;
		double price1 = PRFormatHelper.round(newSalePrice * 2, 3);
		BigDecimal saleDeci = new BigDecimal(price1);
		BigDecimal roundOff = saleDeci.setScale(2, BigDecimal.ROUND_HALF_UP);
		
		double newSaleMPrice = PRFormatHelper.round(price1, 2);
		MultiplePrice salePrice = new MultiplePrice(2, newSaleMPrice);
	
	}
	
}

