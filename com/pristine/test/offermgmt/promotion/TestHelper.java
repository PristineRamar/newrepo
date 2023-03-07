package com.pristine.test.offermgmt.promotion;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.pristine.dto.fileformatter.gianteagle.GERewardPromotionDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.promotion.BlockDetail;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.service.offermgmt.DisplayTypeLookup;

public class TestHelper {

	public static BlockDetail setBlockDetail(int pageNo, int blockNo, HashMap<Integer, String> deptDetail, double predRev) {
		BlockDetail blockDetail = new BlockDetail();

		blockDetail.setDepartments(deptDetail);
		blockDetail.getSalePricePredictionMetrics().setPredRev(predRev);

		return blockDetail;
	}
	
	public static PromoItemDTO setBasicPromoItemDetail(int productLevelId, int productId, int deptId, boolean isPPGItemSummary, long ppgGroupId) {
		PromoItemDTO promoItemDTO = new PromoItemDTO();
		
		promoItemDTO.setProductKey(new ProductKey(productLevelId, productId));
		promoItemDTO.setDeptId(deptId);
		promoItemDTO.setPPGLevelSummary(isPPGItemSummary);
		promoItemDTO.setPpgGroupId(ppgGroupId);
		
		return promoItemDTO;
	}
	
	public static void updatePromoItemPredictions(PromoItemDTO promoItemDTO, double predMovSales, double predRevSales, double predMarSales, double predMovReg,
			double predRevReg, double predMarReg, int householdCnt) {

		promoItemDTO.setPredMov(predMovSales);
		promoItemDTO.setPredRev(predRevSales);
		promoItemDTO.setPredMar(predMarSales);
		
		promoItemDTO.setPredMovReg(predMovReg);
		promoItemDTO.setPredRevReg(predRevReg);
		promoItemDTO.setPredMarReg(predMarReg);
		
		promoItemDTO.setNoOfHHRecommendedTo(householdCnt);
	}
	
	
	public static void updatePromoDetails(PromoItemDTO promoItemDTO, double regPrice, int regQty, double salePrice,
			int saleQty, int promoTypeId, int adPageNo, int blockNo, int displayTypeId) {
		
		promoItemDTO.setRegPrice(new MultiplePrice(regQty, regPrice));
		
		PRItemSaleInfoDTO saleInfo = new PRItemSaleInfoDTO();
		saleInfo.setSalePrice(new MultiplePrice(saleQty, salePrice));
		saleInfo.setPromoTypeId(promoTypeId);
		promoItemDTO.setSaleInfo(saleInfo);
		
		PRItemAdInfoDTO adInfo = new PRItemAdInfoDTO();
		adInfo.setAdPageNo(adPageNo);
		adInfo.setAdBlockNo(blockNo);
		promoItemDTO.setAdInfo(adInfo);
		
		PRItemDisplayInfoDTO displayInfo = new PRItemDisplayInfoDTO();
		displayInfo.setDisplayTypeLookup(DisplayTypeLookup.get(displayTypeId));
		promoItemDTO.setDisplayInfo(displayInfo);
	}	
	
	
	public static void setGERewardPromotionDetail(int itemCode, int regQty, double price, int locationId, String zoneNo, List<GERewardPromotionDTO> rewardPromoSet){
		GERewardPromotionDTO geRewardPromotionDTO = new GERewardPromotionDTO();
		geRewardPromotionDTO.setItemCode(itemCode);
		geRewardPromotionDTO.setRegularPrice(new MultiplePrice(regQty, price));
		geRewardPromotionDTO.setLocationId(locationId);
		geRewardPromotionDTO.setZoneNo(zoneNo);
		rewardPromoSet.add(geRewardPromotionDTO);
	}
	
	public static void setGERewardPromotionDetail(int itemCode, int regQty, double price, int locationId, String zoneNo,
			String promotTypeCode, List<GERewardPromotionDTO> rewardPromoSet) {
		GERewardPromotionDTO geRewardPromotionDTO = new GERewardPromotionDTO();
		geRewardPromotionDTO.setItemCode(itemCode);
		geRewardPromotionDTO.setRegularPrice(new MultiplePrice(regQty, price));
		geRewardPromotionDTO.setLocationId(locationId);
		geRewardPromotionDTO.setZoneNo(zoneNo);
		geRewardPromotionDTO.setPromoCode(promotTypeCode);
		rewardPromoSet.add(geRewardPromotionDTO);
	}
}
