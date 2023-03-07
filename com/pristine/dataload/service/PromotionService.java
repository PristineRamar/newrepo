package com.pristine.dataload.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.pristine.dto.PromoDataStandardDTO;
import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.dto.offermgmt.promotion.PromoBuyRequirement;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
import com.pristine.dto.offermgmt.promotion.PromoOfferDetail;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAd;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdBlock;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdPage;
import com.pristine.dto.riteaid.RAPromoFileDTO;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PromotionService {
	private static Logger logger = Logger.getLogger("PromotionService");

	/**
	 * identifies the promotion type by specific columns and sets it in promotion
	 * definition obj
	 * 
	 * @param promoFileDTO
	 * @param promoDefinition
	 */
	public void identifyAndSetPromotionType(RAPromoFileDTO promoFileDTO, PromoDefinition promoDefinition) {

		int promoTypeId = getPromotioType(promoFileDTO, promoDefinition);
		promoDefinition.setPromoDefnTypeId(promoTypeId);
		promoDefinition.setPromoTypeId(promoTypeId);
	}

	/**
	 * 
	 * @param promoFileDTO
	 * @return promotion Type
	 */
	public int getPromotioType(RAPromoFileDTO promoFileDTO, PromoDefinition promoDefinition) {
		int promoTypeId = Constants.PROMO_TYPE_STANDARD;
		if ((promoFileDTO.getGetQty() == 1 && promoFileDTO.getBuyQty() == 1) && ((promoFileDTO.getSalePrice() == 0)
				|| promoFileDTO.getItemPriceCode().equals("P") && promoFileDTO.getSalePrice() > 0)) {
			// Sale price should be zero or Item price code should be P.
			// P means % off on second item, when you buy 1 item,
			promoTypeId = Constants.PROMO_TYPE_BOGO;
			if (promoFileDTO.getItemPriceCode().equals("P") && promoFileDTO.getSalePrice() > 0) {
				promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_BOGO_X_PCT_OFF);
			}
		} else if (promoFileDTO.getSaleQty() > 1 && promoFileDTO.getSalePrice() > 0
				&& promoFileDTO.getSecondItemPrice() > 0) {
			// When there is a multiple sale price and the second item price is greater than
			// zero,
			// It is a Must buy promotion
			promoTypeId = Constants.PROMO_TYPE_MUST_BUY;
		} else if (promoFileDTO.getGetQty() == 1 && promoFileDTO.getBuyQty() == 1
				&& promoFileDTO.getGroupPromoType().equalsIgnoreCase("CUPN")) {
			// Ignore this kind of promotions.
			// Plenti points.. Not yet comfirmed
			promoTypeId = Constants.PROMO_TYPE_UNKNOWN;
			promoDefinition.setCanBeAdded(false);
		} else if ((promoFileDTO.getGetQty() > 1 || promoFileDTO.getBuyQty() > 1) && ((promoFileDTO.getSalePrice() == 0)
				|| promoFileDTO.getItemPriceCode().equals("P") && promoFileDTO.getSalePrice() > 0)) {
			// If get qty or buy qty is greater than 1, then it is B2G1 kind of promotion
			promoTypeId = Constants.PROMO_TYPE_BXGY_SAME;
			if (promoFileDTO.getItemPriceCode().equals("P") && promoFileDTO.getSalePrice() > 0) {
				promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_BXGY_SAME_X_PCT_OFF);
			}
		} else if (promoFileDTO.getCpnAmount() != 0 && promoFileDTO.getSalePrice() == 0
				&& promoFileDTO.getItemPriceCode().equals("D")) {
			// Since the Price Code is D off and there is no amount in Item Price,
			// The Cpn Amt is the Dollar and it is an in Ad Cpn Data
			// Consider this In Ad CPN, only if there no other promotion going on
			promoTypeId = Constants.PROMO_TYPE_IN_AD_COUPON;
			promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_IN_AD_COUPON);
		} else if (!promoFileDTO.getCpnDesc().equals("") && !promoFileDTO.getThresholdType().equals("")
				&& promoFileDTO.getThresholdValue() > 0) {
			// To understand this overlay, the column Threshold type is D,
			// Threshold Value is $15 and Coupon Description should match Buy should be $15
			// then Get amount $5 should be the amt.
			// Ensure this value is not in 100s , that may mean Plenti points in historic
			// files.
			promoTypeId = Constants.PROMO_TYPE_EBONUS_COUPON;
			promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_OVERLAY);
		} else {
			promoTypeId = Constants.PROMO_TYPE_STANDARD;
			if (promoFileDTO.getItemPriceCode().equals("D")) {
				promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_STANDARD_DOLLAR_OFF);
			} else if (promoFileDTO.getItemPriceCode().equals("P")) {
				promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_STANDARD_PCT_OFF);
			}
		}

		return promoTypeId;
	}

	/**
	 * Calculate and sets the sale price based on item price code
	 * 
	 * @param promoBuyItems
	 * @param promoFileDTO
	 */
	public void setSalePrice(PromoBuyItems promoBuyItems, RAPromoFileDTO promoFileDTO) {
		if (promoFileDTO.getSalePrice() > 0) {
			String priceCode = promoFileDTO.getItemPriceCode();
			// % off from reg price
			if (priceCode.equals("P")) {
				if (promoFileDTO.getSalePrice() > 0) {
					double discount = (double) (promoFileDTO.getSalePrice() * promoFileDTO.getRegPrice()) / 100;
					double salePr = promoFileDTO.getRegPrice() - discount;
					promoBuyItems.setSaleQty(1);
					promoBuyItems.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePr)));
					// BOGO with 50% off on second item's price
					if (((promoFileDTO.getBuyQty() == 1 && promoFileDTO.getGetQty() == 1)
							|| (promoFileDTO.getBuyQty() > 1 || promoFileDTO.getGetQty() > 1)
									&& promoFileDTO.getSalePrice() > 0)) {
						discount = (double) (promoFileDTO.getSalePrice() * promoFileDTO.getRegPrice()) / 100;
						double secondItemPrice = promoFileDTO.getRegPrice() - discount;
						promoBuyItems.setSaleQty(2);
						salePr = promoFileDTO.getRegPrice() + secondItemPrice;
						promoBuyItems.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePr)));
					}
				}
			} else if (priceCode.equals("D")) { // $ off from reg price
				if (promoFileDTO.getRegPrice() > promoFileDTO.getSalePrice() && promoFileDTO.getSalePrice() > 0) {
					double salePr = promoFileDTO.getRegPrice() - promoFileDTO.getSalePrice();
					promoBuyItems.setSaleQty(1);
					promoBuyItems.setSalePrice(salePr);
				}
			} else if (priceCode.equals("F")) { // Sale price is directly given
				promoBuyItems.setSaleQty(promoFileDTO.getSaleQty());
				promoBuyItems.setSalePrice(promoFileDTO.getSalePrice());
			}
		}
	}

	/**
	 * Sets buy requirement and offer details based on coupon
	 * 
	 * @param promotion
	 */
	public void parseCouponDescAndSetAddlDetails(PromoDefinition promotion, PromoBuyRequirement buyReq,
			HashMap<Pattern, Integer> compiledPatterns, Set<String> notProcessedCouponDesc) {

		PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
		if (promotion.getCouponDesc() != null && !promotion.getCouponDesc().equals("")
				&& promotion.getThresholdValue() > 0) {
			String cpnDesc = promotion.getCouponDesc().toUpperCase();
			String matched = null;

			if (promotion.getThresholdType().equals("Q")) {
				buyReq.setMinQtyReqd((int) promotion.getThresholdValue());
			} else if (promotion.getThresholdType().equals("D")) {
				buyReq.setMinAmtReqd(promotion.getThresholdValue());
			} else {
				if (promotion.getThresholdType().equals("")) {
					logger.warn("parseCouponDescAndSetAddlDetails() - Threshold type is not given for -> "
							+ promotion.getUPC());
				} else {
					logger.warn("parseCouponDescAndSetAddlDetails() - Unknown threshold type -> " + promotion.getUPC());
				}
				return;
			}

			for (Map.Entry<Pattern, Integer> patterns : compiledPatterns.entrySet()) {
				if (matched == null) {
					logger.debug("Pattern: " + patterns.getKey().toString());
					logger.debug("Desc: " + cpnDesc + ", Group: " + patterns.getValue());
					matched = getMatchedString(patterns.getKey(), patterns.getValue(), cpnDesc);
				}
			}

			if (matched == null) {
				// logger.warn("parseCouponDescAndSetAddlDetails() - New Pattern found in coupon
				// desc -> " + cpnDesc);
				notProcessedCouponDesc.add(cpnDesc);
				buyReq.setMinQtyReqd(null);
				buyReq.setMinAmtReqd(null);
				return;
			}

			try {

				double getDollar = Double.parseDouble(matched);
				promoOfferDetail.setOfferValue(getDollar);
				promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_COUPON);
				promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);

			} catch (Exception e) {
				logger.error("parseCouponDescAndSetAddlDetails() - Error while parsing coupon - " + cpnDesc, e);
				buyReq.setMinQtyReqd(null);
				buyReq.setMinAmtReqd(null);
				return;
			}
			setPromotionSubtypeForOverlay(promotion);
			buyReq.addOfferDetail(promoOfferDetail);
		}
	}

	/**
	 * 
	 * @param pattern
	 * @param group
	 * @param desc
	 * @return matched string based on pattern
	 */
	private String getMatchedString(Pattern pattern, int group, String desc) {
		Matcher matcher = pattern.matcher(desc);
		String matched = null;
		if (matcher.find()) {
			matched = matcher.group(group).replaceAll("\\$", "");
		}
		return matched;
	}

	/**
	 * Sets promo sub type for overlay based on existing promotion
	 * 
	 * @param promotion
	 */
	private void setPromotionSubtypeForOverlay(PromoDefinition promotion) {
		if (promotion.getPromoSubTypeId() == Constants.PROMO_SUB_TYPE_BOGO_X_PCT_OFF) {
			promotion.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_BOGO_X_PCT_OFF_WITH_OVERLAY);
		} else if (promotion.getPromoSubTypeId() == Constants.PROMO_SUB_TYPE_BXGY_SAME_X_PCT_OFF) {
			promotion.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_BXGY_SAME_X_PCT_OFF_WITH_OVERLAY);
		} else if (promotion.getPromoSubTypeId() == Constants.PROMO_SUB_TYPE_STANDARD_DOLLAR_OFF) {
			promotion.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_STANDARD_DOLLAR_OFF_WITH_OVERLAY);
		} else if (promotion.getPromoSubTypeId() == Constants.PROMO_SUB_TYPE_STANDARD_PCT_OFF) {
			promotion.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_BXGY_SAME_X_PCT_OFF_WITH_OVERLAY);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_BOGO) {
			promotion.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_BOGO_WITH_OVERLAY);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_BXGY_SAME) {
			promotion.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_BXGY_SAME_WITH_OVERLAY);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_MUST_BUY) {
			promotion.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_MUST_BUY_WITH_OVERLAY);
		} else {
			promotion.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_OVERLAY);
		}
	}

	/**
	 * 
	 * @param promoGroups
	 * @param weeklyAd
	 */
	public void setupWeeklyAdAndPromotions(List<PromoDefinition> allPromotions, WeeklyAd weeklyAd) {

		HashMap<String, HashMap<String, List<PromoDefinition>>> pageBlockMap = new HashMap<>();

		for (PromoDefinition promoDefinition : allPromotions) {
			// PromoDefinition promoDefinition = promoEntry.getValue();

			HashMap<String, List<PromoDefinition>> blockMap = null;

			if (pageBlockMap.containsKey(promoDefinition.getAdpage())) {
				blockMap = pageBlockMap.get(promoDefinition.getAdpage());
			} else {
				blockMap = new HashMap<>();
			}

			pageBlockMap.put(promoDefinition.getAdpage(), blockMap);

			if (blockMap.containsKey(promoDefinition.getBlockNum())) {
				List<PromoDefinition> promoList = blockMap.get(promoDefinition.getBlockNum());
				promoList.add(promoDefinition);
				blockMap.put(promoDefinition.getBlockNum(), promoList);
			} else {
				List<PromoDefinition> promoList = new ArrayList<>();
				promoList.add(promoDefinition);
				blockMap.put(promoDefinition.getBlockNum(), promoList);
			}
		}

		logger.info("setupWeeklyAdAndPromotions() - # of pages for Ad " + weeklyAd.getWeekStartDate() + ": "
				+ pageBlockMap.size());
		// Set total pages
		weeklyAd.setTotalPages(pageBlockMap.size());

		for (Map.Entry<String, HashMap<String, List<PromoDefinition>>> pageBlockEntry : pageBlockMap.entrySet()) {
			// Create new page
			WeeklyAdPage weeklyAdPage = new WeeklyAdPage();
			weeklyAdPage.setPageNumber(Integer.parseInt(pageBlockEntry.getKey()));
			weeklyAdPage.setTotalBlocks(pageBlockEntry.getValue().size());
			weeklyAdPage.setStatus(3);

			for (Map.Entry<String, List<PromoDefinition>> blockEntry : pageBlockEntry.getValue().entrySet()) {
				// Create new block
				WeeklyAdBlock weeklyAdBlock = new WeeklyAdBlock();

				weeklyAdBlock.setBlockNumber(Integer.parseInt(blockEntry.getKey()));
				weeklyAdBlock.setStatus(3);

				// Set promotions
				ArrayList<PromoDefinition> promotions = new ArrayList<>(blockEntry.getValue());
				weeklyAdBlock.setPromotions(promotions);

				// Attach block to page
				weeklyAdPage.addBlock(weeklyAdBlock);
			}

			// Attach page to weekly Ad
			weeklyAd.addPage(weeklyAdPage);
		}
	}

	public void identifyAndSetPromotionType(PromoDataStandardDTO promoFileDTO, PromoDefinition promoDefinition) {

		int promoTypeId = getPromotionType(promoFileDTO, promoDefinition);
		promoDefinition.setPromoDefnTypeId(promoTypeId);
		promoDefinition.setPromoTypeId(promoTypeId);
	}

	/**
	 * 
	 * @param promoFileDTO
	 * @return promotion Type
	 */
	public int getPromotionType(PromoDataStandardDTO promoFileDTO, PromoDefinition promoDefinition) {
		int promoTypeId = Constants.PROMO_TYPE_STANDARD;

		//Added for RA 
		//to handle BAGB Promotion
		int otherItem = 0;
		String anotherItem = promoFileDTO.getAnotherItem();
		// logger.info("value of another Item " + promoFileDTO.getAnotherItem());
		if (anotherItem != null) {
			if (anotherItem.equals("null") || anotherItem.isEmpty()) {
				otherItem = 0;

			} else {
				otherItem = 1;
			}

		}
	
		// logger.info("getPromotionType-anotherItemPresent value: "+ otherItem);

		// BOGO
		// Sale price should be zero or there should be percentage off/dollar off along
		// with Sale Price.
		// % off or $ off on second item, when you buy 1 item
		if ((promoFileDTO.getGetQty() == 1 && promoFileDTO.getBuyQty() == 1 && otherItem == 0)
				&& ((promoFileDTO.getSalePrice() == 0)
						|| (promoFileDTO.getPctOff() > 0 && promoFileDTO.getSalePrice() > 0)
						|| (promoFileDTO.getDollarOff() > 0 && promoFileDTO.getSalePrice() > 0))) {
			promoTypeId = Constants.PROMO_TYPE_BOGO;
			logger.debug("getPromotionType  for : " + promoFileDTO.getItemName() + "is " + promoTypeId);
			if (promoFileDTO.getPctOff() > 0) {
				promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_BOGO_X_PCT_OFF);
			} else if (promoFileDTO.getDollarOff() > 0 && promoFileDTO.getSalePrice() > 0) {
				promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_BOGO_X_DOLLAR_OFF);
			}
		} // BXGY
			// Sale price should be zero or there should be percentage off/dollar off along
			// with Sale Price.
			// % off or $ off on y items, when you buy x items
		else if ((promoFileDTO.getGetQty() > 1 || promoFileDTO.getBuyQty() > 1) && ((promoFileDTO.getSalePrice() == 0)
				|| promoFileDTO.getPctOff() > 0 || promoFileDTO.getDollarOff() > 0)) {
			promoTypeId = Constants.PROMO_TYPE_BXGY_SAME;

			if (promoFileDTO.getPctOff() > 0) {
				promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_BXGY_SAME_X_PCT_OFF);
			} else if (promoFileDTO.getDollarOff() > 0) {
				promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_BXGY_SAME_X_DOLLAR_OFF);
			}
			logger.debug("getPromotionType  for : " + promoFileDTO.getItemName() + "is " + promoTypeId);
		}
		// MUSTBUY
		// Sale price has to be given along with mustBuy qty/mustBuyPrice and there can
		// be percentage off/dollar off along
		else if ((promoFileDTO.getMustBuyQty() > 0 && promoFileDTO.getMustbuyPrice() > 0)) {

			promoTypeId = Constants.PROMO_TYPE_MUST_BUY;

		} // BMSM minimum qty has to be specified along with BMSM Dollar Off Per Units or
			// BMSM Pct Off per Unit
		else if (promoFileDTO.getBmsmDollaroffperunits() > 0 || promoFileDTO.getBmsmPctoffperunit() > 0
				|| (promoFileDTO.getBmsmsalePrice() > 0 || promoFileDTO.getBmsmsaleQty() > 0)) {
			promoTypeId = Constants.PROMO_TYPE_BMSM;
			if (promoFileDTO.getBmsmPctoffperunit() > 0) {
				promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_BMSM_PCT_OFF);
				logger.debug("subtype for : " + promoFileDTO.getItemName() + "is " + "PROMO_SUB_TYPE_BMSM_PCT_OFF");
			} else if (promoFileDTO.getBmsmDollaroffperunits() > 0) {
				promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_BMSM_DOLLAR_OFF);
				logger.debug("subtype for : " + promoFileDTO.getItemName() + "is " + "PROMO_SUB_TYPE_BMSM_DOLLAR_OFF");
			}
			logger.debug("getPromotionType  for : " + promoFileDTO.getItemName() + "is " + promoTypeId);
		} else if (!Constants.EMPTY.equals(promoFileDTO.getCouponType()) && promoFileDTO.getCouponType() != null
				&& promoFileDTO.getCouponAmt() > 0 && promoFileDTO.getDollarOff() == 0 && promoFileDTO.getPctOff() == 0
				&& promoFileDTO.getSalePrice() == 0) {

			if (Constants.DIGITAL_COUPON.equals(promoFileDTO.getCouponType().toUpperCase())) {
				promoTypeId = Constants.PROMO_TYPE_DIGITAL_COUPON;
			} else if (Constants.AD_COUPON.equals(promoFileDTO.getCouponType().toUpperCase())) {
				promoTypeId = Constants.PROMO_TYPE_IN_AD_COUPON;

			}
		} else if (promoFileDTO.getBuyQty() > 0 && promoFileDTO.getGetQty() > 0 && otherItem == 1) {

			promoTypeId = Constants.PROMO_TYPE_BXGY_DIFF;

		} else {
			promoTypeId = Constants.PROMO_TYPE_STANDARD;
			if (promoFileDTO.getDollarOff() > 0) {
				promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_STANDARD_DOLLAR_OFF);
			} else if (promoFileDTO.getPctOff() > 0) {
				promoDefinition.setPromoSubTypeId(Constants.PROMO_SUB_TYPE_STANDARD_PCT_OFF);
			}
			logger.debug("getPromotionType  for : " + promoFileDTO.getItemName() + "is " + promoTypeId);
		}
		logger.debug("getPromotionType  end : ");
		return promoTypeId;
	}

	/**
	 * Calculate and sets the sale price based on item price code
	 * 
	 * @param promoBuyItems
	 * @param promoFileDTO
	 */
	public void setPromoItemsSalePrice(PromoBuyItems promoBuyItems, PromoDataStandardDTO promoFileDTO) {
		logger.debug("setPromoItemsSalePrice()-start");

		double pctOff = promoFileDTO.getPctOff();
		double dollarOff = promoFileDTO.getDollarOff();
		double dollarOffPerUnit = promoFileDTO.getBmsmDollaroffperunits();
		double pctOffPerUnit = promoFileDTO.getBmsmPctoffperunit();
		double regprice = 0;

		double salePr = 0;
		double discount = 0;

		//Added for RA 
		//to handle BAGB Promotion
		int otherItem = 0;
		String anotherItem = promoFileDTO.getAnotherItem();
		if (anotherItem != null) {
			if (anotherItem.equals("null") || anotherItem.isEmpty()) {
				otherItem = 0;

			} else {
				otherItem = 1;
			}

		}
	
		if (Double.parseDouble(promoFileDTO.getEverdayQty()) == 1)
			regprice = Double.parseDouble(promoFileDTO.getEverydayPrice());
		else
			regprice = Double.parseDouble(promoFileDTO.getEverydayPrice())
					/ Integer.parseInt(promoFileDTO.getEverdayQty());

		if (promoFileDTO.getSalePrice() > 0 && promoFileDTO.getSaleQty() > 0) {
			promoBuyItems.setSaleQty(promoFileDTO.getSaleQty());
			promoBuyItems
					.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(promoFileDTO.getSalePrice())));

			logger.debug("Item  : " + promoFileDTO.getItemName() + "has Saleprice directly given  "
					+ promoBuyItems.getSalePrice() + " and sale quantity" + promoBuyItems.getSaleQty());
		} else if (dollarOff > 0 && promoFileDTO.getSalePrice() == 0) {
			salePr = regprice - dollarOff;
			promoBuyItems.setSaleQty(1);
			promoBuyItems.setSalePrice(salePr);
			logger.debug("Item : " + promoFileDTO.getItemName() + "has Saleprice " + salePr + " and sale quantity"
					+ promoFileDTO.getSaleQty());
		} else if (pctOff > 0 && promoFileDTO.getSalePrice() == 0) {
			// on regPrice and saleprice==0
			discount = (pctOff * regprice) / 100;
			salePr = regprice - discount;
			Double sp = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePr));
			promoBuyItems.setSaleQty(1);
			promoBuyItems.setSalePrice(sp);
			logger.debug("Item : " + promoFileDTO.getItemName() + "has Saleprice " + sp + " and sale quantity "
					+ promoFileDTO.getSaleQty());
		} else if (promoFileDTO.getBmsmsaleQty() > 0 && promoFileDTO.getBmsmsalePrice() > 0) {
			promoBuyItems.setSaleQty(promoFileDTO.getBmsmsaleQty());
			promoBuyItems.setSalePrice(promoFileDTO.getBmsmsalePrice());
		} else if (dollarOffPerUnit > 0) {

			if (promoFileDTO.getSalePrice() == 0 && promoFileDTO.getMinimumQty() > 0) {
				salePr = regprice - dollarOffPerUnit;
				promoBuyItems.setSaleQty(promoFileDTO.getMinimumQty());
				promoBuyItems.setSalePrice(
						Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePr)) * promoFileDTO.getMinimumQty());
			}

			logger.debug("Item1 : " + promoFileDTO.getItemName() + "has Saleprice " + promoBuyItems.getSalePrice()
					+ " and sale quantity" + promoBuyItems.getSaleQty());
		}
		// handle BMSM promotions % off on regPrice along with min quantity specified
		else if (pctOffPerUnit > 0) {
			if (promoFileDTO.getSalePrice() == 0 && promoFileDTO.getMinimumQty() > 0) {
				discount = (pctOffPerUnit * regprice) / 100;
				salePr = regprice - discount;
				promoBuyItems.setSaleQty(promoFileDTO.getMinimumQty());
				promoBuyItems.setSalePrice(salePr * promoFileDTO.getMinimumQty());
				logger.debug("Item2 : " + promoFileDTO.getItemName() + "has Saleprice " + promoBuyItems.getSalePrice()
						+ " and sale quantity" + promoBuyItems.getSaleQty());
			}
		} else if (promoFileDTO.getBuyQty() == 1 && promoFileDTO.getGetQty() == 1 && pctOff == 0 && dollarOff == 0) {
			logger.info("inside");
			int saleQty = 2;
			logger.debug("saleQty is: " + saleQty);
			promoBuyItems.setSaleQty(saleQty);
			promoBuyItems.setSalePrice(regprice);
			logger.info("Item : " + promoFileDTO.getItemName() + "has Saleprice " + promoBuyItems.getSalePrice()
					+ " and sale quantity " + promoBuyItems.getSaleQty());

		} else if ((promoFileDTO.getBuyQty() >= 1 && promoFileDTO.getGetQty() > 1)
				|| (promoFileDTO.getBuyQty() > 1 && promoFileDTO.getGetQty() >= 1)) {

			salePr = regprice * promoFileDTO.getBuyQty();
			int sq = promoFileDTO.getBuyQty() + promoFileDTO.getGetQty();
			promoBuyItems.setSaleQty(sq);
			promoBuyItems.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePr)));
			logger.debug("Item : " + promoFileDTO.getItemName() + "has Saleprice " + salePr + " and sale quantity "
					+ promoFileDTO.getSaleQty());
		} else if (promoFileDTO.getCouponAmt() != 0 && promoFileDTO.getBuyQty() == 0 && promoFileDTO.getGetQty() == 0
				&& pctOff == 0 && dollarOff == 0 && promoFileDTO.getSalePrice() == 0) {
			logger.info(" regprice is: " + regprice + " coupon Discount" + promoFileDTO.getCouponAmt());

			salePr = regprice - promoFileDTO.getCouponAmt();
			int saleQty = 1;
			promoBuyItems.setSaleQty(saleQty);
			promoBuyItems.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePr)));
			logger.debug("Item : " + promoFileDTO.getItemName() + "has Saleprice " + salePr + " and sale quantity "
					+ promoBuyItems.getSaleQty());
		} else if (otherItem == 1 && promoFileDTO.getPctOff() > 0) {
			discount = (promoFileDTO.getPctOff() * regprice) / 100;
			salePr = regprice - discount;
			Double sp = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePr));
			promoBuyItems.setSaleQty(promoFileDTO.getBuyQty());
			promoBuyItems.setSalePrice(sp);
		}

	}

}
