package com.pristine.dao.offermgmt.promotion;

//import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
//import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.LocationKey;
//import com.pristine.dto.offermgmt.MultiplePrice;
//import com.pristine.dto.offermgmt.PRSizeDTO;
import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.dto.offermgmt.promotion.PromoBuyRequirement;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
//import com.pristine.dto.offermgmt.promotion.PromoDisplay;
import com.pristine.dto.offermgmt.promotion.PromoLocation;
import com.pristine.dto.offermgmt.promotion.PromoOfferDetail;
import com.pristine.dto.offermgmt.promotion.PromoOfferItem;
//import com.pristine.dto.offermgmt.weeklyad.WeeklyAd;
//import com.pristine.dto.offermgmt.weeklyad.WeeklyAdPage;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PromotionDAO {
	static Logger logger = Logger.getLogger("PromotionDAO");
	
	private Connection conn = null;
	
	private static final Integer STATUS = 3;
	private static final String GET_PROMO_DEFN_ID = "SELECT PM_PROMO_DEFINITION_ID_SEQ.NEXTVAL PROMO_DEFN_ID FROM DUAL";
	private static final String GET_CUR_PROMO_DEFN_SEQ_NO = "SELECT PM_PROMO_DEFINITION_ID_SEQ.CURRVAL PROMO_DEFN_ID FROM DUAL";
	private static final String IS_PROMOTION_EXISTS = "SELECT PROMO_DEFINITION_ID FROM PM_PROMO_DEFINITION WHERE PROMO_TYPE_ID = ? AND START_CALENDAR_ID = ? AND END_CALENDAR_ID = ? AND NAME = ?";
	private static final String IS_PROMOTION_EXISTS_V2 = 
			"SELECT PROMO_DEFINITION_ID FROM PM_PROMO_DEFINITION WHERE PROMO_TYPE_ID = ? AND START_CALENDAR_ID = ? " +
			"AND END_CALENDAR_ID = ? AND RET_LIR_ID = ?";
	private static final String INSERT_PROMOTION_LOCATION = "INSERT INTO PM_PROMO_LOCATION (PROMO_DEFINITION_ID, LOCATION_LEVEL_ID, LOCATION_ID) VALUES (?, ?, ?)";
	private static final String SELECT_PROMO_LOCATION = "SELECT PROMO_DEFINITION_ID FROM PM_PROMO_LOCATION WHERE PROMO_DEFINITION_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ?";
	private static final String INSERT_PROMOTION = "INSERT INTO PM_PROMO_DEFINITION (PROMO_DEFINITION_ID, PROMO_TYPE_ID, START_CALENDAR_ID, END_CALENDAR_ID, " +
												   " STATUS_LOOKUP_ID, \"NUMBER\", \"NAME\", CREATED_BY, CREATED, APPROVED_BY, APPROVED, PREDICTION_ID) VALUES " +
												   "(?, ?, ?, ?, ?, ?, ?, ?, TO_DATE(?, 'MM/DD/YYYY'), ?, TO_DATE(?, 'MM/DD/YYYY'),  PM_PREDICTION_ID_SEQ.NEXTVAL)";
	
	private static final String INSERT_PROMOTION_V2 = "INSERT INTO PM_PROMO_DEFINITION (PROMO_DEFINITION_ID, PROMO_TYPE_ID, "
			+ " START_CALENDAR_ID, END_CALENDAR_ID, STATUS_LOOKUP_ID, \"NUMBER\", \"NAME\", CREATED_BY, CREATED, "
			+ " APPROVED_BY, APPROVED, DESCRIPTION, PREDICTION_ID, RET_LIR_ID, SRC_VENDOR_AND_ITEM_ID, PROMO_SUB_TYPE_ID, IS_EDLP_PROMO, PROMO_GROUP_ID  ) VALUES "
			+ "(?, ?, ?, ?, ?, ?, ?, ?, SYSDATE, ?, SYSDATE, ?, PM_PREDICTION_ID_SEQ.NEXTVAL, ?, ?, ?, ?, ?)";
	
	private static final String UPDATE_PROMOTION = "UPDATE PM_PROMO_DEFINITION SET STATUS_LOOKUP_ID = ?, MODIFIED_BY = ?, PROMO_TYPE_ID = ?,MODIFIED = TO_DATE(?, 'MM/DD/YYYY') " +
												   "WHERE PROMO_DEFINITION_ID = ?";
//	private static final String UPDATE_PROMOTION_V2 = "UPDATE PM_PROMO_DEFINITION SET STATUS_LOOKUP_ID = ?, MODIFIED_BY = ?, MODIFIED = SYSDATE,"
//			+ " \"NAME\" = ?, IS_IN_WEEKLY_AD = ?, BATCH_SESSION_ID = ?, ACTIVE_INDICATOR = ? WHERE PROMO_DEFINITION_ID = ?";
	
	private static final String INSERT_PROMO_BUY_ITEMS = 
			"INSERT INTO PM_PROMO_BUY_ITEM (PROMO_DEFINITION_ID, ITEM_CODE, LIST_COST, DEAL_COST, OFF_INVOICE_COST, "
			+ " BILLBACK_COST, SCAN_COST, REG_QTY, REG_PRICE, REG_M_PRICE, SALE_QTY, SALE_PRICE, SALE_M_PRICE, TPR_QTY,"
			+ " TPR_PRICE, TPR_M_PRICE, CASE_PACK, OFF_INVOICE_COST_CASE, BILLBACK_COST_CASE, DISPLAY_TYPE_FLAG, "
			+ " IS_IN_WEEKLY_AD, IS_PROMO_IN_MID_OF_WEEK, START_CALENDAR_ID, END_CALENDAR_ID) "
			+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String UPDATE_DISPLAY_TYPE_ID = "UPDATE PM_PROMO_DEFINITION SET SPECIAL_DISPLAY_TYPE_ID = ? WHERE START_CALENDAR_ID = ? AND END_CALENDAR_ID = ? AND PROMO_NAME = ?";
	private static final String DELETE_PROMOTION_LOCATION = "DELETE FROM PM_PROMO_LOCATION WHERE PROMO_DEFINITION_ID = ?";
	private static final String DELETE_PROMO_BUY_ITEM = "DELETE FROM PM_PROMO_BUY_ITEM WHERE PROMO_DEFINITION_ID = ?";
	private static final String DELETE_PROMO_OFFER_ITEM = "DELETE FROM PM_PROMO_OFFER_ITEM WHERE PROMO_OFFER_DETAIL_ID IN (SELECT PROMO_OFFER_DETAIL_ID FROM PM_PROMO_OFFER_DETAIL WHERE " +
														  "PROMO_BUY_REQUIREMENT_ID IN (SELECT PROMO_BUY_REQUIREMENT_ID FROM PM_PROMO_BUY_REQUIREMENT WHERE PROMO_DEFINITION_ID = ?))";
	private static final String DELETE_PROMO_OFFER_DETAIL = "DELETE FROM PM_PROMO_OFFER_DETAIL WHERE PROMO_BUY_REQUIREMENT_ID IN (SELECT PROMO_BUY_REQUIREMENT_ID FROM PM_PROMO_BUY_REQUIREMENT WHERE PROMO_DEFINITION_ID = ?)";
	private static final String DELETE_PROMO_BUY_REQUIREMENT = "DELETE FROM PM_PROMO_BUY_REQUIREMENT WHERE PROMO_DEFINITION_ID = ?";
	private static final String DELETE_PROMOTION_DEFINITION = "DELETE FROM PM_PROMO_DEFINITION WHERE PROMO_DEFINITION_ID = ?";
	private static final String GET_PROMO_BUY_REQUIREMENT_ID = "SELECT PM_PROMO_BUY_REQ_ID_SEQ.NEXTVAL PROMO_BUY_REQUIREMENT_ID FROM DUAL";
	private static final String GET_PROMO_OFFER_DETAIL_ID = "SELECT PM_PROMO_OFFER_DETAIL_ID_SEQ.NEXTVAL PROMO_OFFER_DETAIL_ID FROM DUAL";
	private static final String INSERT_PROMO_BUY_REQUIREMENT = "INSERT INTO PM_PROMO_BUY_REQUIREMENT (PROMO_BUY_REQUIREMENT_ID, PROMO_DEFINITION_ID, BUY_AND_GET_IS_SAME, BUY_X, MUSTBUY_QTY, MUSTBUY_AMT, " +
															   "MIN_QTY_REQUIRED, MIN_WEIGHT_REQUIRED, MIN_AMT_REQUIRED, MANUFACTURER_COUPON, STORE_COUPON) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String INSERT_PROMO_OFFER_DETAIL = "INSERT INTO PM_PROMO_OFFER_DETAIL (PROMO_OFFER_DETAIL_ID, PROMO_BUY_REQUIREMENT_ID, PROMO_OFFER_TYPE_ID, OFFER_UNIT_TYPE, OFFER_COUNT, " +
															"OFFER_VALUE, OFFER_LIMIT) VALUES (?, ?, ?, ?, ?, ? , ?)";
	private static final String INSERT_PROMO_OFFER_ITEM = "INSERT INTO PM_PROMO_OFFER_ITEM (PROMO_OFFER_DETAIL_ID, ITEM_CODE) VALUES (?, ?)";
	private static final String DELETE_WEEKLY_AD_PROMO = "DELETE FROM PM_WEEKLY_AD_PROMO WHERE PROMO_DEFINITION_ID = ?";
	private static final String DELETE_DISPLAY_PROMO = "DELETE FROM PM_PROMO_DISPLAY WHERE PROMO_DEFINITION_ID = ?";
	
	private static final String GET_PROMO_DETAILS_BY_ITEM_CODE_AD_FILE = 
			"SELECT PROMO_DEFINITION_ID,  START_CALENDAR_ID, END_CALENDAR_ID, START_DATE, END_DATE, PROMO_NO, PROMO_NAME, TOTAL_ITEMS "
			+ " FROM (SELECT PD.PROMO_DEFINITION_ID, PD.START_CALENDAR_ID, PD.END_CALENDAR_ID, RC.START_DATE START_DATE, " 
			+ " RC1.START_DATE END_DATE, PD.\"NUMBER\" PROMO_NO, PD.\"NAME\" PROMO_NAME, "
			+ " COUNT(PBI.ITEM_CODE) TOTAL_ITEMS FROM PM_PROMO_DEFINITION PD JOIN PM_PROMO_BUY_ITEM PBI " 
			+ " ON PD.PROMO_DEFINITION_ID = PBI.PROMO_DEFINITION_ID LEFT JOIN RETAIL_CALENDAR RC ON "
			+ " PD.START_CALENDAR_ID = RC.CALENDAR_ID LEFT JOIN RETAIL_CALENDAR RC1 ON PD.END_CALENDAR_ID = RC1.CALENDAR_ID " 
			+ " WHERE PD.PROMO_DEFINITION_ID IN (SELECT DISTINCT PROMO_DEFINITION_ID "
			+ " FROM PM_PROMO_BUY_ITEM WHERE ITEM_CODE IN (%s)) GROUP BY PD.PROMO_DEFINITION_ID, PD.START_CALENDAR_ID, PD.END_CALENDAR_ID, " 
			+ " PD.\"NUMBER\", PD.\"NAME\", RC.START_DATE , RC1.START_DATE) "
			+ " WHERE START_DATE >= TO_DATE(?, 'MM/DD/YYYY') AND END_DATE <= TO_DATE(?, 'MM/DD/YYYY')";
	
	private static final String GET_PROMO_DETAILS_BY_ITEM_CODE = "SELECT PROMO_DEFINITION_ID, ITEM_CODE FROM PM_PROMO_BUY_ITEM PBI "
			+ " WHERE PROMO_DEFINITION_ID IN ( " + " SELECT DISTINCT PROMO_DEFINITION_ID "
			+ " FROM PM_PROMO_DEFINITION WHERE START_CALENDAR_ID = %START_CALENDAR_ID% AND END_CALENDAR_ID= %END_CALENDAR_ID% ) "
			+ " %PROMO_LOCATIONS%"
			+ " AND ITEM_CODE IN (%s)";
	
	private static final String UPDATE_PROMO = "UPDATE PM_PROMO_DEFINITION SET \"NAME\" = ?, ADJUSTED_UNITS = ? WHERE PROMO_DEFINITION_ID = ?";
//	private static final String ALTER_PROMO_SEQUENCE = "ALTER SEQUENCE PM_PROMO_DEFINITION_ID_SEQ START WITH ";
	
	private static final String GET_MAX_PROMO_DEF_ID = " SELECT MAX(PROMO_DEFINITION_ID) AS PROMO_DEFINITION_ID FROM PM_PROMO_DEFINITION ";
	
	private String minStartDate;
	private String maxEndDate;

	private static final String GET_PROMOTIONS_WITHIN_DATE_RANGE = 
			"SELECT RC.START_DATE START_DATE, RC1.START_DATE END_DATE, PD.PROMO_DEFINITION_ID, PD.START_CALENDAR_ID, "
			+ " PD.END_CALENDAR_ID, PD.RET_LIR_ID, PD.SRC_VENDOR_AND_ITEM_ID, PD.PROMO_TYPE_ID "
			+ " ,BI.SALE_QTY, BI.SALE_PRICE, BI.SALE_M_PRICE, PL.LOCATION_LEVEL_ID, PL.LOCATION_ID "
			+ " FROM PM_PROMO_DEFINITION PD "
			+ " LEFT JOIN PM_PROMO_LOCATION PL ON PL.PROMO_DEFINITION_ID = PD.PROMO_DEFINITION_ID "
			+ " LEFT JOIN PM_PROMO_BUY_ITEM BI ON PD.PROMO_DEFINITION_ID = BI.PROMO_DEFINITION_ID " 
			+ " JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = PD.START_CALENDAR_ID "
			+ " JOIN RETAIL_CALENDAR RC1 ON RC1.CALENDAR_ID = PD.END_CALENDAR_ID "
			+ " WHERE RC.START_DATE >= TO_DATE(?, 'MM/DD/YYYY') AND RC1.START_DATE <= TO_DATE(?, 'MM/DD/YYYY')";
	
	private static final String GET_PROMOTIONS_WITHIN_DATE_RANGE_AND_ITEMS = 
			"SELECT RC.START_DATE START_DATE, RC1.START_DATE END_DATE, PD.PROMO_DEFINITION_ID, PD.START_CALENDAR_ID, "
			+ " PD.END_CALENDAR_ID, PD.RET_LIR_ID, PD.SRC_VENDOR_AND_ITEM_ID, PD.PROMO_TYPE_ID "
			+ " ,BI.SALE_QTY, BI.SALE_PRICE, BI.SALE_M_PRICE, BI.ITEM_CODE, PL.LOCATION_ID, "
			+ " PL.LOCATION_LEVEL_ID FROM PM_PROMO_DEFINITION PD "
			+ " LEFT JOIN PM_PROMO_LOCATION PL ON PL.PROMO_DEFINITION_ID = PD.PROMO_DEFINITION_ID "
			+ " LEFT JOIN PM_PROMO_BUY_ITEM BI ON PD.PROMO_DEFINITION_ID = BI.PROMO_DEFINITION_ID " 
			+ " JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = PD.START_CALENDAR_ID "
			+ " JOIN RETAIL_CALENDAR RC1 ON RC1.CALENDAR_ID = PD.END_CALENDAR_ID "
			+ " WHERE RC.START_DATE >= TO_DATE(?, 'MM/DD/YYYY') AND RC1.START_DATE <= TO_DATE(?, 'MM/DD/YYYY') AND PD.PROMO_TYPE_ID IN (%PROMO_TYPE_IDS%)";
			//+ " AND PL.LOCATION_LEVEL_ID = ? AND PL.LOCATION_ID = ? ";
	
	private static final String GET_PROMO_TYPE_LOOKUP = "SELECT PROMO_TYPE_ID, PROMO_TYPE_NAME FROM PM_PROMO_TYPE_LOOKUP";
	
//	private static final String UPDATE_PROMO_INACTIVE = " UPDATE PM_PROMO_DEFINITION SET ACTIVE_INDICATOR = 'N' WHERE PROMO_DEFINITION_ID IN ("
//			+ " SELECT PD.PROMO_DEFINITION_ID FROM PM_PROMO_DEFINITION PD JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = PD.START_CALENDAR_ID "
//			+ " JOIN RETAIL_CALENDAR RC1 ON RC1.CALENDAR_ID = PD.END_CALENDAR_ID "
//			+ " WHERE RC.START_DATE >= TO_DATE(?, 'MM/DD/YYYY') AND RC1.START_DATE <= TO_DATE(?, 'MM/DD/YYYY') "
//			+ " AND (BATCH_SESSION_ID IS NULL OR BATCH_SESSION_ID <> ?))";
	private static final String UPDATE_AD_PLEX = "UPDATE PM_PROMO_BUY_ITEM SET IS_IN_AD_PLEX = 'Y' "
			+ " WHERE PROMO_DEFINITION_ID = ? AND ITEM_CODE = ?";
				
	
	
	private static final String GET_NO_OF_LIG_OR_NON_LIG = "SELECT BLOCK_ID, SUM(COUNT_PROMO) NO_OF_LIG_OR_NON_LIG FROM ( "
			+ " SELECT WAP.BLOCK_ID, DECODE(PD.SRC_VENDOR_AND_ITEM_ID, NULL, COUNT( DISTINCT PD.RET_LIR_ID), "
			+ " COUNT( DISTINCT PD.SRC_VENDOR_AND_ITEM_ID)) COUNT_PROMO "
			+ " FROM PM_WEEKLY_AD_PROMO WAP LEFT JOIN PM_PROMO_DEFINITION PD ON "
			+ " PD.PROMO_DEFINITION_ID = WAP.PROMO_DEFINITION_ID "
			+ " WHERE PD.START_CALENDAR_ID = (SELECT CALENDAR_ID FROM "
			+ " RETAIL_CALENDAR WHERE START_DATE = TO_DATE(?, '" + Constants.APP_DATE_FORMAT + "') AND ROW_TYPE = 'D') "
			+ " AND PD.END_CALENDAR_ID = (SELECT CALENDAR_ID FROM RETAIL_CALENDAR "
			+ " WHERE START_DATE = TO_DATE(?, '" + Constants.APP_DATE_FORMAT + "') AND ROW_TYPE = 'D') "
			+ " GROUP BY WAP.BLOCK_ID, PD.RET_LIR_ID, PD.SRC_VENDOR_AND_ITEM_ID) "
			+ " GROUP BY BLOCK_ID ";
	
	private static final String UPDATE_NO_OF_LIG_OR_NON_LIG = "UPDATE PM_WEEKLY_AD_BLOCK SET NO_OF_LIG_OR_NON_LIG = ? "
			+ " WHERE BLOCK_ID = ? ";
	
	private static final String GET_PROMO_LOCATIONS = "SELECT PROMO_DEFINITION_ID, LOCATION_LEVEL_ID, LOCATION_ID"
			+ " FROM PM_PROMO_LOCATION WHERE PROMO_DEFINITION_ID IN (%s)";
	
	private static final String IS_PROMOTION_EXISTS_IN_BUY_ITEM = "SELECT DISTINCT(PROMO_DEFINITION_ID) FROM PM_PROMO_BUY_ITEM "
			+ "WHERE PROMO_DEFINITION_ID IN (SELECT PROMO_DEFINITION_ID FROM PM_PROMO_DEFINITION WHERE START_CALENDAR_ID = ? "
			+ "AND END_CALENDAR_ID = ? AND PROMO_TYPE_ID = ?) AND ITEM_CODE IN(%ITEM_CODE_LIST%) ";
	private static final String IS_PROMOTION_EXISTS_IN_OFFER_ITEM = "SELECT DISTINCT(PROMO_DEFINITION_ID) FROM PM_PROMO_DEFINITION "
			+ "WHERE START_CALENDAR_ID = ? AND END_CALENDAR_ID = ? AND PROMO_TYPE_ID = ? AND PROMO_DEFINITION_ID IN( "
			+ "SELECT PROMO_DEFINITION_ID FROM PM_PROMO_BUY_REQUIREMENT "
			+ "WHERE PROMO_BUY_REQUIREMENT_ID IN(SELECT PROMO_BUY_REQUIREMENT_ID FROM PM_PROMO_OFFER_DETAIL "
			+ "WHERE PROMO_OFFER_DETAIL_ID IN(SELECT PROMO_OFFER_DETAIL_ID FROM PM_PROMO_OFFER_ITEM "
			+ "WHERE ITEM_CODE IN (%ITEM_CODE%))))";
	 
	public PromotionDAO(Connection conn){
		this.conn = conn;
	}
	
	/**
	 * Saves promotion information
	 * @param promotion
	 * @throws GeneralException 
	 */
	public void savePromotion(PromoDefinition promotion) throws GeneralException{
		boolean dbStatus = false;
		long promoDefnId = -1;
		List<PromoOfferItem> offerDetailsList = null;
		for(PromoBuyRequirement promoBuyReq:promotion.getPromoBuyRequirement()){
			for (PromoOfferDetail offerDetail : promoBuyReq.getOfferDetail()) {
				offerDetailsList = offerDetail.getOfferItems();
			}
		}
		if(offerDetailsList != null && !offerDetailsList.isEmpty()){
			promoDefnId = getPromoDefnIdUsingOfferItem(offerDetailsList, promotion);
		}
		else if(promotion.getBuyItems()!= null){
			promoDefnId = getPromoDefnIdForSuperCoupon(promotion);
			//logger.info("Promo defintion Id: "+promoDefnId);
		}
		
		if(promoDefnId > 0){
			logger.debug("Deleting Promo Related Tables");
			deletePromoRelTables(promotion, promoDefnId);
			logger.debug("Updating promotion " + promoDefnId);
			promotion.setPromoDefnId(promoDefnId);
			dbStatus = updatePromotion(promotion);
		}else{
			promoDefnId = getPromoDefnId();
			logger.debug("Inserting promotion " + promoDefnId);
			promotion.setPromoDefnId(promoDefnId);
			dbStatus = insertPromotion(promotion);
			
		}
		
		if(dbStatus){
			for(PromoLocation location : promotion.getPromoLocation()){
				location.setPromoDefnId(promoDefnId);
				boolean isLocationExists = checkLocationExists(location);
				if(!isLocationExists)
					insertPromoLocation(location);
			}
			insertPromoBuyItem(promoDefnId, promotion.getBuyItems());
						
			for(PromoBuyRequirement buyReq : promotion.getPromoBuyRequirement()){
				buyReq.setPromoDefnId(promoDefnId);
				savePromoBuyRequirement(buyReq);
			}
		}else{
			logger.error("Insert/Update promotion failed");
			return;
		}
	}
	
	//Get all promotion definition id
	//Separate to be updated and to be inserted list
	//Update all promotions
	//Insert all new promotions

	/**
	 * Saves promotion information
	 * @param promotion
	 * @throws GeneralException 
	 */
//	public void savePromotionV2(List<PromoDefinition> allPromotions, Date processingWeek) throws GeneralException {
//		// boolean dbStatus = false;
//		long promoDefnId = -1;
//		List<PromoDefinition> updatePromotions = new ArrayList<PromoDefinition>();
//		List<PromoDefinition> insertPromotions = new ArrayList<PromoDefinition>();
//		List<PromoDefinition> updateAndInsertPromotions = new ArrayList<PromoDefinition>();
//		long batchSessionId = 0;
//		int insertPromoCount = 0;
//		// get next promo definition seq id
//		long promoDefnIdForInsert = 0;
//
//		Date curDate = new Date();
//		DateFormat formatterMMddyy = new SimpleDateFormat("MMddyyHms");
//		batchSessionId = Long.valueOf(formatterMMddyy.format(curDate));
//		
//		List<PromoDefinition> existingPromoList = getPromotionsByCustomDateRange(allPromotions);
//
//		for (PromoDefinition promotion : allPromotions) {
//			// promoDefnId = getPromoDefnIdV2(promotion);
//			promoDefnId = checkPromotionsWithDB(existingPromoList, promotion);
//			// logger.debug("Getting promo def id is completed");
//			if (promoDefnId > 0) {
//				// Check if promotion is already exists and not falling
//				// under processing week, ignore updating in this case so that
//				// promotion history will be maintained.
//				if (!promotion.getPromoEndDate().before(processingWeek)) {
//					updatePromotionId(promotion, promoDefnId);
//					updatePromotions.add(promotion);
//					updateAndInsertPromotions.add(promotion);
//				}
//
//			} else {
//				// get sequence for the first time alone
//				if (insertPromoCount == 0) {
//					promoDefnIdForInsert = getMaxPromoDefId();
//				}
//				promoDefnIdForInsert = promoDefnIdForInsert + 1;
//				updatePromotionId(promotion, promoDefnIdForInsert);
//				insertPromotions.add(promotion);
//				updateAndInsertPromotions.add(promotion);
//				insertPromoCount = insertPromoCount + 1;
//			}
//		}
//		logger.debug("Getting promo def id is Completed");
//		logger.debug("No of promotions to be updated:" + updatePromotions.size());
//		logger.debug("No of promotions to be inserted:" + insertPromotions.size());
//
//		logger.debug("Batch Mode starts");
//		int recCounts = 0;
//		// Delete existing promotions
//		deletePromoRelTablesBatch(updateAndInsertPromotions);
//
//		// Update Promotions
//		recCounts = updatePromotionBatch(updatePromotions, batchSessionId);
//		// logger.debug("No of Promotions Updated:" + recCounts);
//		// Insert promotions
//
//		recCounts = insertPromotionBatch(insertPromotions, batchSessionId);
//		logger.debug("No of Promotions Inserted:" + recCounts);
//
//		// logger.debug("Inserting locations is started");
//		recCounts = insertPromoLocationBatch(updateAndInsertPromotions);
//		logger.debug("No of Locations Inserted:" + recCounts);
//		// logger.debug("Inserting locations is completed");
//
//		// logger.debug("Inserting buy items is started");
//		recCounts = insertPromoBuyItemBatch(updateAndInsertPromotions);
//		logger.debug("No of Buy Item Inserted:" + recCounts);
//
//		// logger.debug("Inserting buy items is completed");
//		logger.debug("Batch Mode ends");
//		// logger.debug("Inserting buy requirement is started");
//		for (PromoDefinition promotion : updateAndInsertPromotions) {
//			for (PromoBuyRequirement buyReq : promotion.getPromoBuyRequirement()) {
//				savePromoBuyRequirement(buyReq);
//			}
//		}
//		//Make promotion as in-active which are not present in current feed, but was there from previous feed
//		updatePromoAsInActive(batchSessionId);
//		logger.debug("Batch Mode ends");
//	}
	
	public void savePromotionV2(List<PromoDefinition> allPromotions, Date processingWeek, boolean isDeltaMode) throws GeneralException {
		// boolean dbStatus = false;
		List<PromoDefinition> insertPromotions = new ArrayList<PromoDefinition>();
		HashSet<Long> distinctExistingPromoId = new HashSet<Long>();
		List<Long> distinctPromoId = new ArrayList<Long>();
		int insertPromoCount = 0;
		// get next promo definition seq id
		long promoDefnIdForInsert = 0;
		boolean takeIdFromSeq = false;
		
		List<PromoDefinition> filteredByLocation =new ArrayList<PromoDefinition>();
		
		String DELETE = PropertyManager.getProperty("DELETE", "TRUE");
	
		logger.info("DELETE FLAG:" + DELETE);
		
		if(DELETE.equalsIgnoreCase("TRUE"))
		{
		//Get all existing promotions for all the processing week 
		logger.info("Getting Existing promotions details is Started...");
		List<PromoDefinition> existingPromoList = null;
		
		if (isDeltaMode) {
			existingPromoList = new ArrayList<>();
			List<PromoDefinition> existingPromoListByItems = getPromotionsByCustomDateAndItems(allPromotions);
			logger.info(
					"Filtering existing promotions by items. Existing promo size: " + existingPromoListByItems.size());
			for (PromoDefinition existingPromotion : existingPromoListByItems) {
				for (PromoDefinition promotion : allPromotions) {
					if (promotion.getStartCalId() == existingPromotion.getStartCalId()
							&& promotion.getEndCalId() == existingPromotion.getEndCalId()) {
						for (PromoBuyItems existingPromoBuyItem : existingPromotion.getBuyItems()) {
							for (PromoBuyItems promoBuyItem : promotion.getBuyItems()) {
								if (existingPromoBuyItem.getItemCode() == promoBuyItem.getItemCode()) {
									existingPromoList.add(existingPromotion);
									break;
								}
							}
						}
					}
				}
			}

			logger.info("Filtered existing promotions by items. Promotions to be deleted: " + existingPromoList.size());
		} else {
			existingPromoList = getPromotionsByCustomDateRange(allPromotions);
		}
		
		logger.info("Getting Existing promotions details is Completed...");
		
		
		 filteredByLocation = new ArrayList<>();
		boolean deletePromoByLocation = Boolean
				.parseBoolean(PropertyManager.getProperty("DELETE_PROMO_BY_LOCATION", "FALSE"));

		if (deletePromoByLocation) {
			Set<LocationKey> distinctLocations = getDistinctLocation(allPromotions);
			for (PromoDefinition promoDef : existingPromoList) {
				LocationKey locationKey = new LocationKey(promoDef.getLocationLevelId(), promoDef.getLocationId());
				if (!distinctExistingPromoId.contains(promoDef.getPromoDefnId())
						&& distinctLocations.contains(locationKey)) {
					distinctExistingPromoId.add(promoDef.getPromoDefnId());
					distinctPromoId.add(promoDef.getPromoDefnId());
					filteredByLocation.add(promoDef);
				}
			}
		} else {
			for (PromoDefinition promoDef : existingPromoList) {
				if (!distinctExistingPromoId.contains(promoDef.getPromoDefnId())) {
					distinctExistingPromoId.add(promoDef.getPromoDefnId());
					distinctPromoId.add(promoDef.getPromoDefnId());
				}
			}
			
			filteredByLocation = existingPromoList;
		}
		
		}
		//Insert promotions, make use of promotion definition id
		long maxPromoDefId = getMaxPromoDefId();
		for (PromoDefinition promotion : allPromotions) {
			if(distinctPromoId.size() > insertPromoCount && distinctPromoId.get(insertPromoCount) != null && !takeIdFromSeq) {
				promoDefnIdForInsert = distinctPromoId.get(insertPromoCount);
			} else {
				takeIdFromSeq = true;
				maxPromoDefId = maxPromoDefId + 1;
				promoDefnIdForInsert = maxPromoDefId;
			}
			updatePromotionId(promotion, promoDefnIdForInsert);
			insertPromotions.add(promotion);
			insertPromoCount = insertPromoCount + 1;
		}
		
		logger.debug("No of promotions to be inserted:" + insertPromotions.size());

		logger.debug("Batch Mode starts");
		int recCounts = 0;
		
		
		//delete all existing promotions
		if(filteredByLocation.size() > 0){
			logger.info("Deletion of PromotionBatch is Started...Size: " + filteredByLocation.size());
			deletePromoRelTablesBatch(filteredByLocation);	
			logger.info("Deletion of PromotionBatch is Completed.");
		}
		
		logger.info("Insertion of PromotionBatch is Started...");
		recCounts = insertPromotionBatch(insertPromotions);
		logger.info("Insertion of PromotionBatch is Completed...");
		logger.info("No of Promotions Inserted:" + recCounts);

		// logger.debug("Inserting locations is started");
		recCounts = insertPromoLocationBatch(insertPromotions);
		logger.info("No of Locations Inserted:" + recCounts);
		// logger.debug("Inserting locations is completed");

		// logger.debug("Inserting buy items is started");
		recCounts = insertPromoBuyItemBatch(insertPromotions);
		logger.info("No of Buy Item Inserted:" + recCounts);

		// logger.debug("Inserting buy items is completed");
		logger.debug("Batch Mode ends");
		// logger.debug("Inserting buy requirement is started");
		for (PromoDefinition promotion : insertPromotions) {
			for (PromoBuyRequirement buyReq : promotion.getPromoBuyRequirement()) {
				savePromoBuyRequirement(buyReq);
			}
		}
		logger.debug("Batch Mode ends");
		
//		if (takeIdFromSeq) {
//			alterSequenceNumber(maxPromoDefId);
//		}
	}
	
	/**
	 * 
	 * @param allPromotions
	 * @return distinct locations
	 */
	private Set<LocationKey> getDistinctLocation(List<PromoDefinition> allPromotions){
		Set<LocationKey> distinctLocations = new HashSet<>();
		for(PromoDefinition promo: allPromotions){
			for(PromoLocation pl: promo.getPromoLocation()){
				LocationKey locationKey = new LocationKey(pl.getLocationLevelId(), pl.getLocationId());
				distinctLocations.add(locationKey);
			}
		}
		
		return distinctLocations;
	}

	private void updatePromotionId(PromoDefinition promotion, long promoDefnId){
		promotion.setPromoDefnId(promoDefnId);
		for(PromoLocation location : promotion.getPromoLocation()){
			location.setPromoDefnId(promoDefnId);
		}
		for(PromoBuyRequirement buyReq : promotion.getPromoBuyRequirement()){
			buyReq.setPromoDefnId(promoDefnId);
		}
	}
	
	

	/**
	 * Returns promo defn id
	 * @return
	 * @throws GeneralException 
	 */
	private long getPromoDefnId() throws GeneralException{
		long promoDefnId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(GET_PROMO_DEFN_ID);
			rs = stmt.executeQuery();
			if(rs.next()){
				promoDefnId = rs.getInt("PROMO_DEFN_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving promo defn id - " + exception);
			throw new GeneralException("Exception in getAuthorizedItemsOfZoneAndStore() " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return promoDefnId;
	}
	
	@SuppressWarnings("unused")
	private long getCurrentPromoDefnSeqNo(){
		long promoDefnId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(GET_CUR_PROMO_DEFN_SEQ_NO);
			rs = stmt.executeQuery();
			if(rs.next()){
				promoDefnId = rs.getInt("PROMO_DEFN_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving promo defn id - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return promoDefnId;
	}
	
	/**
	 * Returns promo defn id
	 * @return
	 */
	private long getPromoDefnId(PromoDefinition promotion){
		long promoDefnId = -1;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(IS_PROMOTION_EXISTS);
			int counter = 0;
			stmt.setInt(++counter, promotion.getPromoTypeId());
			stmt.setInt(++counter, promotion.getStartCalId());
			stmt.setInt(++counter, promotion.getEndCalId());
			stmt.setString(++counter, promotion.getPromoName());
			rs = stmt.executeQuery();
			if(rs.next()){
				promoDefnId = rs.getInt("PROMO_DEFINITION_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving promo defn id - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return promoDefnId;
	}
		
	/**
	 * Returns promo defn id
	 * @return
	 */
	@SuppressWarnings("unused")
	private long getPromoDefnIdV2(PromoDefinition promotion){
		long promoDefnId = -1;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(IS_PROMOTION_EXISTS_V2);
			int counter = 0;
			stmt.setInt(++counter, promotion.getPromoTypeId());
			stmt.setInt(++counter, promotion.getStartCalId());
			stmt.setInt(++counter, promotion.getEndCalId());
			stmt.setInt(++counter, promotion.getRetLirId());
			rs = stmt.executeQuery();
			if(rs.next()){
				promoDefnId = rs.getInt("PROMO_DEFINITION_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving promo defn id - " + exception);
			
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return promoDefnId;
	}
	
	/**
	 * Inserts into PM_PROMO_DEFINITION
	 * @param promotion
	 * @return
	 * @throws GeneralException 
	 */
	private boolean insertPromotion(PromoDefinition promotion) throws GeneralException{
		boolean status = false;
		PreparedStatement stmt = null;
		try{	
			stmt = conn.prepareStatement(INSERT_PROMOTION);
			int counter = 0;
			stmt.setLong(++counter, promotion.getPromoDefnId());
			stmt.setInt(++counter, promotion.getPromoDefnTypeId());
			stmt.setInt(++counter, promotion.getStartCalId());
			stmt.setInt(++counter, promotion.getEndCalId());
			stmt.setInt(++counter, STATUS);
			stmt.setString(++counter, promotion.getPromoNumber());
			stmt.setString(++counter, promotion.getPromoName());
			stmt.setString(++counter, promotion.getCreatedBy());
			stmt.setString(++counter, promotion.getWeekStartDate());
			stmt.setString(++counter, promotion.getApprovedBy());
			stmt.setString(++counter, promotion.getWeekStartDate());
			//Updated on 09/28/16 by Dinesh..
//			stmt.setString(++counter, promotion.getAddtlDetails());
			int count = stmt.executeUpdate();
			
			if(count > 0)
				status = true;
		}catch(SQLException exception){
			logger.error("Error when inserting promotion - " + exception);
			throw new GeneralException("Error in insertPromotion", exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
		
		return status;
	}
	
	private int insertPromotionBatch(List<PromoDefinition> promotions) throws GeneralException {
		PreparedStatement stmt = null;
		int[] count = null;
		int insertCount = 0;
		try {
			if (promotions.size() > 0) {
				stmt = conn.prepareStatement(INSERT_PROMOTION_V2);
				for (PromoDefinition promotion : promotions) {
					int counter = 0;
					stmt.setLong(++counter, promotion.getPromoDefnId());
					stmt.setInt(++counter, promotion.getPromoTypeId());
					stmt.setInt(++counter, promotion.getStartCalId());
					stmt.setInt(++counter, promotion.getEndCalId());
					if(promotion.getStatus() == 0){
						stmt.setInt(++counter, STATUS);
					}else{
						stmt.setInt(++counter, promotion.getStatus());
					}
					
					stmt.setString(++counter, promotion.getPromoNumber());
					stmt.setString(++counter, promotion.getPromoName());
					stmt.setString(++counter, promotion.getCreatedBy());
					stmt.setString(++counter, promotion.getApprovedBy());
					stmt.setString(++counter, promotion.getAddtlDetails());
					stmt.setInt(++counter, promotion.getRetLirId());
					stmt.setString(++counter, promotion.getRetailerItemCode());
					stmt.setInt(++counter, promotion.getPromoSubTypeId());
					stmt.setString(++counter, promotion.getEdlpPromoFlag());
					stmt.setString(++counter, promotion.getPromoGroup());
//					stmt.setString(++counter, promotion.getIsinAd());
//					stmt.setLong(++counter, batchSessionId);
					//stmt.setString(++counter, "Y");
					stmt.addBatch();
					}
					count = stmt.executeBatch();
				}
		} catch (SQLException e) {
			logger.error("Error when inserting promotion - " + e);
			throw new GeneralException("Error in insertPromotionBatch", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		if(count != null)
			insertCount = count.length;
		return insertCount;
	}
	
	/**
	 * Inserts into PM_PROMO_DEFINITION
	 * @param promotion
	 * @return
	 */
	@SuppressWarnings("unused")
	private boolean insertPromotionV2(PromoDefinition promotion){
		boolean status = false;
		PreparedStatement stmt = null;
		
		try{	
			stmt = conn.prepareStatement(INSERT_PROMOTION_V2);
			int counter = 0;
			stmt.setLong(++counter, promotion.getPromoDefnId());
			stmt.setInt(++counter, promotion.getPromoTypeId());
			stmt.setInt(++counter, promotion.getStartCalId());
			stmt.setInt(++counter, promotion.getEndCalId());
			stmt.setInt(++counter, STATUS);
			stmt.setString(++counter, promotion.getPromoNumber());
			stmt.setString(++counter, promotion.getPromoName());
			stmt.setString(++counter, promotion.getCreatedBy());
			stmt.setString(++counter, promotion.getWeekStartDate());
			stmt.setString(++counter, promotion.getApprovedBy());
			stmt.setString(++counter, promotion.getWeekStartDate());
			stmt.setString(++counter, promotion.getAddtlDetails());
			stmt.setInt(++counter, promotion.getRetLirId());
			int count = stmt.executeUpdate();
			
			if(count > 0)
				status = true;
		}catch(SQLException exception){
			logger.error("Error when inserting promotion - " + exception);

		}finally{
			PristineDBUtil.close(stmt);
		}
		
		return status;
	}

	
	

	/**
	 * Updates PM_PROMO_DEFINITION
	 * @param promotion
	 * @return
	 */
	private boolean updatePromotion(PromoDefinition promotion){
		boolean status = false;
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(UPDATE_PROMOTION);
			int counter = 0;
			stmt.setInt(++counter, STATUS);
			stmt.setString(++counter, promotion.getModifiedBy());
			stmt.setInt(++counter, promotion.getPromoDefnTypeId());
			stmt.setString(++counter, promotion.getWeekStartDate());
			stmt.setLong(++counter, promotion.getPromoDefnId());
			
			int count = stmt.executeUpdate();
			
			if(count > 0)
				status = true;
		}catch(SQLException exception){
			logger.error("Error when updating promotion - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
		
		return status;
	}
	
	
	/**
	 * Updates PM_PROMO_DEFINITION
	 * @param promotion
	 * @return
	 * @throws GeneralException 
	 */
//	private boolean updatePromotionV2(PromoDefinition promotion){
//		boolean status = false;
//		PreparedStatement stmt = null;
//		
//		try{
//			stmt = conn.prepareStatement(UPDATE_PROMOTION_V2);
//			int counter = 0;
//			stmt.setInt(++counter, STATUS);
//			stmt.setString(++counter, promotion.getModifiedBy());
//			stmt.setLong(++counter, promotion.getPromoDefnId());
//			stmt.setString(++counter, promotion.getPromoName());
//			int count = stmt.executeUpdate();
//			
//			if(count > 0)
//				status = true;
//		}catch(SQLException exception){
//			logger.error("Error when updating promotion - " + exception);
//		}finally{
//			PristineDBUtil.close(stmt);
//		}
//		
//		return status;
//	}
	
//	private int updatePromotionBatch(List<PromoDefinition> promotions, long batchSessionId) throws GeneralException {
//		PreparedStatement stmt = null;
//		int[] count = null;
//		int updateCount = 0;
//		try {
//			if (promotions.size() > 0) {
//				stmt = conn.prepareStatement(UPDATE_PROMOTION_V2);
//				for (PromoDefinition promotion : promotions) {
//					int counter = 0;
//					stmt.setInt(++counter, STATUS);
//					stmt.setString(++counter, promotion.getModifiedBy());
//					stmt.setString(++counter, promotion.getPromoName());
//					stmt.setString(++counter, promotion.getIsinAd());
//					stmt.setLong(++counter, batchSessionId);
//					stmt.setString(++counter, "Y");
//
//					stmt.setLong(++counter, promotion.getPromoDefnId());
//					stmt.addBatch();
//				}
//				count = stmt.executeBatch();
//			}
//		} catch (SQLException e) {
//			logger.error("Error when updating promotion - " + e);
//			throw new GeneralException("Error in updatePromotionBatch", e);
//		} finally {
//			PristineDBUtil.close(stmt);
//		}
//		if (count != null)
//			updateCount = count.length;
//		return updateCount;
//	}

	private void deletePromoRelTables(PromoDefinition promotion, long promoDefnId) {
		deletePromoLocation(promoDefnId);
		deletePromoBuyItem(promoDefnId);
		deletePromoOfferItem(promoDefnId);
		deletePromoOfferDetail(promoDefnId);
		deletePromoBuyRequirement(promoDefnId);
	}
	
	/**
	 * Deletes from PM_PROMO_LOCATION table
	 * @param promoDefnId
	 * @return
	 */
	private void deletePromoLocation(long promoDefnId){
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(DELETE_PROMOTION_LOCATION);
			int counter = 0;
			stmt.setLong(++counter, promoDefnId);
			stmt.executeUpdate();
		}catch(SQLException exception){
			logger.error("Error when deleting promotion location - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	/**
	 * Deletes from PM_PROMO_BUY_ITEM table
	 * @param promoDefnId
	 * @return
	 */
	private void deletePromoBuyItem(long promoDefnId){
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(DELETE_PROMO_BUY_ITEM);
			int counter = 0;
			stmt.setLong(++counter, promoDefnId);
			stmt.executeUpdate();
		}catch(SQLException exception){
			logger.error("Error when deleting promo buy item - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	/**
	 * Deletes from PM_PROMO_OFFER_ITEM table
	 * @param promoDefnId
	 * @return
	 */
	private void deletePromoOfferItem(long promoDefnId){
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(DELETE_PROMO_OFFER_ITEM);
			int counter = 0;
			stmt.setLong(++counter, promoDefnId);
			stmt.executeUpdate();
		}catch(SQLException exception){
			logger.error("Error when deleting promo offer item - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	/**
	 * Deletes from PM_PROMO_OFFER_DETAIL table
	 * @param promoDefnId
	 * @return
	 */
	private void deletePromoOfferDetail(long promoDefnId){
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(DELETE_PROMO_OFFER_DETAIL);
			int counter = 0;
			stmt.setLong(++counter, promoDefnId);
			stmt.executeUpdate();
		}catch(SQLException exception){
			logger.error("Error when deleting promo offer detail - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	/**
	 * Deletes from PM_PROMO_BUY_REQUIREMENT table
	 * @param promoDefnId
	 * @return
	 */
	private void deletePromoBuyRequirement(long promoDefnId){
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(DELETE_PROMO_BUY_REQUIREMENT);
			int counter = 0;
			stmt.setLong(++counter, promoDefnId);
			stmt.executeUpdate();
		}catch(SQLException exception){
			logger.error("Error when deleting promo buy requirement - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	/**
	 * Inserts into PM_PROMO_LOCATION
	 * @param location
	 * @throws GeneralException
	 */
	private int insertPromoLocationBatch(List<PromoDefinition> promotions) throws GeneralException {
		PreparedStatement stmt = null;
		HashMap<PromoLocation, Integer> distinctLocations = new HashMap<PromoLocation, Integer>();
		int[] count = null;
		int insertCount = 0;
		try {
			if (promotions.size() > 0) {
				stmt = conn.prepareStatement(INSERT_PROMOTION_LOCATION);
				for (PromoDefinition promotion : promotions) {
					for (PromoLocation promoLocation : promotion.getPromoLocation()) {
						if (distinctLocations.get(promoLocation) == null) {
							int counter = 0;
							stmt.setLong(++counter, promoLocation.getPromoDefnId());
							stmt.setInt(++counter, promoLocation.getLocationLevelId());
							stmt.setInt(++counter, promoLocation.getLocationId());
							stmt.addBatch();
							distinctLocations.put(promoLocation, 0);
						}
					}
				}
				count = stmt.executeBatch();
			}
		} catch (SQLException e) {
			logger.error("Error when inserting promo locations - " + e);
			throw new GeneralException("Error while processing records", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		if(count != null)
			insertCount = count.length;
		return insertCount;
	}
	
	private void insertPromoLocation(PromoLocation location){
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(INSERT_PROMOTION_LOCATION);
			int counter = 0;
			stmt.setLong(++counter, location.getPromoDefnId());
			stmt.setInt(++counter, location.getLocationLevelId());
			stmt.setInt(++counter, location.getLocationId());
			
			stmt.executeUpdate();
		}catch(SQLException exception){
			logger.info("Promo def id - " + location.getPromoDefnId());
			logger.info("Location level id - " + location.getLocationLevelId());
			logger.info("Location id - " + location.getLocationId());
			logger.error("Error when inserting promotion location- " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	/**
	 * Inserts into PM_PROMO_LOCATION
	 * @param location
	 * @throws GeneralException
	 */
	private boolean checkLocationExists(PromoLocation location){
		PreparedStatement stmt = null;
		ResultSet resultSet = null;
		boolean locationExists = false;
		try{
			stmt = conn.prepareStatement(SELECT_PROMO_LOCATION);
			int counter = 0;
			stmt.setLong(++counter, location.getPromoDefnId());
			stmt.setInt(++counter, location.getLocationLevelId());
			stmt.setInt(++counter, location.getLocationId());
			
			resultSet = stmt.executeQuery();
			if(resultSet.next()){
				long promoDefId = resultSet.getLong("PROMO_DEFINITION_ID");
				if(promoDefId > 0)
					locationExists = true;
			}
		}catch(SQLException exception){
			logger.info("Promo def id - " + location.getPromoDefnId());
			logger.info("Location level id - " + location.getLocationLevelId());
			logger.info("Location id - " + location.getLocationId());
			logger.error("Error when inserting promotion location- " + exception);
		}finally{
			PristineDBUtil.close(stmt);
			PristineDBUtil.close(resultSet);
		}
		return locationExists;
	}
	
	
	/**
	 * Insert into PM_PROMO_BUY_ITEMS
	 * @param promoBuyItem
	 * @return
	 * @throws GeneralException 
	 */
	private int insertPromoBuyItemBatch(List<PromoDefinition> promotions) throws GeneralException {
		PreparedStatement stmt = null;
		int[] count = null;
		int insertCount = 0;
		
		try {
			if (promotions.size() > 0) {
				stmt = conn.prepareStatement(INSERT_PROMO_BUY_ITEMS);
				for (PromoDefinition promotion : promotions) {
					for (PromoBuyItems promoBuyItem : promotion.getBuyItems()) {
						promoBuyItem.setPromoDefnId(promotion.getPromoDefnId());
//					
//						if (promoBuyItem.getListCost() >= 999.99 || promoBuyItem.getDealCost() >= 999.99 || promoBuyItem.getRegQty() >= 999
//								|| promoBuyItem.getRegPrice() >= 999.99 || promoBuyItem.getRegMPrice() >= 999.99 || promoBuyItem.getSaleQty() >= 999
//								|| promoBuyItem.getSalePrice() >= 999.99 || promoBuyItem.getSaleMPrice() >= 999.99) {
//							logger.debug(promoBuyItem.toString());
//						}
								
						int counter = 0;
						stmt.setLong(++counter, promoBuyItem.getPromoDefnId());
						stmt.setInt(++counter, promoBuyItem.getItemCode());
						stmt.setDouble(++counter, promoBuyItem.getListCost());
						stmt.setDouble(++counter, promoBuyItem.getDealCost());
						stmt.setDouble(++counter, promoBuyItem.getOffInvoiceCost());
						stmt.setDouble(++counter, promoBuyItem.getBillbackCost());
						stmt.setDouble(++counter, promoBuyItem.getScanCost());
						stmt.setDouble(++counter, promoBuyItem.getRegQty());
						stmt.setDouble(++counter, promoBuyItem.getRegPrice());
						stmt.setDouble(++counter, promoBuyItem.getRegMPrice());
						stmt.setDouble(++counter, promoBuyItem.getSaleQty());
						stmt.setDouble(++counter, promoBuyItem.getSalePrice());
						stmt.setDouble(++counter, promoBuyItem.getSaleMPrice());
						stmt.setDouble(++counter, promoBuyItem.getTprQty());
						stmt.setDouble(++counter, promoBuyItem.getTprPrice());
						stmt.setDouble(++counter, promoBuyItem.getTprMPrice());
						stmt.setInt(++counter, promoBuyItem.getCasePack());
						stmt.setDouble(++counter, promoBuyItem.getOffInvoiceCostCase());
						stmt.setDouble(++counter, promoBuyItem.getBillbackCostCase());
						stmt.setString(++counter, promoBuyItem.getDisplayTypeFlag());
						stmt.setString(++counter, promoBuyItem.getIsInAd());
						stmt.setString(++counter, promoBuyItem.isPromoInMidOfWeek() ? String.valueOf(Constants.YES) : String.valueOf(Constants.NO));
						stmt.setInt(++counter, promoBuyItem.getActualStartCalId());
						stmt.setInt(++counter, promoBuyItem.getActualEndCalId());
						stmt.addBatch();
					}
				}
				count = stmt.executeBatch();
			}
		} catch (SQLException e) {
			logger.error("Error when inserting promo buy item - " + e);
			throw new GeneralException("Error in insertPromoBuyItemBatch()", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		if(count != null)
			insertCount = count.length;
		return insertCount;
	}
	
	private int insertPromoBuyItem(long promoDefnId, List<PromoBuyItems> promoBuyItems){
		PreparedStatement stmt = null;
		int[] count = null;
		int insertCount = 0;
		
		try{
			stmt = conn.prepareStatement(INSERT_PROMO_BUY_ITEMS);
			for(PromoBuyItems promoBuyItem : promoBuyItems){
				promoBuyItem.setPromoDefnId(promoDefnId);
				int counter = 0;
				stmt.setLong(++counter, promoBuyItem.getPromoDefnId());
				stmt.setInt(++counter, promoBuyItem.getItemCode());
				stmt.setDouble(++counter, promoBuyItem.getListCost());
				stmt.setDouble(++counter, promoBuyItem.getDealCost());
				stmt.setDouble(++counter, promoBuyItem.getOffInvoiceCost());
				stmt.setDouble(++counter, promoBuyItem.getBillbackCost());
				stmt.setDouble(++counter, promoBuyItem.getScanCost());
				stmt.setDouble(++counter, promoBuyItem.getRegQty());
				stmt.setDouble(++counter, promoBuyItem.getRegPrice());
				stmt.setDouble(++counter, promoBuyItem.getRegMPrice());
				stmt.setDouble(++counter, promoBuyItem.getSaleQty());
				stmt.setDouble(++counter, promoBuyItem.getSalePrice());
				stmt.setDouble(++counter, promoBuyItem.getSaleMPrice());
				stmt.setDouble(++counter, promoBuyItem.getTprQty());
				stmt.setDouble(++counter, promoBuyItem.getTprPrice());
				stmt.setDouble(++counter, promoBuyItem.getTprMPrice());
				stmt.setInt(++counter, promoBuyItem.getCasePack());
				stmt.setDouble(++counter, promoBuyItem.getOffInvoiceCostCase());
				stmt.setDouble(++counter, promoBuyItem.getBillbackCostCase());
				stmt.setString(++counter, promoBuyItem.getDisplayTypeFlag());
				stmt.setString(++counter, promoBuyItem.getIsInAd());
				stmt.setString(++counter, "N");
				stmt.setLong(++counter, promoBuyItem.getActualStartCalId());
				stmt.setLong(++counter, promoBuyItem.getActualEndCalId());
				stmt.addBatch();
			}

			count = stmt.executeBatch();

		}catch(SQLException exception){
			logger.error("Error when inserting promo buy item - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
		if(count != null)
			insertCount = count.length;
		return insertCount;
	}
		
	public void updateDisplayType(HashMap<String, Integer> displayTypeMap, int calendarId){
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(UPDATE_DISPLAY_TYPE_ID);
			for(Map.Entry<String, Integer> entry : displayTypeMap.entrySet()){
				int counter = 0;
				stmt.setInt(++counter, entry.getValue());
				stmt.setInt(++counter, calendarId);
				stmt.setInt(++counter, calendarId);
				stmt.setString(++counter, entry.getKey());
				
				int count = stmt.executeUpdate();
				if(count <= 0){
					logger.warn("Promo - " + entry.getKey() + " not found for calendar - " + calendarId);
				}
			}
			
		}catch(SQLException exception){
			logger.error("Error when updating display type - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	/**
	 * Saves Promo Buy Requirement
	 * @param buyReq
	 * @throws GeneralException 
	 */
	private void savePromoBuyRequirement(PromoBuyRequirement buyReq) throws GeneralException {
		long promoBuyReqId = getPromoBuyRequirementId();
		buyReq.setPromoBuyReqId(promoBuyReqId);
		insertPromoBuyRequirement(buyReq);

		for (PromoOfferDetail offerDetail : buyReq.getOfferDetail()) {
			offerDetail.setPromoBuyReqId(promoBuyReqId);
			savePromoOfferDetail(offerDetail);
		}
	}
	
	/**
	 * Returns promo buy requirement id
	 * @return
	 */
	private long getPromoBuyRequirementId(){
		long promoBuyReqId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(GET_PROMO_BUY_REQUIREMENT_ID);
			rs = stmt.executeQuery();
			if(rs.next()){
				promoBuyReqId = rs.getInt("PROMO_BUY_REQUIREMENT_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving promo buy requirement id - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return promoBuyReqId;
	}
	
	/**
	 * Inserts into PM_PROMO_BUY_REQUIREMENT
	 * @param buyReq
	 * @return
	 * @throws GeneralException 
	 */
	private void insertPromoBuyRequirement(PromoBuyRequirement buyReq) throws GeneralException{
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(INSERT_PROMO_BUY_REQUIREMENT);
			int counter = 0;
			stmt.setLong(++counter, buyReq.getPromoBuyReqId());
			stmt.setLong(++counter, buyReq.getPromoDefnId());
			stmt.setString(++counter, buyReq.getBuyAndGetIsSame());
			if(buyReq.getBuyX() != null)
				stmt.setInt(++counter, buyReq.getBuyX());
			else
				stmt.setNull(++counter, Types.INTEGER);
			if(buyReq.getMustBuyQty() != null)
				stmt.setInt(++counter, buyReq.getMustBuyQty());
			else
				stmt.setNull(++counter, Types.INTEGER);
			if(buyReq.getMustBuyAmt() != null)
				stmt.setDouble(++counter, buyReq.getMustBuyAmt());
			else
				stmt.setNull(++counter, Types.DOUBLE);
			if(buyReq.getMinQtyReqd() != null)
				stmt.setInt(++counter, buyReq.getMinQtyReqd());
			else
				stmt.setNull(++counter, Types.INTEGER);
			if(buyReq.getMinWeightReqd() != null)
				stmt.setDouble(++counter, buyReq.getMinWeightReqd());
			else
				stmt.setNull(++counter, Types.DOUBLE);
			if(buyReq.getMinAmtReqd() != null)
				stmt.setDouble(++counter, buyReq.getMinAmtReqd());
			else
				stmt.setNull(++counter, Types.DOUBLE);
			if(buyReq.getManfCoupon() != null)
				stmt.setDouble(++counter, buyReq.getManfCoupon());
			else
				stmt.setNull(++counter, Types.DOUBLE);
			if(buyReq.getStoreCoupon() != null)
				stmt.setDouble(++counter, buyReq.getStoreCoupon());
			else
				stmt.setNull(++counter, Types.DOUBLE);
			stmt.executeUpdate();
			
		}catch(SQLException e){
			logger.error("Error when inserting promo buy requirement - " + e);
			throw new GeneralException("Error in insertPromoBuyRequirement()", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	/**
	 * Saves Promo Offer Detail
	 * @param buyReq
	 * @throws GeneralException 
	 */
	private void savePromoOfferDetail(PromoOfferDetail offerDetail) throws GeneralException {
		long promoOfferDetailId = getPromoOfferDetailId();
		offerDetail.setPromoOfferDetailId(promoOfferDetailId);
		insertPromoOfferDetail(offerDetail);
		insertPromoOfferItem(promoOfferDetailId, offerDetail.getOfferItems());		 
	}

	/**
	 * Returns promo offer detail id
	 * @return
	 */
	private long getPromoOfferDetailId(){
		long promoOfferDetailId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(GET_PROMO_OFFER_DETAIL_ID);
			rs = stmt.executeQuery();
			if(rs.next()){
				promoOfferDetailId = rs.getInt("PROMO_OFFER_DETAIL_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving promo offer detail id - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return promoOfferDetailId;
	}
	
	/**
	 * Inserts into PM_PROMO_OFFER_DETAIL
	 * @param offerDetail
	 * @throws GeneralException 
	 */
	public void insertPromoOfferDetail(PromoOfferDetail offerDetail) throws GeneralException{
		PreparedStatement stmt = null;
		try{
			stmt = conn.prepareStatement(INSERT_PROMO_OFFER_DETAIL);
			int counter = 0;
			stmt.setLong(++counter, offerDetail.getPromoOfferDetailId());
			stmt.setLong(++counter, offerDetail.getPromoBuyReqId());
			stmt.setInt(++counter, offerDetail.getPromoOfferTypeId());
			stmt.setString(++counter, offerDetail.getOfferUnitType());
			if(offerDetail.getOfferUnitCount() != null)
				stmt.setInt(++counter, offerDetail.getOfferUnitCount());
			else
				stmt.setNull(++counter, Types.INTEGER);
			if(offerDetail.getOfferValue() != null)
				stmt.setDouble(++counter, offerDetail.getOfferValue());
			else
				stmt.setNull(++counter, Types.DOUBLE);
			if(offerDetail.getOfferLimit() != null)
				stmt.setDouble(++counter, offerDetail.getOfferLimit());
			else
				stmt.setNull(++counter, Types.DOUBLE);
			
			
			logger.debug("Offer detail: " + offerDetail.getPromoOfferTypeId() + ", "
					 + ", " + offerDetail.getOfferUnitType()
					 + ", " + offerDetail.getOfferUnitCount()
					 + ", " + offerDetail.getOfferValue()
					 + ", " + offerDetail.getOfferLimit());
			
			stmt.executeUpdate();
			
		}catch(SQLException e){
			logger.error("Error when inserting promo offer detail - " + e);
			throw new GeneralException("Error in insertPromoOfferDetail()", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	/**
	 * Inserts into PM_PROMO_OFFER_ITEMS
	 * @param promoOfferDetailId
	 * @param offerItems
	 * @throws GeneralException 
	 */
	private void insertPromoOfferItem(long promoOfferDetailId, ArrayList<PromoOfferItem> offerItems) throws GeneralException {
		PreparedStatement stmt = null;
		try{
			stmt = conn.prepareStatement(INSERT_PROMO_OFFER_ITEM);
			for(PromoOfferItem offerItem : offerItems){
				offerItem.setPromoOfferDetailId(promoOfferDetailId);
				int counter = 0;
				stmt.setLong(++counter, offerItem.getPromoOfferDetailId());
				stmt.setLong(++counter, offerItem.getItemCode());
				stmt.addBatch();
			}
			stmt.executeBatch();
		}catch(SQLException e){
			logger.error("Error when inserting promo offer items - " + e);
			throw new GeneralException("Error in insertPromoOfferItem()", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	//This function is kept to load weekly ad from ad financial files like (09_26_15_TOPS adjusted.xlsm)
	public List<PromoDefinition> getPromotionsByItemsForAdFile(Connection conn, List<String> itemCodes, String weekStartDate, String weekEndDate){
		List<PromoDefinition> promotionList = new ArrayList<PromoDefinition>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			Object[] values = itemCodes.toArray();
			statement = conn.prepareStatement(String.format(GET_PROMO_DETAILS_BY_ITEM_CODE_AD_FILE, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			int count = values.length;
			statement.setString(++count, weekStartDate);
			statement.setString(++count, weekEndDate);
	     	resultSet = statement.executeQuery();
	     	while(resultSet.next()){
	     		PromoDefinition promotion = new PromoDefinition();
	     		promotion.setPromoDefnId(resultSet.getLong("PROMO_DEFINITION_ID"));
	     		promotion.setStartCalId(resultSet.getInt("START_CALENDAR_ID"));
	     		promotion.setEndCalId(resultSet.getInt("END_CALENDAR_ID"));
	     		promotion.setPromoName(resultSet.getString("PROMO_NAME"));
	     		promotion.setPromoNumber(resultSet.getString("PROMO_NO"));
	     		promotion.setTotalItems(resultSet.getInt("TOTAL_ITEMS"));
	     		promotionList.add(promotion);
	     	}
		}
		catch(SQLException SqlE){
			logger.error("getPromotionsByItems() - Error while retreiving promotions - " + SqlE.toString()); 
		}
		finally{
			PristineDBUtil.close(statement);
			PristineDBUtil.close(resultSet);
		}
		return promotionList;
	}
	
	/**
	 * 
	 * @param conn
	 * @param itemCodes
	 * @param weekStartDate
	 * @param weekEndDate
	 * @return List of promotions for given list of items
	 * @throws GeneralException 
	 */
	public List<PromoDefinition> getPromotionsByItems(Connection conn, Set<String> itemCodes,
			int startCalendarId, int endCalendarId, int locationLevelId, int locationId, int chainId) throws GeneralException{
		List<PromoDefinition> promotionList = new ArrayList<PromoDefinition>();
		 List<String> itemCodeList = new ArrayList<>();
		try{
		int limitCount = 0;
			for(String itemCode: itemCodes){
				itemCodeList.add(itemCode);
				limitCount++;
				if(limitCount > 0 && (limitCount % Constants.BATCH_UPDATE_COUNT == 0)){
					Object[] values = itemCodeList.toArray();
					retreivePromitions(promotionList, startCalendarId, endCalendarId, locationLevelId, locationId, chainId, itemCodeList, values);
					itemCodeList.clear();
				}
			}
			if(itemCodeList.size() > 0){
				Object[] values = itemCodeList.toArray();
				retreivePromitions(promotionList, startCalendarId, endCalendarId, locationLevelId, locationId, chainId, itemCodeList, values);
				itemCodeList.clear();
			}
		} catch (GeneralException e) {
			logger.error("Error -- getPromotionsByItems() - ", e);
			throw new GeneralException("Error in getPromotionsByItems()", e);
		}
		return promotionList;
	}
	
	/**
	 * Retrieves promotions for given items
	 * @param promotionList
	 * @param weekStartDate
	 * @param weekEndDate
	 * @param values
	 * @throws GeneralException
	 */
	private void retreivePromitions(List<PromoDefinition> promotionList, int startCalendarId, int endCalendarId, int locationLevelId, int locationId,
			int chainId, List<String> itemCodes, Object... values) throws GeneralException{
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			String query = new String(GET_PROMO_DETAILS_BY_ITEM_CODE);
			query = query.replaceAll("%START_CALENDAR_ID%", String.valueOf(startCalendarId));
			query = query.replaceAll("%END_CALENDAR_ID%", String.valueOf(endCalendarId));
			
			//NU::9th Sep 2016, item promoted for GU also comes inside TOPS WITHOUT GU weekly ad,
			//So location is added to take item promoted only for corresponding location
			if(locationLevelId > 0) {
				String locCondition = "AND PROMO_DEFINITION_ID IN (SELECT DISTINCT(PROMO_DEFINITION_ID) FROM PM_PROMO_LOCATION PL "
						+ " WHERE PL.PROMO_DEFINITION_ID IN (SELECT DISTINCT(PROMO_DEFINITION_ID) FROM PM_PROMO_DEFINITION PD "
						+ " WHERE START_CALENDAR_ID = " + startCalendarId +
						" AND END_CALENDAR_ID =" + endCalendarId + ")"; 
				locCondition = locCondition + getPromoLocationsForStoreListQuery(chainId, locationId, itemCodes);
				locCondition = locCondition + ")";
				query = query.replaceAll("%PROMO_LOCATIONS%", locCondition);
			} else {
				query = query.replaceAll("%PROMO_LOCATIONS%", "");
			}
			
			
			statement = conn.prepareStatement(String.format(query, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
//			int count = values.length;
//			statement.setInt(++count, startCalendarId);
//			statement.setInt(++count, endCalendarId);
			statement.setFetchSize(200000);
			logger.debug("GET_PROMO_DETAILS_BY_ITEM_CODE:" + query + ",startCalendarId:" + startCalendarId +
					",endCalendarId:" + endCalendarId);
	     	resultSet = statement.executeQuery();
	     	logger.debug("statement is executed");
	     	while(resultSet.next()){
	     		PromoDefinition promotion = new PromoDefinition();
	     		promotion.setPromoDefnId(resultSet.getLong("PROMO_DEFINITION_ID"));
//	     		promotion.setStartCalId(resultSet.getInt("START_CALENDAR_ID"));
//	     		promotion.setEndCalId(resultSet.getInt("END_CALENDAR_ID"));
	     		promotion.setStartCalId(startCalendarId);
	     		promotion.setEndCalId(endCalendarId);
//	     		promotion.setPromoName(resultSet.getString("PROMO_NAME"));
//	     		promotion.setPromoNumber(resultSet.getString("PROMO_NO"));
	     		//promotion.setTotalItems(resultSet.getInt("TOTAL_ITEMS"));
	     		promotion.setItemCode(resultSet.getInt("ITEM_CODE"));
	     		promotionList.add(promotion);
	     	}
		}
		catch(SQLException SqlE){
			throw new GeneralException("retreivePromitions() - Error while retreiving promotions ", SqlE); 
		}
		finally{
			PristineDBUtil.close(statement);
			PristineDBUtil.close(resultSet);
		}
	}
	
	public List<PromoLocation> getPromotionLocation(long promoDefId) {
		String GET_PROMO_LOCATION = "SELECT LOCATION_LEVEL_ID, LOCATION_ID FROM PM_PROMO_LOCATION "
				+ "WHERE PROMO_DEFINITION_ID  = " + promoDefId;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<PromoLocation> promoLocations = new ArrayList<PromoLocation>();
		try {
			stmt = conn.prepareStatement(GET_PROMO_LOCATION);
			rs = stmt.executeQuery();
			while (rs.next()) {
				PromoLocation promoLocation = new PromoLocation();
				promoLocation.setLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
				promoLocation.setLocationId(rs.getInt("LOCATION_ID"));
				promoLocations.add(promoLocation);
			}
		} catch (SQLException exception) {
			logger.error("Error when retrieving promo buy requirement id - " + exception);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return promoLocations;
	}
	
	public int updatePromo(Connection conn, List<PromoDefinition> updateList) throws GeneralException{
		int updateCount = 0;
		PreparedStatement statement = null;
		try{
			int itemsInBatch = 0;
			statement = conn.prepareStatement(UPDATE_PROMO);
			for(PromoDefinition promotion: updateList){
				int counter = 0;
				statement.setString(++counter, promotion.getPromoName());
				statement.setLong(++counter, promotion.getAdjustedUnits());
				statement.setLong(++counter, promotion.getPromoDefnId());
				statement.addBatch();
				itemsInBatch++;
				if(itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		statement.clearBatch();
	        		itemsInBatch = 0;
	        		updateCount += PristineDBUtil.getUpdateCount(count);
	        	}

		    }
        	if(itemsInBatch > 0){
        		int[] count = statement.executeBatch();
        		statement.clearBatch();
        		updateCount +=  PristineDBUtil.getUpdateCount(count);
        	}
			
			
		}
		catch(SQLException SqlE){
			logger.error("updatePromoName() - Error while updating promotions - " + SqlE.toString()); 
			throw new GeneralException("updatePromoName() - Error while updating promotions", SqlE);
		}
		finally{
			PristineDBUtil.close(statement);
		}
		
		
		return updateCount;
		
	}
	private void deletePromoRelTablesBatch(List<PromoDefinition> allPromotions) throws GeneralException {
		logger.info("Deleting locations...");
		deletePromoLocationBatch(allPromotions);
		logger.info("Deleting buy items...");
		deletePromoBuyItemBatch(allPromotions);
		logger.info("Deleting offer item...");
		deletePromoOfferItemBatch(allPromotions);
		logger.info("Deleting offer details...");
		deletePromoOfferDetailBatch(allPromotions);
		logger.info("Deleting buy requirement...");
		deletePromoBuyRequirementBatch(allPromotions);
		logger.info("Deleting weekly Ad promo...");
		deleteWeeklyAdPromoBatch(allPromotions);
		logger.info("Deleting display...");
		deletePromoDisplayBatch(allPromotions);
		logger.info("Deleting promo definition...");
		deletePromoDefinitionBatch(allPromotions);
	}
	
	/**
	 * Bulk Deletes from PM_PROMO_LOCATION table
	 * @param promoDefnId
	 * @return
	 * @throws GeneralException 
	 */
	private int deletePromoLocationBatch(List<PromoDefinition> allPromotions) throws GeneralException{
		PreparedStatement stmt = null;
		int[] count = null;
		int deleteCount = 0;
		
		try{
			stmt = conn.prepareStatement(DELETE_PROMOTION_LOCATION);
			for(PromoDefinition promoDefinition: allPromotions){
				int counter = 0;
				stmt.setLong(++counter, promoDefinition.getPromoDefnId());
				stmt.addBatch();
			}
			count = stmt.executeBatch();
		}catch(SQLException e){
			logger.error("Error when deleting promotion location - " + e);
			throw new GeneralException("Error in deletePromoLocationBatch()", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
		if(count != null)
			deleteCount = count.length;
		
		return deleteCount;
	}
	
	/**
	 * Bulk Deletes from PM_PROMO_BUY_ITEM table
	 * @param promoDefnId
	 * @return
	 * @throws GeneralException 
	 */
	private int deletePromoBuyItemBatch(List<PromoDefinition> allPromotions) throws GeneralException{
		PreparedStatement stmt = null;
		int[] count = null;
		int deleteCount = 0;
		try{
			stmt = conn.prepareStatement(DELETE_PROMO_BUY_ITEM);
			for(PromoDefinition promotion: allPromotions){
				int counter = 0;
				stmt.setLong(++counter, promotion.getPromoDefnId());
				stmt.addBatch();
			}
			count = stmt.executeBatch();
		}catch(SQLException e){
			logger.error("Error when deleting promo buy item - " + e);
			throw new GeneralException("Error in deletePromoBuyItemBatch()", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
		if(count != null)
			deleteCount = count.length;
		
		return deleteCount;
	}
	
	/**
	 * Bulk Deletes from PM_PROMO_OFFER_ITEM table
	 * @param promoDefnId
	 * @return
	 * @throws GeneralException 
	 */
	private int deletePromoOfferItemBatch(List<PromoDefinition> allPromotions) throws GeneralException{
		PreparedStatement stmt = null;
		int[] count = null;
		int deleteCount = 0;
		try{
			stmt = conn.prepareStatement(DELETE_PROMO_OFFER_ITEM);
			for(PromoDefinition promotion: allPromotions){
				int counter = 0;
				stmt.setLong(++counter, promotion.getPromoDefnId());
				stmt.addBatch();
			}
			count = stmt.executeBatch();
		}catch(SQLException e){
			logger.error("Error when deleting promo offer item - " + e);
			throw new GeneralException("Error in deletePromoOfferItemBatch()", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
		if(count != null)
			deleteCount = count.length;
		
		return deleteCount;
	}
	
	/**
	 * Bulk Deletes from PM_PROMO_OFFER_DETAIL table
	 * @param promoDefnId
	 * @return
	 * @throws GeneralException 
	 */
	private int deletePromoOfferDetailBatch(List<PromoDefinition> allPromotions) throws GeneralException{
		PreparedStatement stmt = null;
		int[] count = null;
		int deleteCount = 0;
		try{
			stmt = conn.prepareStatement(DELETE_PROMO_OFFER_DETAIL);
			for(PromoDefinition promotion: allPromotions){
				int counter = 0;
				stmt.setLong(++counter, promotion.getPromoDefnId());
				stmt.addBatch();
			}
			count = stmt.executeBatch();
		}catch(SQLException e){
			logger.error("Error when deleting promo offer detail - " + e);
			throw new GeneralException("Error in deletePromoOfferDetailBatch()", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
		if(count != null)
			deleteCount = count.length;
		
		return deleteCount;
	}
	
	/**
	 * Bulk Deletes from PM_PROMO_BUY_REQUIREMENT table
	 * @param promoDefnId
	 * @return
	 * @throws GeneralException 
	 */
	private int deletePromoBuyRequirementBatch(List<PromoDefinition> allPromotions) throws GeneralException{
		PreparedStatement stmt = null;
		int[] count = null;
		int deleteCount = 0;
		try{
			stmt = conn.prepareStatement(DELETE_PROMO_BUY_REQUIREMENT);
			for(PromoDefinition promotion: allPromotions){
				int counter = 0;
				stmt.setLong(++counter, promotion.getPromoDefnId());
				stmt.addBatch();
			}
			count = stmt.executeBatch();
		}catch(SQLException e){
			logger.error("Error when deleting promo buy requirement - " + e);
			throw new GeneralException("Error in deletePromoBuyRequirementBatch()", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
		if(count != null)
			deleteCount = count.length;
		
		return deleteCount;
	}
	
	/**
	 * Bulk Deletes from PM_WEEKLY_AD_PROMO table
	 * @param promoDefnId
	 * @return
	 * @throws GeneralException 
	 */
	private int deleteWeeklyAdPromoBatch(List<PromoDefinition> allPromotions) throws GeneralException{
		PreparedStatement stmt = null;
		int[] count = null;
		int deleteCount = 0;
		try{
			stmt = conn.prepareStatement(DELETE_WEEKLY_AD_PROMO);
			for(PromoDefinition promotion: allPromotions){
				int counter = 0;
				stmt.setLong(++counter, promotion.getPromoDefnId());
				stmt.addBatch();
			}
			count = stmt.executeBatch();
		}catch(SQLException e){
			logger.error("Error when deleting weekly ad promo - " + e);
			throw new GeneralException("Error in PM_WEEKLY_AD_PROMO()", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
		if(count != null)
			deleteCount = count.length;
		
		return deleteCount;
	}
	
	private int deletePromoDisplayBatch(List<PromoDefinition> allPromotions) throws GeneralException{
		PreparedStatement stmt = null;
		int[] count = null;
		int deleteCount = 0;
		try{
			stmt = conn.prepareStatement(DELETE_DISPLAY_PROMO);
			for(PromoDefinition promotion: allPromotions){
				int counter = 0;
				stmt.setLong(++counter, promotion.getPromoDefnId());
				stmt.addBatch();
			}
			count = stmt.executeBatch();
		}catch(SQLException e){
			logger.error("Error in deletePromoDisplayBatch() - " + e);
			throw new GeneralException("Error in deletePromoDisplayBatch()", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
		if(count != null)
			deleteCount = count.length;
		
		return deleteCount;
	}
	
	private int deletePromoDefinitionBatch(List<PromoDefinition> allPromotions) throws GeneralException {
		PreparedStatement stmt = null;
		int[] count = null;
		int deleteCount = 0;

		try {
			stmt = conn.prepareStatement(DELETE_PROMOTION_DEFINITION);
			for (PromoDefinition promoDefinition : allPromotions) {
				int counter = 0;
				stmt.setLong(++counter, promoDefinition.getPromoDefnId());
				stmt.addBatch();
			}
			count = stmt.executeBatch();
		} catch (SQLException e) {
			logger.error("Error in deletePromoDefinitionBatch() - " + e);
			throw new GeneralException("Error in deletePromoDefinitionBatch()", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		if (count != null)
			deleteCount = count.length;

		return deleteCount;
	}
	
	public void alterSequenceNumber(long promoDefId) throws GeneralException {
		PreparedStatement statement = null;
		StringBuffer sql = new StringBuffer();

		try {
			sql.append("ALTER SEQUENCE PM_PROMO_DEFINITION_ID_SEQ RESTART START WITH ").append(promoDefId);
			PristineDBUtil.execute(conn, sql, "Altering Sequence PM_PROMO_DEFINITION_ID_SEQ");
		} catch (Exception e) {
			logger.error("Exception alterSequenceNumber()  - " + e.toString());
			throw new GeneralException("Error in alterSequenceNumber()", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	private long getMaxPromoDefId() throws GeneralException {
		long promoDefnId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_MAX_PROMO_DEF_ID);
			rs = stmt.executeQuery();
			if (rs.next()) {
				promoDefnId = rs.getLong("PROMO_DEFINITION_ID");
			}
		} catch (SQLException e) {
			logger.error("Error when retrieving promo defn id - " + e);
			throw new GeneralException("Error in getMaxPromoDefId()", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return promoDefnId;
	}
	
	private List<PromoDefinition> getPromotionsByCustomDateRange(
			List<PromoDefinition> allPromotions) throws GeneralException {
		setMinAndMaxDateRangeFromPromotions(allPromotions);
		List<PromoDefinition> promoList = new ArrayList<PromoDefinition>();

		if (minStartDate != null && maxEndDate != null) {
			PreparedStatement statement = null;
			ResultSet resultSet = null;
			try {
				// PD.PROMO_DEFINITION_ID, PD.RET_LIR_ID,
				// PD.SRC_VENDOR_AND_ITEM_ID, PD.PROMO_TYPE_ID,
				// START_CALENDAR_ID, END_CALENDAR_ID
				statement = conn
						.prepareStatement(GET_PROMOTIONS_WITHIN_DATE_RANGE);
				int counter = 0;
				statement.setString(++counter, minStartDate);
				statement.setString(++counter, maxEndDate);
				//logger.debug("SELECT RC.START_DATE START_DATE, RC1.START_DATE END_DATE, PD.PROMO_DEFINITION_ID, PD.START_CALENDAR_ID, PD.END_CALENDAR_ID, "
				//+ "PD.RET_LIR_ID, PD.SRC_VENDOR_AND_ITEM_ID, PD.PROMO_TYPE_ID FROM PM_PROMO_DEFINITION PD JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = PD.START_CALENDAR_ID "
				//+ "JOIN RETAIL_CALENDAR RC1 ON RC1.CALENDAR_ID = PD.END_CALENDAR_ID WHERE RC.START_DATE >= TO_DATE(" + minStartDate + " , 'MM/DD/YYYY') AND RC1.START_DATE <= TO_DATE(" + maxEndDate + ", 'MM/DD/YYYY')");
				resultSet = statement.executeQuery();
				while (resultSet.next()) {
					PromoDefinition promotion = new PromoDefinition();
					PromoBuyItems promoBuyItem = new PromoBuyItems();
					promotion.setPromoDefnId(resultSet
							.getLong("PROMO_DEFINITION_ID"));
					promotion.setStartCalId(resultSet
							.getInt("START_CALENDAR_ID"));
					promotion.setEndCalId(resultSet.getInt("END_CALENDAR_ID"));
					promotion.setRetLirId(resultSet.getInt("RET_LIR_ID"));
					promotion.setRetailerItemCode(resultSet
							.getString("SRC_VENDOR_AND_ITEM_ID"));
					promotion.setPromoTypeId(resultSet.getInt("PROMO_TYPE_ID"));
					promoBuyItem.setSaleQty(resultSet.getInt("SALE_QTY"));
					promoBuyItem.setSalePrice(resultSet.getDouble("SALE_PRICE"));
					promoBuyItem.setSaleMPrice(resultSet.getDouble("SALE_M_PRICE"));
					promotion.setLocationLevelId(resultSet.getInt("LOCATION_LEVEL_ID"));
					promotion.setLocationId(resultSet.getInt("LOCATION_ID"));
					promotion.addBuyItems(promoBuyItem);
					promoList.add(promotion);
				}
			} catch (SQLException sqlE) {
				logger.error("Error while getting promotions by date range - "
						+ sqlE);
				throw new GeneralException(
						"Error while getting promotions by date range", sqlE);
			} finally {
				PristineDBUtil.close(resultSet);
				PristineDBUtil.close(statement);
			}
		} else
			throw new GeneralException("Unable to proceed without date range");

		return promoList;
	}
	
	private void setMinAndMaxDateRangeFromPromotions(List<PromoDefinition> allPromotions){
		SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		List<Date> startDates = new ArrayList<Date>();
		List<Date> endDates = new ArrayList<Date>();
			for(PromoDefinition promotion : allPromotions){
				startDates.add(promotion.getPromoStartDate());
				endDates.add(promotion.getPromoEndDate());
			}
			
		Date minDate = java.util.Collections.min(startDates);
		Date maxDate = java.util.Collections.max(endDates);
		minStartDate = dateFormat.format(minDate);
		maxEndDate = dateFormat.format(maxDate);
	}
	
//	private long checkPromotionsWithDB(List<PromoDefinition> existingPromotions, PromoDefinition newPromotion) {
//		long promoDefId = -1;
//		// logger.debug("Count of existing promotions - " +
//		// existingPromotions.size());
//		for (PromoDefinition existingPromotion : existingPromotions) {
//			if (newPromotion.getStartCalId() == existingPromotion.getStartCalId() && newPromotion.getEndCalId() == existingPromotion.getEndCalId()
//					&& newPromotion.getPromoTypeId() == existingPromotion.getPromoTypeId()) {
//				// logger.debug("Step 1 - Promo type and calendar matched");
//				if ((newPromotion.getRetLirId() != -1 && newPromotion.getRetLirId() == existingPromotion.getRetLirId())
//						|| (newPromotion.getRetailerItemCode() != null
//								&& newPromotion.getRetailerItemCode().equals(existingPromotion.getRetailerItemCode()))) {
//					// Check sale price also
//					PromoBuyItems sampleBuyItemNewPromo = newPromotion.getBuyItems().get(0);
//					PromoBuyItems sampleBuyItemExistingPromo = existingPromotion.getBuyItems().get(0);
//
//					MultiplePrice newPromoSalePrice = PRCommonUtil.getMultiplePrice(sampleBuyItemNewPromo.getSaleQty(), 
//							sampleBuyItemNewPromo.getSalePrice(), sampleBuyItemNewPromo.getSaleMPrice());
//					MultiplePrice existingPromoSalePrice = PRCommonUtil.getMultiplePrice(sampleBuyItemExistingPromo.getSaleQty(), 
//							sampleBuyItemExistingPromo.getSalePrice(), sampleBuyItemExistingPromo.getSaleMPrice());
//
//					if (newPromoSalePrice.equals(existingPromoSalePrice)) {
//						promoDefId = existingPromotion.getPromoDefnId();
//					}
//					// logger.debug("Step 2 - Matched");
//				}
//			}
//		}
//		return promoDefId;
//	}
	
	public HashMap<Integer, String> getPromoTypeLookup() throws GeneralException{
		HashMap<Integer, String> promoTypeLookup = new HashMap<Integer, String>();
		ResultSet resultSet = null;
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(GET_PROMO_TYPE_LOOKUP);
			resultSet = statement.executeQuery();
			while (resultSet.next()){
				promoTypeLookup.put(resultSet.getInt("PROMO_TYPE_ID"), resultSet.getString("PROMO_TYPE_NAME"));
			}
		}
		catch(SQLException sqlE){
			logger.error("Error while getting promo type lookup", sqlE);
			throw new GeneralException("Error while getting promo type lookup", sqlE);
		}
		return promoTypeLookup;
	}
	
//	private boolean updatePromoAsInActive(long batchSessionId) throws GeneralException{
//		boolean status = false;
//		PreparedStatement stmt = null;
//		
//		try{
//			stmt = conn.prepareStatement(UPDATE_PROMO_INACTIVE);
//			int counter = 0;
//			stmt.setString(++counter, minStartDate);
//			stmt.setString(++counter, maxEndDate);
//			stmt.setLong(++counter, batchSessionId);
//			int count = stmt.executeUpdate();
//			
//			if(count > 0)
//				status = true;
//			logger.debug("No of promotions made as inactive: " + count);
//		}catch(SQLException exception){
//			logger.error("Error in updatePromotion() - " + exception);
//			throw new GeneralException("Error in updatePromotion()", exception);
//		}finally{
//			PristineDBUtil.close(stmt);
//		}
//		return status;
//	}
	
	
	public int updateAdplexFlag(Connection conn, List<PromoBuyItems> updateList) throws GeneralException{
		int updateCount = 0;
		PreparedStatement statement = null;
		try{
			int itemsInBatch = 0;
			statement = conn.prepareStatement(UPDATE_AD_PLEX);
			for(PromoBuyItems promoBuyItems: updateList){
				int counter = 0;
				statement.setLong(++counter, promoBuyItems.getPromoDefnId());
				statement.setInt(++counter, promoBuyItems.getItemCode());
				statement.addBatch();
				itemsInBatch++;
				if(itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		statement.clearBatch();
	        		itemsInBatch = 0;
	        		updateCount += PristineDBUtil.getUpdateCount(count);
	        	}

		    }
        	if(itemsInBatch > 0){
        		int[] count = statement.executeBatch();
        		statement.clearBatch();
        		updateCount +=  PristineDBUtil.getUpdateCount(count);
        	}
			
			
		}
		catch(SQLException SqlE){
			logger.error("updateAdplexFlag() - Error while updating promotions - " + SqlE.toString()); 
			throw new GeneralException("updateAdplexFlag() - Error while updating promotions", SqlE);
		}
		finally{
			PristineDBUtil.close(statement);
		}
		return updateCount;
	}
	
	
	/**
	 * @param conn
	 * @param startDate
	 * @param endDate
	 * @return Map of blocks with distinct item count
	 * @throws GeneralException
	 */
	public HashMap<Long, Integer> getNoOfLigAndNonLigAtBlockLevel(Connection conn, 
			String startDate, String endDate ) throws GeneralException{
		HashMap<Long, Integer> blockItemMap = new HashMap<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_NO_OF_LIG_OR_NON_LIG);
			int counter = 0;
			statement.setString(++counter, startDate);
			statement.setString(++counter, endDate);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				long blockId = resultSet.getLong("BLOCK_ID");
				int itemCount = resultSet.getInt("NO_OF_LIG_OR_NON_LIG");
				blockItemMap.put(blockId, itemCount);
			}
		}
		catch(SQLException sqlE){
			logger.error("Error -- getNoOfLigAndNonLigAtBlockLevel()", sqlE);
			throw new GeneralException("Error -- getNoOfLigAndNonLigAtBlockLevel()", sqlE);
		}
		finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return blockItemMap;
	}
	
	/**
	 * Updates item count for each block of given week
	 * @param conn
	 * @param startDate
	 * @param endDate
	 * @param blockItemMap
	 * @return # of rows updated
	 * @throws GeneralException
	 */
	public int updateBlocksWithItemCount(Connection conn, 
			 HashMap<Long, Integer> blockItemMap ) throws GeneralException{
		PreparedStatement statement = null;
		int rowsAffected = 0;
		try{
			statement = conn.prepareStatement(UPDATE_NO_OF_LIG_OR_NON_LIG);
			int batchUpdateCount = 0;
			for(Map.Entry<Long, Integer> entry: blockItemMap.entrySet()){
				int counter = 0;
				batchUpdateCount++;
				statement.setInt(++counter, entry.getValue());
				statement.setLong(++counter, entry.getKey());
				statement.addBatch();
				if(batchUpdateCount % Constants.BATCH_UPDATE_COUNT == 0){
					int[] count = statement.executeBatch();
					statement.clearBatch();
					batchUpdateCount = 0;
					rowsAffected += PristineDBUtil.getUpdateCount(count);
				}
			}
			
			if(batchUpdateCount > 0){
				int[] count = statement.executeBatch();
				statement.clearBatch();
				rowsAffected += PristineDBUtil.getUpdateCount(count);
			}
		}
		catch(SQLException sqlE){
			logger.error("Error -- updateBlocksWithItemCount()", sqlE);
			throw new GeneralException("Error -- updateBlocksWithItemCount()", sqlE);
		}
		finally {
			PristineDBUtil.close(statement);
		} 
		return rowsAffected;
	}
	
	private String getPromoLocationsForStoreListQuery(int chainId, int storeListId, List<String> itemCodes) {
		StringBuffer subQuery = new StringBuffer("");
		
		subQuery.append(" AND (");
		// Chain level
		subQuery.append("(LOCATION_LEVEL_ID=").append(Constants.CHAIN_LEVEL_ID).append(" AND LOCATION_ID=").append(chainId).append(")");

		// Store list level
		subQuery.append("OR (LOCATION_LEVEL_ID=").append(Constants.STORE_LIST_LEVEL_ID).append(" AND LOCATION_ID=").append(storeListId).append(")");

		// Zone
		subQuery.append("OR (LOCATION_LEVEL_ID=").append(Constants.ZONE_LEVEL_ID).append(" AND LOCATION_ID IN(");
		subQuery.append("SELECT DISTINCT(PRICE_ZONE_ID) FROM COMPETITOR_STORE WHERE COMP_STR_ID IN ");
		subQuery.append("(SELECT DISTINCT(CHILD_LOCATION_ID) FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID=").append(Constants.STORE_LIST_LEVEL_ID);
		subQuery.append("AND LOCATION_ID=").append(storeListId);
		subQuery.append(" AND CHILD_LOCATION_LEVEL_ID= ").append(Constants.STORE_LEVEL_ID).append(") AND PRICE_ZONE_ID IS NOT NULL))");

		// any store in the zone
		subQuery.append("OR (LOCATION_LEVEL_ID = ").append(Constants.STORE_LEVEL_ID).append(" AND LOCATION_ID IN(");
		subQuery.append("SELECT DISTINCT(CHILD_LOCATION_ID) FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID=").append(Constants.STORE_LIST_LEVEL_ID);
		subQuery.append("AND LOCATION_ID=").append(storeListId).append(" AND CHILD_LOCATION_LEVEL_ID= ").append(Constants.STORE_LEVEL_ID);
		subQuery.append("))");
		
		//NU:: 10th Nov 2016, for giant eagle, zone id has to be taken from store item map
		//as store doesn't directly mapped to zone
		if(Boolean.parseBoolean(PropertyManager.getProperty("ZONE_ID_FROM_STORE_ITEM_MAP", "FALSE"))) {
			subQuery.append("OR (LOCATION_LEVEL_ID=").append(Constants.ZONE_LEVEL_ID).append(" AND LOCATION_ID IN(");
			subQuery.append("SELECT DISTINCT(PRICE_ZONE_ID) FROM STORE_ITEM_MAP WHERE ITEM_CODE IN (");
			subQuery.append(PRCommonUtil.getCommaSeperatedStringFromStrArray(itemCodes));
			subQuery.append(") AND LEVEL_TYPE_ID=").append(Constants.STORE_LEVEL_TYPE_ID);
			subQuery.append("AND LEVEL_ID IN");
			subQuery.append("(SELECT DISTINCT(CHILD_LOCATION_ID) FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID=");
			subQuery.append(Constants.STORE_LIST_LEVEL_ID);
			subQuery.append("AND LOCATION_ID=").append(storeListId);
			subQuery.append(" AND CHILD_LOCATION_LEVEL_ID= ").append(Constants.STORE_LEVEL_ID).append(")");
			subQuery.append("))");
		}
		
		subQuery.append(" ) ");
		return subQuery.toString();
	}
	
	
	public int resetAdplexFlag(Connection conn, List<PromoBuyItems> updateList) throws GeneralException{
		int updateCount = 0;
		PreparedStatement statement = null;
		try{
			int itemsInBatch = 0;
			statement = conn.prepareStatement(UPDATE_AD_PLEX);
			for(PromoBuyItems promoBuyItems: updateList){
				int counter = 0;
				statement.setLong(++counter, promoBuyItems.getPromoDefnId());
				statement.setInt(++counter, promoBuyItems.getItemCode());
				statement.addBatch();
				itemsInBatch++;
				if(itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		statement.clearBatch();
	        		itemsInBatch = 0;
	        		updateCount += PristineDBUtil.getUpdateCount(count);
	        	}

		    }
        	if(itemsInBatch > 0){
        		int[] count = statement.executeBatch();
        		statement.clearBatch();
        		updateCount +=  PristineDBUtil.getUpdateCount(count);
        	}
			
			
		}
		catch(SQLException SqlE){
			logger.error("updateAdplexFlag() - Error while updating promotions - " + SqlE.toString()); 
			throw new GeneralException("updateAdplexFlag() - Error while updating promotions", SqlE);
		}
		finally{
			PristineDBUtil.close(statement);
		}
		return updateCount;
	}
	
	public HashMap<Long, List<LocationKey>> getPromoLocations(Connection conn, HashSet<Long> promoIds) {
		HashMap<Long, List<LocationKey>> promoLocations = new HashMap<Long, List<LocationKey>>();
		List<Long> promoIdList = new ArrayList<>();
		try {
			int limitCount = 0;
			for (Long promoId : promoIds) {
				promoIdList.add(promoId);
				limitCount++;
				if (limitCount > 0 && (limitCount % Constants.BATCH_UPDATE_COUNT == 0)) {
					Object[] values = promoIdList.toArray();
					retreivePromoLocations(promoLocations, values);
					promoIdList.clear();
				}
			}
			if (promoIdList.size() > 0) {
				Object[] values = promoIdList.toArray();
				retreivePromoLocations(promoLocations, values);
				promoIdList.clear();
			}
		} catch (GeneralException e) {
			logger.error("Error -- getPromoLocations() - ", e);
		}
		return promoLocations;
	}
	
	private void retreivePromoLocations(HashMap<Long, List<LocationKey>> promoLocations, Object... values) throws GeneralException{
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			String query = new String(GET_PROMO_LOCATIONS);
			statement = conn.prepareStatement(String.format(query, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			statement.setFetchSize(200000);
	     	resultSet = statement.executeQuery();
	     	logger.debug("statement is executed");
	     	while(resultSet.next()){
	     		long promoDefId = resultSet.getLong("PROMO_DEFINITION_ID");
	     		LocationKey locationKey = new LocationKey(resultSet.getInt("LOCATION_LEVEL_ID"), resultSet.getInt("LOCATION_ID"));
	     		List<LocationKey> locations = new ArrayList<LocationKey>();
	     		
	     		if(promoLocations.get(promoDefId)!=null){
	     			locations = promoLocations.get(promoDefId);
	     		}
	     		locations.add(locationKey);
	     		promoLocations.put(promoDefId, locations);
	     	}
		}
		catch(SQLException SqlE){
			throw new GeneralException("retreivePromoLocations() - Error while retreiving promotions ", SqlE); 
		}
		finally{
			PristineDBUtil.close(statement);
			PristineDBUtil.close(resultSet);
		}
	}
	
	
	private long getPromoDefnIdForSuperCoupon(PromoDefinition promotion) throws GeneralException{
		long promoDefnId = -1;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<String> itemCodeList = new ArrayList<String>();
		int cnt = 0;
		for(PromoBuyItems promoBuyItems:promotion.getBuyItems()){
			cnt++;
			if(cnt < 999)
				itemCodeList.add(String.valueOf(promoBuyItems.getItemCode()));
			else 
				break;
		}
		String itemCodeValue = PRCommonUtil.getCommaSeperatedStringFromStrArray(itemCodeList);
		try{
			String query = new String(IS_PROMOTION_EXISTS_IN_BUY_ITEM);
			query = query.replaceAll("%ITEM_CODE_LIST%", itemCodeValue);
			stmt = conn.prepareStatement(query);
			int counter = 0;
			stmt.setInt(++counter, promotion.getStartCalId());
			stmt.setInt(++counter, promotion.getEndCalId());
			stmt.setInt(++counter, promotion.getPromoDefnTypeId());
			rs = stmt.executeQuery();
			if(rs.next()){
				promoDefnId = rs.getInt("PROMO_DEFINITION_ID");
			}
		}catch(SQLException exception){
			logger.error("Error in getPromoDefnIdForSuperCoupon() - " + exception);
			throw new GeneralException("Exception in getPromoDefnIdForSuperCoupon() " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return promoDefnId;
	}
	
	private long getPromoDefnIdUsingOfferItem(List<PromoOfferItem> offerDetailsList, PromoDefinition promotion){
		long promoDefnId = -1;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<String> itemCodeList = new ArrayList<String>();
		for(PromoOfferItem promoOfferItems:offerDetailsList){
			itemCodeList.add(String.valueOf(promoOfferItems.getItemCode()));
		}
		String itemCodeValue = PRCommonUtil.getCommaSeperatedStringFromStrArray(itemCodeList);
		try{
			String query = new String(IS_PROMOTION_EXISTS_IN_OFFER_ITEM);
			query = query.replaceAll("%ITEM_CODE%", itemCodeValue);
			stmt = conn.prepareStatement(query);
			int counter = 0;
			stmt.setInt(++counter, promotion.getStartCalId());
			stmt.setInt(++counter, promotion.getEndCalId());
			stmt.setInt(++counter, promotion.getPromoDefnTypeId());
			rs = stmt.executeQuery();
			if(rs.next()){
				promoDefnId = rs.getInt("PROMO_DEFINITION_ID");
			}
		}catch(SQLException exception){
			logger.error("Error in getPromoDefnIdUsingOfferItem() - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return promoDefnId;
	}
	
	private List<PromoDefinition> getPromotionsByCustomDateAndItems(
			List<PromoDefinition> allPromotions) throws GeneralException {
		setMinAndMaxDateRangeFromPromotions(allPromotions);
		List<PromoDefinition> promoList = new ArrayList<PromoDefinition>();
		Set<Integer> intPromoTypeIds =  allPromotions.stream().map(PromoDefinition::getPromoTypeId).collect(Collectors.toSet());
		Set<String> promoTypeIds = intPromoTypeIds.stream().map(Object::toString).collect(Collectors.toSet());
		Set<LocationKey> locations = allPromotions.stream().map(PromoDefinition::getLocationKey).collect(Collectors.toSet());
		/*int locationLevelId = allPromotions.get(0).getLocationLevelId();
		int locationId = allPromotions.get(0).getLocationId();*/
		if (minStartDate != null && maxEndDate != null) {
			PreparedStatement statement = null;
			ResultSet resultSet = null;
			try {
				// PD.PROMO_DEFINITION_ID, PD.RET_LIR_ID,
				// PD.SRC_VENDOR_AND_ITEM_ID, PD.PROMO_TYPE_ID,
				// START_CALENDAR_ID, END_CALENDAR_ID
				String query = new String(GET_PROMOTIONS_WITHIN_DATE_RANGE_AND_ITEMS);
				query = query.replaceAll("%PROMO_TYPE_IDS%", promoTypeIds.stream().collect(Collectors.joining(",")));
				statement = conn
						.prepareStatement(query);
				int counter = 0;
				statement.setString(++counter, minStartDate);
				statement.setString(++counter, maxEndDate);
				/*statement.setInt(++counter, locationLevelId);
				statement.setInt(++counter, locationId);*/
				//logger.debug("SELECT RC.START_DATE START_DATE, RC1.START_DATE END_DATE, PD.PROMO_DEFINITION_ID, PD.START_CALENDAR_ID, PD.END_CALENDAR_ID, "
				//+ "PD.RET_LIR_ID, PD.SRC_VENDOR_AND_ITEM_ID, PD.PROMO_TYPE_ID FROM PM_PROMO_DEFINITION PD JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = PD.START_CALENDAR_ID "
				//+ "JOIN RETAIL_CALENDAR RC1 ON RC1.CALENDAR_ID = PD.END_CALENDAR_ID WHERE RC.START_DATE >= TO_DATE(" + minStartDate + " , 'MM/DD/YYYY') AND RC1.START_DATE <= TO_DATE(" + maxEndDate + ", 'MM/DD/YYYY')");
				resultSet = statement.executeQuery();
				while (resultSet.next()) {
					PromoDefinition promotion = new PromoDefinition();
					PromoBuyItems promoBuyItem = new PromoBuyItems();
					promotion.setPromoDefnId(resultSet
							.getLong("PROMO_DEFINITION_ID"));
					promotion.setStartCalId(resultSet
							.getInt("START_CALENDAR_ID"));
					promotion.setEndCalId(resultSet.getInt("END_CALENDAR_ID"));
					promotion.setRetLirId(resultSet.getInt("RET_LIR_ID"));
					promotion.setRetailerItemCode(resultSet
							.getString("SRC_VENDOR_AND_ITEM_ID"));
					promotion.setPromoTypeId(resultSet.getInt("PROMO_TYPE_ID"));
					promotion.setLocationLevelId(resultSet.getInt("LOCATION_LEVEL_ID"));
					promotion.setLocationId(resultSet.getInt("LOCATION_ID"));
					promoBuyItem.setSaleQty(resultSet.getInt("SALE_QTY"));
					promoBuyItem.setSalePrice(resultSet.getDouble("SALE_PRICE"));
					promoBuyItem.setSaleMPrice(resultSet.getDouble("SALE_M_PRICE"));
					promoBuyItem.setItemCode(resultSet.getInt("ITEM_CODE"));
					promotion.addBuyItems(promoBuyItem);
					if(locations.contains(promotion.getLocationKey())) {
						promoList.add(promotion);	
					}
				}
			} catch (SQLException sqlE) {
				logger.error("Error while getting promotions by date range - "
						+ sqlE);
				throw new GeneralException(
						"Error while getting promotions by date range", sqlE);
			} finally {
				PristineDBUtil.close(resultSet);
				PristineDBUtil.close(statement);
			}
		} else
			throw new GeneralException("Unable to proceed without date range");

		return promoList;
	}
}