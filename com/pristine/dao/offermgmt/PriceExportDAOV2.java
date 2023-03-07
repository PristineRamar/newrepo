package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.ReDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.fileformatter.gianteagle.ZoneDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExportRunHeader;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PriceExportDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.RecommendationStatusLookup;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;


public class PriceExportDAOV2 {

	private static Logger logger = Logger.getLogger("PriceExportDAOV2");
	
	public static String GET_SQL_FOR_EMPTY_START_DATE = "INSERT INTO PR_EC_EXPORT_DETAIL (EC_EXPORT_DETAIL_ID, EC_EXPORT_HEADER_ID,"
			+ " PRICE_CHECK_LIST_ID, ITEM_CODE, STORE_ID, COMMENTS, EC_RETAIL, ZONE_NUM) VALUES ( PR_EC_EXPORT_DET_SEQ.NEXTVAL,"
			+ " ?, ?, ?, ?, ?, ?, ?)";
	public static String GET_SQL_FOR_DATA_WITH_START_DATE = "INSERT INTO PR_EC_EXPORT_DETAIL (EC_EXPORT_DETAIL_ID, EC_EXPORT_HEADER_ID,"
			+ " PRICE_CHECK_LIST_ID, ITEM_CODE, STORE_ID, COMMENTS, START_DATE, EC_RETAIL, ZONE_NUM) VALUES ( PR_EC_EXPORT_DET_SEQ.NEXTVAL,"
			+ " ?, ?, ?, ?, ?, TO_DATE(?,'MM/dd/yyyy'), ?, ?)";
	private static final String GET_EXPORT_ID = "SELECT PR_EXPORT_RUN_SEQ.NEXTVAL AS EXPORT_ID FROM DUAL";
	private static final String INSERT_EXPORT_HEADER = "INSERT INTO PR_EXPORT_RUN_HEADER "
			+ "(EXPORT_ID, USER_ID, START_TIME, STATUS, RUN_TYPE, EXPORT_TYPE, EFFECTIVE_DATE, SF_THRESHOLD) VALUES "
			+ "(?, ?, SYSDATE, ?, ?, ?, ?, ?)";

	private static final String UPDATE_EXPORT_HEADER_END_DATE = "UPDATE PR_EXPORT_RUN_HEADER SET END_RUN_TIME = SYSDATE WHERE EXPORT_ID = ?";

	private static final String UPDATE_EXPORT_HEADER_STATUS = "UPDATE PR_EXPORT_RUN_HEADER SET STATUS = ? WHERE EXPORT_ID = ?";
	
	private static final String UPDATE_EXPORT_HEADER_FILEPATH = "UPDATE PR_EXPORT_RUN_HEADER SET EXPORT_FILE_PATH = ? WHERE EXPORT_ID = ?";

	
	
	public String getUserDetails(Connection conn, String userId) {
		String userDetail = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			StringBuffer sb = new StringBuffer();
			sb.append("SELECT FIRST_NAME, LAST_NAME FROM USER_DETAILS WHERE USER_ID = '").append(userId).append("'");

			stmt = conn.prepareStatement(sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				userDetail = rs.getString("FIRST_NAME") + " " + rs.getString("LAST_NAME");
			}
		} catch (SQLException ex) {
			logger.error("Error when getting user details - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return userDetail;
	}

	public List<PriceExportDTO> getItemsFromApprovedRecommendationsWithPriceTest(Connection conn, List<Long> runIdList,
			String priceExportType, boolean emergency, String currWeekEndDate, List<String> itemStoreCombinationsFromPriceTest) throws GeneralException {
	
		
		
		List<PriceExportDTO> baseList = new ArrayList<PriceExportDTO>();
		List<PriceExportDTO> nonCandidateItemList = new ArrayList<PriceExportDTO>();
		List<String> stringConvertedRunIds = new ArrayList<String>();

		runIdList.forEach(runId -> {
			stringConvertedRunIds.add(String.valueOf(runId));
		});

		if (runIdList.size() > 0) {

			StringBuffer sb = new StringBuffer();
			sb.append(" SELECT CALENDAR_ID, RUN_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, LOCATION_LEVEL_ID, LOCATION_ID, REC_REG_PRICE, ");
			sb.append(" CORE_RETAIL, RET_LIR_ID, RETAILER_ITEM_CODE, IS_EXPORTED, REG_EFF_DATE, ITEM_TYPE, RU_NAME, RU_ID, PART_NAME, REG_PRICE, ");
			sb.append(" REG_MULTIPLE, PREDICTED_BY, PRICE_ZONE_ID, ZONE_NAME, PRICE_TYPE,");
			sb.append(" OVERRIDE_REG_PRICE, UPDATED_BY, FIRST_NAME, LAST_NAME, ZONE_NUM, REC_REG_MULTIPLE, PREDICTED, APPROVED_ON, TOTAL_IMPACT, ");
			sb.append(" PRC_CHANGE_IMPACT, OVERRIDE_REG_MULTIPLE, DENSE_RANK() OVER (ORDER BY CALENDAR_ID) WEEK_RANK, EXPORT_RANK, ");
			//commented for AI #19-b
			//taking exact impact instead of abs impact
			sb.append(" (CASE WHEN ITEM_TYPE = 'S' THEN (DENSE_RANK() OVER (PARTITION BY CALENDAR_ID ORDER BY TOTAL_IMPACT DESC)) ");
			//sb.append(" (CASE WHEN ITEM_TYPE = 'S' THEN (DENSE_RANK() OVER (PARTITION BY CALENDAR_ID ORDER BY ABS(TOTAL_IMPACT) DESC)) ");
			//Attribute priority added for AI #109
			sb.append(" ELSE 0 END)  IMPACT_RANK, FAMILY, PRIORITY, ");
			//added on 01/27/2022
			sb.append(" STORE_ID, REQ_ZONE_ID, COMP_STR_NO FROM  ( ");
			//ends
			sb.append(" SELECT RH.PRODUCT_ID RU_ID, CAL.CALENDAR_ID, IL.USER_ATTR_14 FAMILY, EX.RUN_ID, EX.PRICE_TYPE, RI.PRODUCT_LEVEL_ID, EX.PRIORITY, ");
			sb.append(" RH.LOCATION_ID, RI.REC_REG_PRICE , RI.CORE_RETAIL, RI.RET_LIR_ID, IL.RETAILER_ITEM_CODE, EX.ITEM_CODE AS PRODUCT_ID, RH.LOCATION_LEVEL_ID,");
			sb.append(" RI.IS_EXPORTED,TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY HH24:MI:SS') AS REG_EFF_DATE, IL.USER_ATTR_6 ITEM_TYPE, PG.NAME RU_NAME, ");
			sb.append(" IL.USER_ATTR_3 PART_NAME, RI.REG_PRICE, RI.REG_MULTIPLE, RH.PREDICTED_BY, ");
			sb.append(" RPZ.PRICE_ZONE_ID, RPZ.NAME AS ZONE_NAME, RI.OVERRIDE_REG_PRICE, QRS.UPDATED_BY, ");
			sb.append(" CASE WHEN RH.STATUS = 7 THEN 1 ELSE 2 END EXPORT_RANK, ");
			sb.append(" UD.FIRST_NAME, UD.LAST_NAME, RPZ.ZONE_NUM,  RI.REC_REG_MULTIPLE, ");
			sb.append(" TO_CHAR(RH.PREDICTED, 'MM/DD/YYYY HH24:MI:SS') AS PREDICTED, SUM(CASE WHEN IL.USER_ATTR_6 = 'S' THEN PRC_CHANGE_IMPACT ");
			sb.append(" ELSE 0 END) OVER(PARTITION BY PG.NAME) TOTAL_IMPACT, ");
			sb.append(" RI.PRC_CHANGE_IMPACT, RI.OVERRIDE_REG_MULTIPLE, TO_CHAR(RH.APPROVED, 'MM/DD/YYYY HH24:MI:SS') AS APPROVED_ON, ");
			sb.append(" LGR.CHILD_LOCATION_ID AS STORE_ID, REQ.LOCATION_ID AS REQ_ZONE_ID, CS.COMP_STR_NO ,RPZ.PRICE_ZONE_ID AS ZONE_ID ");
			sb.append(" FROM PR_PRICE_EXPORT EX LEFT JOIN PR_QUARTER_REC_ITEM RI ");
			sb.append(" ON RI.RUN_ID = EX.RUN_ID AND RI.PRODUCT_ID = EX.ITEM_CODE AND RI.PRODUCT_LEVEL_ID = 1");
			sb.append(" LEFT JOIN PR_QUARTER_REC_HEADER RH ON RI.RUN_ID = RH.RUN_ID  ");
			//added on 01/27/2022
			sb.append(" LEFT JOIN PR_PRICE_TEST_REQUEST REQ ON REQ.LOCATION_ID = RH.LOCATION_ID  AND REQ.LOCATION_LEVEL_ID=RH.LOCATION_LEVEL_ID ");
			sb.append(" AND REQ.PRODUCT_LEVEL_ID =RH.PRODUCT_LEVEL_ID AND REQ.PRODUCT_ID =RH.PRODUCT_ID ");		
			sb.append(" LEFT JOIN LOCATION_GROUP_RELATION LGR ON REQ.SL_LEVEL_ID = LGR.LOCATION_LEVEL_ID AND REQ.SL_LOCATION_ID=LGR.LOCATION_ID  ");
			sb.append(" LEFT JOIN LOCATION_GROUP LG ON LG.LOCATION_LEVEL_ID =LGR.LOCATION_LEVEL_ID AND LG.LOCATION_ID = LGR.LOCATION_ID ");
			sb.append(" LEFT JOIN COMPETITOR_STORE CS ON  LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID  ");
			sb.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID ");
			//ends	
			sb.append(" LEFT JOIN RETAIL_CALENDAR cal ON RH.APPROVED BETWEEN CAL.START_DATE AND CAL.END_DATE AND CAL.ROW_TYPE = 'W'  ");
			sb.append(" LEFT JOIN PRODUCT_GROUP PG ON RH.PRODUCT_ID = PG.PRODUCT_ID LEFT JOIN RETAIL_PRICE_ZONE RPZ ");
			sb.append(" ON RH.LOCATION_ID = RPZ.PRICE_ZONE_ID LEFT JOIN ITEM_LOOKUP IL ");
			sb.append(" ON RI.PRODUCT_ID = IL.ITEM_CODE ").append(" LEFT JOIN PR_QUARTER_REC_STATUS QRS ");
			sb.append(" ON RI.RUN_ID = QRS.RUN_ID LEFT JOIN USER_DETAILS UD ON UD.USER_ID = QRS.UPDATED_BY ");
			sb.append(" WHERE RI.CAL_TYPE='Q' AND RI.PRODUCT_LEVEL_ID = 1 ");		
			sb.append(" AND QRS.STATUS IN ( ").append(RecommendationStatusLookup.APPROVED.getStatusId()).append(" , ");
			sb.append(RecommendationStatusLookup.EMERGENCY_APPROVED.getStatusId()).append(")");
			sb.append(" AND RI.RUN_ID IN (").append(String.join(",", stringConvertedRunIds)).append(")");
			if(emergency) {
				sb.append(" AND EX.PRICE_TYPE = '").append(Constants.EMERGENCY).append("'");
			}
			else {
				sb.append(" AND EX.PRICE_TYPE = '").append(Constants.NORMAL).append("'");
			}

			if ((priceExportType.equals(Constants.EMERGENCY_OR_HARDPART) && !emergency)
					|| priceExportType.equals(Constants.HARD_PART_ITEMS)) {
				sb.append(" AND IL.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			} else if ((priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR) && !emergency)
					|| priceExportType.equals(Constants.SALE_FLOOR_ITEMS)) {
				sb.append(" AND IL.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			} 
			if(currWeekEndDate!="") {
				sb.append(" AND TO_DATE(TO_CHAR(RI.REG_EFF_DATE,'MM/dd/yyyy'),'MM/dd/yyyy')=TO_DATE('"+currWeekEndDate+"','MM/dd/yyyy') )");
			}
			
			logger.debug(sb.toString());
			PreparedStatement stmt = null;
			ResultSet rs = null;
			try {

				stmt = conn.prepareStatement(sb.toString());
				rs = stmt.executeQuery();
				while (rs.next()) {
					PriceExportDTO item = new PriceExportDTO();

					item.setZoneType("R");
					item.setRunId(rs.getLong("RUN_ID"));
					item.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
					item.setItemCode(rs.getInt("PRODUCT_ID"));
					item.setChildLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
					item.setChildLocationId(rs.getInt("LOCATION_ID"));
					item.setItemType(rs.getString("ITEM_TYPE"));
					item.setRetLirId(rs.getInt("RET_LIR_ID"));
					item.setPriceZoneId(rs.getInt("PRICE_ZONE_ID"));
					item.setPriceZoneNo(rs.getString("ZONE_NUM"));
					item.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));

					MultiplePrice recommendedPrice = new MultiplePrice(rs.getInt("REC_REG_MULTIPLE"),rs.getDouble("REC_REG_PRICE"));
					item.setRecommendedRegPrice(recommendedPrice);

					MultiplePrice currentPrice = new MultiplePrice(rs.getInt("REG_MULTIPLE"),rs.getDouble("REG_PRICE"));
					item.setCurrentRegPrice(currentPrice);

					item.setRegEffDate(rs.getString("REG_EFF_DATE"));
					item.setPriceExportType(rs.getString("PRICE_TYPE"));

					item.setApprovedOn(rs.getString("APPROVED_ON"));
					item.setApprovedBy(rs.getString("UPDATED_BY"));
					item.setApproverName(rs.getString("FIRST_NAME") + " " + rs.getString("LAST_NAME"));

					item.setVdpRetail(item.getRecommendedRegPrice().getUnitPrice());
					item.setCoreRetail(rs.getDouble("CORE_RETAIL"));
					item.setImpact(rs.getDouble("PRC_CHANGE_IMPACT"));
					// item.setPredicted(rs.getString("PREDICTED"));
					item.setPredicted(rs.getString("PREDICTED"));

					item.setPartNumber(rs.getString("PART_NAME"));
					item.setOverrideRegMultiple(rs.getInt("OVERRIDE_REG_MULTIPLE"));
					item.setOverrideRegPrice(rs.getDouble("OVERRIDE_REG_PRICE"));
					
					item.setSF_RU_rank(rs.getInt("IMPACT_RANK"));
					item.setSF_week_rank(rs.getInt("WEEK_RANK"));
					item.setSF_export_rank(rs.getInt("EXPORT_RANK"));
					//commented for AI #19-b
					//taking exact impact instead of abs impact
					item.setTotal_Impact(rs.getDouble("TOTAL_IMPACT"));
					if(rs.getString("FAMILY") == null) {
						item.setFamilyName("");
					}else {
						item.setFamilyName(rs.getString("FAMILY"));
					}

					//Attribute priority added for AI #109
					item.setPriority(rs.getString("PRIORITY"));
					
					MultiplePrice overridePrice = new MultiplePrice(rs.getInt("OVERRIDE_REG_MULTIPLE"),	rs.getDouble("OVERRIDE_REG_PRICE"));
					item.setOverriddenRegularPrice(overridePrice);

					if (overridePrice.price > 0) {
						item.setDiffRetail(overridePrice.getUnitPrice() - currentPrice.getUnitPrice());
						double diffInPrice = round(item.getDiffRetail(), 2);
						item.setDiffRetail(diffInPrice);
						item.setVdpRetail(overridePrice.getUnitPrice());
					} else {
						item.setDiffRetail(recommendedPrice.getUnitPrice() - currentPrice.getUnitPrice());
						double diffInPrice = round(item.getDiffRetail(), 2);
						item.setDiffRetail(diffInPrice);
						item.setVdpRetail(recommendedPrice.getUnitPrice());
					}

					item.setZoneName(rs.getString("ZONE_NAME"));
					item.setRecommendationUnit(rs.getString("RU_NAME"));
					item.setRecommendationUnitId(rs.getInt("RU_ID"));
					
					//added on 01/27/2022					
					item.setStoreNo(rs.getString("COMP_STR_NO"));
					item.setStoreId(rs.getString("STORE_ID"));
					item.setTestZoneNumReq(rs.getString("REQ_ZONE_ID"));
					item.setChildLocationLevelId(Constants.STORE_LEVEL_TYPE_ID);
					item.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
					//ends
					
					boolean isItemRecommended = true;
					if (overridePrice.price > 0) {
						if (overridePrice.equals(currentPrice)) {
							isItemRecommended = false;
						}
					}else if(recommendedPrice.price > 0){
						//AI#17 - Changes done on 2/7/2022 by Bhargavi
						//changes as part of Zone1000 impact calculation
						if (recommendedPrice.equals(currentPrice) && item.getImpact() == 0) {
							isItemRecommended = false;
						}
					}else {
						isItemRecommended = false;
					}
					
					if (isItemRecommended) {
						boolean validItem = checkMandatoryColumnValidation(item);
						if(validItem) {
							baseList.add(item);
							String key = item.getItemCode() + "_" + item.getStoreId();
							if (!itemStoreCombinationsFromPriceTest.contains(key)) {
								itemStoreCombinationsFromPriceTest.add(key);
							}
						}
						else {
							nonCandidateItemList.add(item);
						}
					}
					else {
						nonCandidateItemList.add(item);
					}
				}
			} catch (Exception e) {
				throw new GeneralException("getBaseDataForExport() - Error getting recommneded items for runids", e);
			} finally {
				PristineDBUtil.close(rs);
				PristineDBUtil.close(stmt);
			}
		}

		logger.info(
				"getItemsFromApprovedRecommendations() - # of approved items in candidate list is: " + baseList.size());
		logger.info(
				"getItemsFromApprovedRecommendations() - # of approved items in non-candidate list is: " + nonCandidateItemList.size());
		if(nonCandidateItemList.size() > 0) {
			List<String> invalidRecords = new ArrayList<>();
			for (PriceExportDTO item : nonCandidateItemList) {
				invalidRecords.add(item.getRunId() + "-" + item.getItemCode());
			}

			logger.debug("Non candidate RunId-item are : " + String.join(";", invalidRecords));
		}
		return baseList;
	}
	
	public List<PriceExportDTO> getItemsFromApprovedRecommendationsV3(Connection conn, List<Long> runIdList,
			String priceExportType, boolean emergency, String currWeekEndDate) throws GeneralException {
	
		
		
		List<PriceExportDTO> baseList = new ArrayList<PriceExportDTO>();
		List<PriceExportDTO> nonCandidateItemList = new ArrayList<PriceExportDTO>();
		List<String> stringConvertedRunIds = new ArrayList<String>();

		runIdList.forEach(runId -> {
			stringConvertedRunIds.add(String.valueOf(runId));
		});

		if (runIdList.size() > 0) {

			StringBuffer sb = new StringBuffer();
			sb.append(" SELECT CALENDAR_ID, RUN_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, LOCATION_LEVEL_ID, LOCATION_ID, REC_REG_PRICE, ");
			sb.append(" CORE_RETAIL, RET_LIR_ID, RETAILER_ITEM_CODE, IS_EXPORTED, REG_EFF_DATE, ITEM_TYPE, RU_NAME, RU_ID, PART_NAME, REG_PRICE, ");
			sb.append(" REG_MULTIPLE, PREDICTED_BY, PRICE_ZONE_ID, ZONE_NAME, PRICE_TYPE,");
			sb.append(" OVERRIDE_REG_PRICE, UPDATED_BY, FIRST_NAME, LAST_NAME, ZONE_NUM, REC_REG_MULTIPLE, PREDICTED, APPROVED_ON, TOTAL_IMPACT, ");
			sb.append(" PRC_CHANGE_IMPACT, OVERRIDE_REG_MULTIPLE, DENSE_RANK() OVER (ORDER BY CALENDAR_ID) WEEK_RANK, EXPORT_RANK, ");
			//commented for AI #19-b
			//taking exact impact instead of abs impact
			sb.append(" (CASE WHEN ITEM_TYPE = 'S' THEN (DENSE_RANK() OVER (PARTITION BY CALENDAR_ID ORDER BY TOTAL_IMPACT DESC)) ");
			//sb.append(" (CASE WHEN ITEM_TYPE = 'S' THEN (DENSE_RANK() OVER (PARTITION BY CALENDAR_ID ORDER BY ABS(TOTAL_IMPACT) DESC)) ");
			//Attribute priority added for AI #109 - hard dates
			sb.append(" ELSE 0 END)  IMPACT_RANK, FAMILY, PRIORITY,HD_REASON_CODE, GLOBAL_ZONE AS IS_GLOBAL_ZONE FROM (");
			sb.append(" SELECT RH.PRODUCT_ID RU_ID, CAL.CALENDAR_ID, IL.USER_ATTR_14 FAMILY, EX.RUN_ID, EX.PRICE_TYPE, RI.PRODUCT_LEVEL_ID, EX.PRIORITY,  RI.HD_REASON_CODE,");
			sb.append(" RH.LOCATION_ID, RI.REC_REG_PRICE , RI.CORE_RETAIL, RI.RET_LIR_ID, IL.RETAILER_ITEM_CODE, EX.ITEM_CODE AS PRODUCT_ID, RH.LOCATION_LEVEL_ID,");
			sb.append(" RI.IS_EXPORTED,TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY HH24:MI:SS') AS REG_EFF_DATE, IL.USER_ATTR_6 ITEM_TYPE, PG.NAME RU_NAME, ");
			sb.append(" IL.USER_ATTR_3 PART_NAME, RI.REG_PRICE, RI.REG_MULTIPLE, RH.PREDICTED_BY, ");
			sb.append(" RPZ.PRICE_ZONE_ID, RPZ.NAME AS ZONE_NAME, RI.OVERRIDE_REG_PRICE, QRS.UPDATED_BY, ");
			sb.append(" CASE WHEN RH.STATUS = 7 THEN 1 ELSE 2 END EXPORT_RANK, ");
			sb.append(" UD.FIRST_NAME, UD.LAST_NAME, RPZ.ZONE_NUM,  RI.REC_REG_MULTIPLE, ");
			sb.append(" TO_CHAR(RH.PREDICTED, 'MM/DD/YYYY HH24:MI:SS') AS PREDICTED, SUM(CASE WHEN IL.USER_ATTR_6 = 'S' THEN PRC_CHANGE_IMPACT ");
			sb.append(" ELSE 0 END) OVER(PARTITION BY PG.NAME) TOTAL_IMPACT, ");
			sb.append(" RI.PRC_CHANGE_IMPACT, RI.OVERRIDE_REG_MULTIPLE, TO_CHAR(RH.APPROVED, 'MM/DD/YYYY HH24:MI:SS') AS APPROVED_ON,RP.GLOBAL_ZONE ");			
			sb.append(" FROM PR_PRICE_EXPORT EX LEFT JOIN PR_QUARTER_REC_ITEM RI ");
			sb.append(" ON RI.RUN_ID = EX.RUN_ID AND RI.PRODUCT_ID = EX.ITEM_CODE AND RI.PRODUCT_LEVEL_ID = 1");
			sb.append(" LEFT JOIN PR_QUARTER_REC_HEADER RH ON RI.RUN_ID = RH.RUN_ID  ");
			sb.append(" LEFT JOIN RETAIL_CALENDAR cal ON RH.APPROVED BETWEEN CAL.START_DATE AND CAL.END_DATE AND CAL.ROW_TYPE = 'W'  ");
			sb.append(" LEFT JOIN PRODUCT_GROUP PG ON RH.PRODUCT_ID = PG.PRODUCT_ID LEFT JOIN RETAIL_PRICE_ZONE RPZ ");
			sb.append(" ON RH.LOCATION_ID = RPZ.PRICE_ZONE_ID LEFT JOIN ITEM_LOOKUP IL ");
			sb.append(" ON RI.PRODUCT_ID = IL.ITEM_CODE LEFT JOIN PR_QUARTER_REC_STATUS QRS ");
			sb.append(" ON RI.RUN_ID = QRS.RUN_ID LEFT JOIN USER_DETAILS UD ON UD.USER_ID = QRS.UPDATED_BY LEFT JOIN RETAIL_PRICE_ZONE RP ON RP.PRICE_ZONE_ID=RH.LOCATION_ID ");
			sb.append(" WHERE RI.CAL_TYPE='Q' AND RI.PRODUCT_LEVEL_ID = 1 ");		
			sb.append(" AND QRS.STATUS IN ( ").append(RecommendationStatusLookup.APPROVED.getStatusId()).append(" , ");
			sb.append(RecommendationStatusLookup.EMERGENCY_APPROVED.getStatusId()).append(")");
			sb.append(" AND RI.RUN_ID IN (").append(String.join(",", stringConvertedRunIds)).append(")");
			if(emergency) {
				sb.append(" AND EX.PRICE_TYPE = '").append(Constants.EMERGENCY).append("'");
			}
			else {
				sb.append(" AND EX.PRICE_TYPE = '").append(Constants.NORMAL).append("'");
			}

			if ((priceExportType.equals(Constants.EMERGENCY_OR_HARDPART) && !emergency)
					|| priceExportType.equals(Constants.HARD_PART_ITEMS)) {
				sb.append(" AND IL.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			} else if ((priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR) && !emergency)
					|| priceExportType.equals(Constants.SALE_FLOOR_ITEMS)) {
				sb.append(" AND IL.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			} 
			if(currWeekEndDate!="") {
				sb.append(" AND TO_DATE(TO_CHAR(RI.REG_EFF_DATE,'MM/dd/yyyy'),'MM/dd/yyyy')=TO_DATE('"+currWeekEndDate+"','MM/dd/yyyy') )");
			}
			
			logger.debug("getItemsFromApprovedRecommendationsV3() qry: "+  sb.toString());
			PreparedStatement stmt = null;
			ResultSet rs = null;
			try {

				stmt = conn.prepareStatement(sb.toString());
				rs = stmt.executeQuery();
				while (rs.next()) {
					PriceExportDTO item = new PriceExportDTO();

					item.setZoneType("R");
					item.setRunId(rs.getLong("RUN_ID"));
					item.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
					item.setItemCode(rs.getInt("PRODUCT_ID"));
					item.setChildLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
					item.setChildLocationId(rs.getInt("LOCATION_ID"));
					item.setItemType(rs.getString("ITEM_TYPE"));
					item.setRetLirId(rs.getInt("RET_LIR_ID"));
					item.setPriceZoneId(rs.getInt("PRICE_ZONE_ID"));
					item.setPriceZoneNo(rs.getString("ZONE_NUM"));
					item.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));

					MultiplePrice recommendedPrice = new MultiplePrice(rs.getInt("REC_REG_MULTIPLE"),rs.getDouble("REC_REG_PRICE"));
					item.setRecommendedRegPrice(recommendedPrice);

					MultiplePrice currentPrice = new MultiplePrice(rs.getInt("REG_MULTIPLE"),rs.getDouble("REG_PRICE"));
					item.setCurrentRegPrice(currentPrice);

					item.setRegEffDate(rs.getString("REG_EFF_DATE"));
					item.setPriceExportType(rs.getString("PRICE_TYPE"));

					item.setApprovedOn(rs.getString("APPROVED_ON"));
					item.setApprovedBy(rs.getString("UPDATED_BY"));
					item.setApproverName(rs.getString("FIRST_NAME") + " " + rs.getString("LAST_NAME"));

					item.setVdpRetail(item.getRecommendedRegPrice().getUnitPrice());
					item.setCoreRetail(rs.getDouble("CORE_RETAIL"));
					item.setImpact(rs.getDouble("PRC_CHANGE_IMPACT"));
					// item.setPredicted(rs.getString("PREDICTED"));
					item.setPredicted(rs.getString("PREDICTED"));

					item.setPartNumber(rs.getString("PART_NAME"));
					item.setOverrideRegMultiple(rs.getInt("OVERRIDE_REG_MULTIPLE"));
					item.setOverrideRegPrice(rs.getDouble("OVERRIDE_REG_PRICE"));
					
					item.setSF_RU_rank(rs.getInt("IMPACT_RANK"));
					item.setSF_week_rank(rs.getInt("WEEK_RANK"));
					item.setSF_export_rank(rs.getInt("EXPORT_RANK"));
					//commented for AI #19-b
					//taking exact impact instead of abs impact
					item.setTotal_Impact(rs.getDouble("TOTAL_IMPACT"));
					if(rs.getString("FAMILY") == null) {
						item.setFamilyName("");
					}else {
						item.setFamilyName(rs.getString("FAMILY"));
					}

					//Attribute priority added for AI #109
					item.setPriority(rs.getString("PRIORITY"));
					
					//changes added for setting hard date
					item.setHardReasonCode(rs.getInt("HD_REASON_CODE"));
					
					if(rs.getInt("HD_REASON_CODE") > 0)
					{
						item.setHdFlag("Y");
					}
					else
					{
						item.setHdFlag("N");
					}
					
					MultiplePrice overridePrice = new MultiplePrice(rs.getInt("OVERRIDE_REG_MULTIPLE"),	rs.getDouble("OVERRIDE_REG_PRICE"));
					item.setOverriddenRegularPrice(overridePrice);

					if (overridePrice.price > 0) {
						item.setDiffRetail(overridePrice.getUnitPrice() - currentPrice.getUnitPrice());
						double diffInPrice = round(item.getDiffRetail(), 2);
						item.setDiffRetail(diffInPrice);
						item.setVdpRetail(overridePrice.getUnitPrice());
					} else {
						item.setDiffRetail(recommendedPrice.getUnitPrice() - currentPrice.getUnitPrice());
						double diffInPrice = round(item.getDiffRetail(), 2);
						item.setDiffRetail(diffInPrice);
						item.setVdpRetail(recommendedPrice.getUnitPrice());
					}

					item.setZoneName(rs.getString("ZONE_NAME"));
					item.setRecommendationUnit(rs.getString("RU_NAME"));
					item.setRecommendationUnitId(rs.getInt("RU_ID"));
					
					boolean isItemRecommended = true;
					if (overridePrice.price > 0) {
						if (overridePrice.equals(currentPrice)) {
							isItemRecommended = false;
						}
					}else if(recommendedPrice.price > 0){
						//Added this condition for AI#17 enhancement.
						//Z1000 can have an impact even if no new price is recommended 
						if (recommendedPrice.equals(currentPrice) && item.getImpact()==0) {
							isItemRecommended = false;
						}
					}else {
						isItemRecommended = false;
					}
					
					if (isItemRecommended) {
						boolean validItem = checkMandatoryColumnValidation(item);
						if(validItem) {
							//Populate TRUE if global zone is recommended.
							//Flag is added as the Z1000 prices were not exporting to Z4 and Z16
							if (rs.getString("IS_GLOBAL_ZONE") != null && 
									!rs.getString("IS_GLOBAL_ZONE").isEmpty() && 
									rs.getString("IS_GLOBAL_ZONE").equalsIgnoreCase("Y")) {
								item.setGlobalZoneRecommended(true);
							}
							else
								item.setGlobalZoneRecommended(false);
							
							baseList.add(item);
						}
						else {
							nonCandidateItemList.add(item);
						}
					}
					else {
						nonCandidateItemList.add(item);
					}
				}
			} catch (Exception e) {
				throw new GeneralException("getItemsFromApprovedRecommendationsV3() - Error getting recommneded items for runids"+  e);
			} finally {
				PristineDBUtil.close(rs);
				PristineDBUtil.close(stmt);
			}
		}

		logger.info("getItemsFromApprovedRecommendationsV3() - # of approved items in candidate list is: "
				+ baseList.size());
		logger.info("getItemsFromApprovedRecommendationsV3() - # of approved items in non-candidate list is: "
				+ nonCandidateItemList.size());
		if(nonCandidateItemList.size() > 0) {
			List<String> invalidRecords = new ArrayList<>();
			for (PriceExportDTO item : nonCandidateItemList) {
				invalidRecords.add(item.getRunId() + "-" + item.getItemCode());
			}

			logger.debug("getItemsFromApprovedRecommendationsV3()-Non candidate RunId-item are : " + String.join(";", invalidRecords));
		}
		return baseList;
	}

	public List<PRItemDTO> getItemsFromApprovedRecommendations(Connection conn, List<Long> runIdList,
			String priceExportType, boolean emergency, String currWeekEndDate) throws GeneralException {
	
		
		
		List<PRItemDTO> baseList = new ArrayList<PRItemDTO>();
		List<PRItemDTO> nonCandidateItemList = new ArrayList<PRItemDTO>();
		List<String> stringConvertedRunIds = new ArrayList<String>();

		runIdList.forEach(runId -> {
			stringConvertedRunIds.add(String.valueOf(runId));
		});

		if (runIdList.size() > 0) {

			StringBuffer sb = new StringBuffer();
			sb.append(" SELECT CALENDAR_ID, RUN_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, LOCATION_LEVEL_ID, LOCATION_ID, REC_REG_PRICE, ");
			sb.append(" CORE_RETAIL, RET_LIR_ID, RETAILER_ITEM_CODE, IS_EXPORTED, REG_EFF_DATE, ITEM_TYPE, RU_NAME, RU_ID, PART_NAME, REG_PRICE, ");
			sb.append(" REG_MULTIPLE, PREDICTED_BY, PRICE_ZONE_ID, ZONE_NAME, PRICE_TYPE,");
			sb.append(" OVERRIDE_REG_PRICE, UPDATED_BY, FIRST_NAME, LAST_NAME, ZONE_NUM, REC_REG_MULTIPLE, PREDICTED, APPROVED_ON, TOTAL_IMPACT, ");
			sb.append(" PRC_CHANGE_IMPACT, OVERRIDE_REG_MULTIPLE, DENSE_RANK() OVER (ORDER BY CALENDAR_ID) WEEK_RANK, EXPORT_RANK, ");
			//commented for AI #19-b
			//taking exact impact instead of abs impact
			sb.append(" (CASE WHEN ITEM_TYPE = 'S' THEN (DENSE_RANK() OVER (PARTITION BY CALENDAR_ID ORDER BY TOTAL_IMPACT DESC)) ");
			//sb.append(" (CASE WHEN ITEM_TYPE = 'S' THEN (DENSE_RANK() OVER (PARTITION BY CALENDAR_ID ORDER BY ABS(TOTAL_IMPACT) DESC)) ");
			//Attribute priority added for AI #109
			sb.append(" ELSE 0 END)  IMPACT_RANK, FAMILY, PRIORITY FROM (");
			sb.append(" SELECT RH.PRODUCT_ID RU_ID, CAL.CALENDAR_ID, IL.USER_ATTR_14 FAMILY, EX.RUN_ID, EX.PRICE_TYPE, RI.PRODUCT_LEVEL_ID, EX.PRIORITY, ");
			sb.append(" RH.LOCATION_ID, RI.REC_REG_PRICE , RI.CORE_RETAIL, RI.RET_LIR_ID, IL.RETAILER_ITEM_CODE, EX.ITEM_CODE AS PRODUCT_ID, RH.LOCATION_LEVEL_ID,");
			sb.append(" RI.IS_EXPORTED,TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY HH24:MI:SS') AS REG_EFF_DATE, IL.USER_ATTR_6 ITEM_TYPE, PG.NAME RU_NAME, ");
			sb.append(" IL.USER_ATTR_3 PART_NAME, RI.REG_PRICE, RI.REG_MULTIPLE, RH.PREDICTED_BY, ");
			sb.append(" RPZ.PRICE_ZONE_ID, RPZ.NAME AS ZONE_NAME, RI.OVERRIDE_REG_PRICE, QRS.UPDATED_BY, ");
			sb.append(" CASE WHEN RH.STATUS = 7 THEN 1 ELSE 2 END EXPORT_RANK, ");
			sb.append(" UD.FIRST_NAME, UD.LAST_NAME, RPZ.ZONE_NUM,  RI.REC_REG_MULTIPLE, ");
			sb.append(" TO_CHAR(RH.PREDICTED, 'MM/DD/YYYY HH24:MI:SS') AS PREDICTED, SUM(CASE WHEN IL.USER_ATTR_6 = 'S' THEN PRC_CHANGE_IMPACT ");
			sb.append(" ELSE 0 END) OVER(PARTITION BY PG.NAME) TOTAL_IMPACT, ");
			sb.append(" RI.PRC_CHANGE_IMPACT, RI.OVERRIDE_REG_MULTIPLE, TO_CHAR(RH.APPROVED, 'MM/DD/YYYY HH24:MI:SS') AS APPROVED_ON ");			
			sb.append(" FROM PR_PRICE_EXPORT EX LEFT JOIN PR_QUARTER_REC_ITEM RI ");
			sb.append(" ON RI.RUN_ID = EX.RUN_ID AND RI.PRODUCT_ID = EX.ITEM_CODE AND RI.PRODUCT_LEVEL_ID = 1");
			sb.append(" LEFT JOIN PR_QUARTER_REC_HEADER RH ON RI.RUN_ID = RH.RUN_ID  ");
			sb.append(" LEFT JOIN RETAIL_CALENDAR cal ON RH.APPROVED BETWEEN CAL.START_DATE AND CAL.END_DATE AND CAL.ROW_TYPE = 'W'  ");
			sb.append(" LEFT JOIN PRODUCT_GROUP PG ON RH.PRODUCT_ID = PG.PRODUCT_ID LEFT JOIN RETAIL_PRICE_ZONE RPZ ");
			sb.append(" ON RH.LOCATION_ID = RPZ.PRICE_ZONE_ID LEFT JOIN ITEM_LOOKUP IL ");
			sb.append(" ON RI.PRODUCT_ID = IL.ITEM_CODE LEFT JOIN PR_QUARTER_REC_STATUS QRS ");
			sb.append(" ON RI.RUN_ID = QRS.RUN_ID LEFT JOIN USER_DETAILS UD ON UD.USER_ID = QRS.UPDATED_BY ");
			sb.append(" WHERE RI.CAL_TYPE='Q' AND RI.PRODUCT_LEVEL_ID = 1 ");		
			sb.append(" AND QRS.STATUS IN ( ").append(RecommendationStatusLookup.APPROVED.getStatusId()).append(" , ");
			sb.append(RecommendationStatusLookup.EMERGENCY_APPROVED.getStatusId()).append(")");
			sb.append(" AND RI.RUN_ID IN (").append(String.join(",", stringConvertedRunIds)).append(")");
			if(emergency) {
				sb.append(" AND EX.PRICE_TYPE = '").append(Constants.EMERGENCY).append("'");
			}
			else {
				sb.append(" AND EX.PRICE_TYPE = '").append(Constants.NORMAL).append("'");
			}

			if ((priceExportType.equals(Constants.EMERGENCY_OR_HARDPART) && !emergency)
					|| priceExportType.equals(Constants.HARD_PART_ITEMS)) {
				sb.append(" AND IL.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			} else if ((priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR) && !emergency)
					|| priceExportType.equals(Constants.SALE_FLOOR_ITEMS)) {
				sb.append(" AND IL.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			} 
			if(currWeekEndDate!="") {
				sb.append(" AND TO_DATE(TO_CHAR(RI.REG_EFF_DATE,'MM/dd/yyyy'),'MM/dd/yyyy')=TO_DATE('"+currWeekEndDate+"','MM/dd/yyyy') )");
			}
			
			logger.debug(sb.toString());
			PreparedStatement stmt = null;
			ResultSet rs = null;
			try {

				stmt = conn.prepareStatement(sb.toString());
				rs = stmt.executeQuery();
				while (rs.next()) {
					PRItemDTO item = new PRItemDTO();

					item.setZoneType("R");
					item.setRunId(rs.getLong("RUN_ID"));
					item.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
					item.setItemCode(rs.getInt("PRODUCT_ID"));
					item.setChildLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
					item.setChildLocationId(rs.getInt("LOCATION_ID"));
					item.setItemType(rs.getString("ITEM_TYPE"));
					item.setRetLirId(rs.getInt("RET_LIR_ID"));
					item.setPriceZoneId(rs.getInt("PRICE_ZONE_ID"));
					item.setPriceZoneNo(rs.getString("ZONE_NUM"));
					item.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));

					MultiplePrice recommendedPrice = new MultiplePrice(rs.getInt("REC_REG_MULTIPLE"),rs.getDouble("REC_REG_PRICE"));
					item.setRecommendedRegPrice(recommendedPrice);

					MultiplePrice currentPrice = new MultiplePrice(rs.getInt("REG_MULTIPLE"),rs.getDouble("REG_PRICE"));
					item.setCurrentRegPrice(currentPrice);

					item.setRegEffDate(rs.getString("REG_EFF_DATE"));
					item.setPriceExportType(rs.getString("PRICE_TYPE"));

					item.setApprovedOn(rs.getString("APPROVED_ON"));
					item.setApprovedBy(rs.getString("UPDATED_BY"));
					item.setApproverName(rs.getString("FIRST_NAME") + " " + rs.getString("LAST_NAME"));

					item.setVdpRetail(item.getRecommendedRegPrice().getUnitPrice());
					item.setCoreRetail(rs.getDouble("CORE_RETAIL"));
					item.setImpact(rs.getDouble("PRC_CHANGE_IMPACT"));
					// item.setPredicted(rs.getString("PREDICTED"));
					item.setPredicted(rs.getString("PREDICTED"));

					item.setPartNumber(rs.getString("PART_NAME"));
					item.setOverrideRegMultiple(rs.getInt("OVERRIDE_REG_MULTIPLE"));
					item.setOverrideRegPrice(rs.getDouble("OVERRIDE_REG_PRICE"));
					
					item.setSF_RU_rank(rs.getInt("IMPACT_RANK"));
					item.setSF_week_rank(rs.getInt("WEEK_RANK"));
					item.setSF_export_rank(rs.getInt("EXPORT_RANK"));
					//commented for AI #19-b
					//taking exact impact instead of abs impact
					item.setTotal_Impact(rs.getDouble("TOTAL_IMPACT"));
					if(rs.getString("FAMILY") == null) {
						item.setFamilyName("");
					}else {
						item.setFamilyName(rs.getString("FAMILY"));
					}

					//Attribute priority added for AI #109
					item.setPriority(rs.getString("PRIORITY"));
					
					MultiplePrice overridePrice = new MultiplePrice(rs.getInt("OVERRIDE_REG_MULTIPLE"),	rs.getDouble("OVERRIDE_REG_PRICE"));
					item.setOverriddenRegularPrice(overridePrice);

					if (overridePrice.price > 0) {
						item.setDiffRetail(overridePrice.getUnitPrice() - currentPrice.getUnitPrice());
						double diffInPrice = round(item.getDiffRetail(), 2);
						item.setDiffRetail(diffInPrice);
						item.setVdpRetail(overridePrice.getUnitPrice());
					} else {
						item.setDiffRetail(recommendedPrice.getUnitPrice() - currentPrice.getUnitPrice());
						double diffInPrice = round(item.getDiffRetail(), 2);
						item.setDiffRetail(diffInPrice);
						item.setVdpRetail(recommendedPrice.getUnitPrice());
					}

					item.setZoneName(rs.getString("ZONE_NAME"));
					item.setRecommendationUnit(rs.getString("RU_NAME"));
					item.setRecommendationUnitId(rs.getInt("RU_ID"));
					
					boolean isItemRecommended = true;
					if (overridePrice.price > 0) {
						if (overridePrice.equals(currentPrice)) {
							isItemRecommended = false;
						}
					}else if(recommendedPrice.price > 0){
						//Added this condition for AI#17 enhancement.
						//Z1000 can have an impact even if no new price is recommended 
						if (recommendedPrice.equals(currentPrice) && item.getImpact()==0) {
							isItemRecommended = false;
						}
					}else {
						isItemRecommended = false;
					}
					
					if (isItemRecommended) {
						boolean validItem = checkMandatoryColumnValidationV3(item);
						if(validItem) {
							baseList.add(item);
						}
						else {
							nonCandidateItemList.add(item);
						}
					}
					else {
						nonCandidateItemList.add(item);
					}
				}
			} catch (Exception e) {
				throw new GeneralException("getBaseDataForExport() - Error getting recommneded items for runids", e);
			} finally {
				PristineDBUtil.close(rs);
				PristineDBUtil.close(stmt);
			}
		}

		logger.info(
				"getItemsFromApprovedRecommendations() - # of approved items in candidate list is: " + baseList.size());
		logger.info(
				"getItemsFromApprovedRecommendations() - # of approved items in non-candidate list is: " + nonCandidateItemList.size());
		if(nonCandidateItemList.size() > 0) {
			List<String> invalidRecords = new ArrayList<>();
			for (PRItemDTO item : nonCandidateItemList) {
				invalidRecords.add(item.getRunId() + "-" + item.getItemCode());
			}

			logger.debug("Non candidate RunId-item are : " + String.join(";", invalidRecords));
		}
		return baseList;
	}

	
	private boolean checkMandatoryColumnValidation(PriceExportDTO item) {					
		boolean status = true;

		if(item.getRecommendedRegPrice().getUnitPrice() <= 0 || item.getCurrentRegPrice().getUnitPrice() <= 0 || 
				(item.getApprovedOn() == null || item.getApprovedOn().isEmpty()) || 
				(item.getApprovedBy() == null || item.getApprovedBy().isEmpty()) ||
				(item.getApproverName() == null || item.getApproverName().isEmpty()) || 
				item.getVdpRetail() <= 0 || (item.getPredicted() == null || item.getPredicted().isEmpty()) || 
				(item.getItemType() == null || item.getItemType().isEmpty()) || 
				item.getVdpRetail() <= 0 || (item.getRecommendationUnit() == null || item.getRecommendationUnit().isEmpty())) {
			status = false;
		}
		return status;
	
	}
	
	private boolean checkMandatoryColumnValidationV3(PRItemDTO item) {					
		boolean status = true;

		if(item.getRecommendedRegPrice().getUnitPrice() <= 0 || item.getCurrentRegPrice().getUnitPrice() <= 0 || 
				(item.getApprovedOn() == null || item.getApprovedOn().isEmpty()) || 
				(item.getApprovedBy() == null || item.getApprovedBy().isEmpty()) ||
				(item.getApproverName() == null || item.getApproverName().isEmpty()) || 
				item.getVdpRetail() <= 0 || (item.getPredicted() == null || item.getPredicted().isEmpty()) || 
				(item.getItemType() == null || item.getItemType().isEmpty()) || 
				item.getVdpRetail() <= 0 || (item.getRecommendationUnit() == null || item.getRecommendationUnit().isEmpty())) {
			status = false;
		}
		return status;
	
	}

	public List<Long> getRunId(Connection conn, String priceExportType, boolean emergencyInHardPart,
			boolean emergencyInSaleFloor) throws GeneralException {
		List<Long> runIdList = new ArrayList<Long>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {

			StringBuffer sb = new StringBuffer();
			sb.append("SELECT DISTINCT(RUN_ID) AS RUN_ID FROM PR_PRICE_EXPORT WHERE PRICE_TYPE = ? ");
			logger.debug(sb.toString());
			stmt = conn.prepareStatement(sb.toString());

			if (priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)
					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)
					|| priceExportType.equals(Constants.EMERGENCY)) {
				stmt.setString(1, Constants.EMERGENCY);
			}
			rs = stmt.executeQuery();
			while (rs.next()) {
				runIdList.add(rs.getLong("RUN_ID"));
			}

		} catch (SQLException e) {
			throw new GeneralException("getRunId() - Error while getting runIds", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return runIdList;
	}

	public List<String> getStoresOfZones(Connection conn, int price_zone_id) {

		List<String> storeData = new ArrayList<String>();
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ").append(price_zone_id);
		sb.append(" AND ACTIVE_INDICATOR = 'Y' ");
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				storeData.add(rs.getString("COMP_STR_NO"));
			}
			// storeData.addAll(price_zone_id);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}

		return storeData;
	}

	public void updateExportItems(Connection conn, List<PRItemDTO> exportList) throws GeneralException {

		List<String> uniqueData = new ArrayList<>();
		List<PRItemDTO> distinctItems = new ArrayList<>();
		for (PRItemDTO data : exportList) {
			if (!uniqueData.contains(data.getRunId() + ";" + data.getItemCode())) {
				distinctItems.add(data);
				uniqueData.add(data.getRunId() + ";" + data.getItemCode());
			}
		}
		
		PreparedStatement statement = null;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PR_QUARTER_REC_ITEM SET IS_EXPORTED = 'Y' WHERE PRODUCT_ID = ?  ");
			sb.append(" AND PRODUCT_LEVEL_ID = ").append(Constants.ITEMLEVELID)
					.append(" AND RUN_ID = ? AND CAL_TYPE = 'Q' ");
			statement = conn.prepareStatement(sb.toString());
			logger.debug("update item table query: " + sb.toString());
			logger.debug("updateExportItems() - updating # of items - " + distinctItems.size());
			
			int counter = 0;
			for (PRItemDTO item : distinctItems) {
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, item.getItemCode());
				statement.setLong(++colIndex, item.getRunId());
				statement.addBatch();
				if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
					logger.debug("updateExportItems() - updating " + counter + " items");
					statement.executeBatch();
					statement.clearBatch();
					counter = 0;
				}
			}

			if (counter > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}

		} catch (SQLException e) {
			logger.error("updateExportItems() - Error updating export status for items");
			throw new GeneralException("updateExportItems() - Error updating export status for items", e);
		} finally {
			PristineDBUtil.close(statement);
		}

	}
	
	public void updateExportItemsV3(Connection conn, List<PriceExportDTO> exportList) throws GeneralException {

		List<String> uniqueData = new ArrayList<>();
		List<PriceExportDTO> distinctItems = new ArrayList<>();
		for (PriceExportDTO data : exportList) {
			if (!uniqueData.contains(data.getRunId() + ";" + data.getItemCode())) {
				distinctItems.add(data);
				uniqueData.add(data.getRunId() + ";" + data.getItemCode());
			}
		}
		
		PreparedStatement statement = null;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PR_QUARTER_REC_ITEM SET IS_EXPORTED = 'Y' WHERE PRODUCT_ID = ?  ");
			sb.append(" AND PRODUCT_LEVEL_ID = ").append(Constants.ITEMLEVELID)
					.append(" AND RUN_ID = ? AND CAL_TYPE = 'Q' ");
			statement = conn.prepareStatement(sb.toString());
			logger.debug("update item table query: " + sb.toString());
			logger.debug("updateExportItems() - updating # of items - " + distinctItems.size());
			
			int counter = 0;
			for (PriceExportDTO item : distinctItems) {
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, item.getItemCode());
				statement.setLong(++colIndex, item.getRunId());
				statement.addBatch();
				if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
					logger.debug("updateExportItems() - updating " + counter + " items");
					statement.executeBatch();
					statement.clearBatch();
					counter = 0;
				}
			}

			if (counter > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}

		} catch (SQLException e) {
			logger.error("updateExportItems() - Error updating export status for items");
			throw new GeneralException("updateExportItems() - Error updating export status for items", e);
		} finally {
			PristineDBUtil.close(statement);
		}

	}
	
	//Updated by Karishma on 03/29/22 
	//Issue fix for  setting incorrect export status for RU's that are not exported 
	public void updateExportStatusV3(Connection conn, List<PriceExportDTO> finalExportList, List<Long> runIdList, HashMap<Long, Integer> actualItemsCtFromExportQueue)
			throws GeneralException {

		if(finalExportList.size() > 0 && runIdList.size() > 0) {
		PreparedStatement stmt = null;
		int statusCode;
		try {

			StringBuffer sb = new StringBuffer();

			sb.append(" UPDATE PR_QUARTER_REC_HEADER SET EXPORTED = SYSDATE, ");
			sb.append(" EXPORTED_BY = ?, STATUS_DATE = SYSDATE, STATUS = ?, STATUS_ROLE = 0, ");
			sb.append(" STATUS_BY = ? WHERE RUN_ID = ? ");
			
			logger.debug("updateExportStatusV3 qry:"+ sb.toString());
			stmt = conn.prepareStatement(sb.toString());
			int counter = 0;

			//get remainingItems in queue
			HashMap<Long, Integer> countByRunId = getNotExportedCountForRunIdsV3(conn, runIdList);
			
			HashMap<Long, Integer> runIdStatusCt = new HashMap<>();
			
			// For all the run ids that were to be processed in the current run, check if
			// that exists in the
			// countByRunId .If the id is not present then its exported completly else
			// compare the current and initial
			// count which should not be equal then mark it as exported partially

				for (Long runId : runIdList) {
					if (actualItemsCtFromExportQueue.get(runId) != null) {
						// partially exported items
						if (countByRunId.containsKey(runId) && countByRunId.get(runId) != 0) {

							int actualItemsCount = actualItemsCtFromExportQueue.get(runId);
							int count = countByRunId.get(runId);
							if (actualItemsCount != count) {
								statusCode = PRConstants.STATUS_PARTIALLY_EXPORTED;

								runIdStatusCt.put(runId, statusCode);
							}

						} else

						{
							statusCode = PRConstants.EXPORT_STATUS;
							runIdStatusCt.put(runId, statusCode);
						}
					}
				}
				
				if (runIdStatusCt.size() > 0) {
					for (Map.Entry<Long, Integer> entry : runIdStatusCt.entrySet()) {
						counter++;
						int colIndex = 0;
						stmt.setString(++colIndex, PRConstants.BATCH_USER);
						stmt.setInt(++colIndex, entry.getValue());
						stmt.setString(++colIndex, PRConstants.BATCH_USER);
						stmt.setLong(++colIndex, entry.getKey());

						stmt.addBatch();
						if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
							stmt.executeBatch();
							stmt.clearBatch();
							counter = 0;
						}

					}

					if (counter > 0) {
						stmt.executeBatch();
						stmt.clearBatch();
					}
				}

		} catch (Exception e) {
			logger.error("updateExportStatusV3() - Error while updating the status in PR_QUARTER_REC_HEADER ");
			throw new GeneralException("updateExportStatusV3() - Error while updating the status in PR_QUARTER_REC_HEADER: ", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		}
	}

	public void updateExportStatus(Connection conn, List<PRItemDTO> finalExportList, List<Long> runIdList)
			throws GeneralException {

		if(finalExportList.size() > 0 && runIdList.size() > 0) {
		PreparedStatement stmt = null;
		int statusCode;
		try {

			StringBuilder runIdStr = new StringBuilder();
			for (int i = 0; i < runIdList.size(); i++) {
				long runId = runIdList.get(i);
				if (i == runIdList.size() - 1) {
					runIdStr.append(runId);
				} else {
					runIdStr.append(runId).append(",");
				}
			}

			StringBuffer sb = new StringBuffer();

			sb.append(" UPDATE PR_QUARTER_REC_HEADER SET EXPORTED = SYSDATE, ");
			sb.append(" EXPORTED_BY = ?, STATUS_DATE = SYSDATE, STATUS = ?, STATUS_ROLE = 0, ");
			sb.append(" STATUS_BY = ? WHERE RUN_ID = ? ");
			/*
			 * sb.append(" STATUS_BY = ? WHERE RUN_ID IN (").append(runIdStr.toString());
			 * sb.append(")");
			 */
			logger.debug(sb.toString());
			stmt = conn.prepareStatement(sb.toString());
			int counter = 0;

			HashMap<Long, Integer> countByRunId = getNotExportedCountForRunIds(conn, runIdList, finalExportList);

			for (Map.Entry<Long, Integer> countEntry : countByRunId.entrySet()) {
				long runId = countEntry.getKey();
				int count = countByRunId.get(runId);

				if (count > 0) {
					statusCode = PRConstants.STATUS_PARTIALLY_EXPORTED;
					logger.debug("updateExportStatus() - Export Status: Exported Partially, status code = "
							+ PRConstants.STATUS_PARTIALLY_EXPORTED);

				} else {
					statusCode = PRConstants.EXPORT_STATUS;
					logger.debug("updateExportStatus() - Export Status: Exported, status code = "
							+ PRConstants.EXPORT_STATUS);

				}

				counter++;
				int colIndex = 0;
				stmt.setString(++colIndex, PRConstants.BATCH_USER);
				stmt.setInt(++colIndex, statusCode);
				stmt.setString(++colIndex, PRConstants.BATCH_USER);
				stmt.setLong(++colIndex, runId);

				stmt.addBatch();
				if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					counter = 0;
				}

				if (counter > 0) {
					stmt.executeBatch();
					stmt.clearBatch();
				}
			}
		} catch (Exception e) {
			logger.error("updateExportStatus() - error while updating the status in summary ");
			throw new GeneralException("updateExportStatus() - error while updating the status in summary: ", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		}
	}

	public void insertExportStatus(Connection conn, List<Long> runIdList,
			List<PRItemDTO> itemsFiltered) throws GeneralException {

		if(itemsFiltered.size() > 0 && runIdList.size() > 0) {
		PreparedStatement stmt = null;
		int statusCode;
		String exportStatus = "";

		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" INSERT INTO PR_QUARTER_REC_STATUS (RECOMMENDATION_STATUS_ID, RUN_ID, ");
			sb.append(
					" STATUS, UPDATED_BY, UPDATED, STATUS_ROLE, MESSAGE)  VALUES (RECOMMENDATION_STATUS_ID_SEQ.NEXTVAL, ");
			sb.append(" ?, ?, ?, SYSDATE, 0, ?)");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("Query for insertExportStatus() - " + sb.toString());
			int itemCountInBatch = 0;

			HashMap<Long, Integer> countByRunId = getNotExportedCountForRunIds(conn, runIdList, itemsFiltered);

			for (Map.Entry<Long, Integer> countEntry : countByRunId.entrySet()) {
				long runId = countEntry.getKey();
				int count = countByRunId.get(runId);

				if (count > 0) {
					statusCode = PRConstants.STATUS_PARTIALLY_EXPORTED;
					exportStatus = "Partially Exported";
				} else {
					statusCode = PRConstants.EXPORT_STATUS;
					exportStatus = "Exported";
				}

				itemCountInBatch++;
				int colIndex = 0;
				stmt.setLong(++colIndex, runId);
				stmt.setInt(++colIndex, statusCode);
				stmt.setString(++colIndex, PRConstants.BATCH_USER);
				stmt.setString(++colIndex, exportStatus);
				stmt.addBatch();

			}
			if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
				stmt.executeBatch();
				stmt.clearBatch();
				itemCountInBatch = 0;
			}

			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("insertExportStatus() - Error inserting export status for items");
			throw new GeneralException("insertExportStatus() - Error inserting export status for items", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		}
		// stmt.executeUpdate();

	}

	// Updated by karishma on 03/29/22
	// Issue fix for setting the correct export status
	public void insertExportStatusV3(Connection conn, List<Long> runIdList, List<PriceExportDTO> itemsFiltered,
			HashMap<Long, Integer> actualItemsCtFromExportQueue) throws GeneralException {

		if (itemsFiltered.size() > 0 && runIdList.size() > 0) {
			PreparedStatement stmt = null;
			int statusCode;
			String exportStatus = "";
			HashMap<Long, String> runIdStatusCt = new HashMap<>();

			try {
				StringBuilder sb = new StringBuilder();
				sb.append(" INSERT INTO PR_QUARTER_REC_STATUS (RECOMMENDATION_STATUS_ID, RUN_ID, ");
				sb.append(
						" STATUS, UPDATED_BY, UPDATED, STATUS_ROLE, MESSAGE)  VALUES (RECOMMENDATION_STATUS_ID_SEQ.NEXTVAL, ");
				sb.append(" ?, ?, ?, SYSDATE, 0, ?)");

				stmt = conn.prepareStatement(sb.toString());
				logger.debug("Query for insertExportStatus() - " + sb.toString());

				// get the existing count of items by RunID from Export Que Table
				HashMap<Long, Integer> countByRunId = getNotExportedCountForRunIdsV3(conn, runIdList);

				// For all the run ids that were to be processed in the current run, check if
				// that exists in the
				// countByRunId .If the id is not present then its exported completly else
				// compare the current and initial
				// count which should not be equal then mark it as exported partially

				for (Long runId : runIdList) {
					if (actualItemsCtFromExportQueue.get(runId) != null) {
						// partially exported items
						if (countByRunId.containsKey(runId) && countByRunId.get(runId) != 0) {

							int actualItemsCount = actualItemsCtFromExportQueue.get(runId);
							int count = countByRunId.get(runId);
							if (actualItemsCount != count) {
								statusCode = PRConstants.STATUS_PARTIALLY_EXPORTED;
								exportStatus = "Partially Exported";
								runIdStatusCt.put(runId, statusCode + "-" + exportStatus);
							}
						} else {
							statusCode = PRConstants.EXPORT_STATUS;
							exportStatus = "Exported";
							runIdStatusCt.put(runId, statusCode + "-" + exportStatus);
						}
					}
				}

				if (runIdStatusCt.size() > 0) {
					int counter = 0;
					for (Map.Entry<Long, String> entry : runIdStatusCt.entrySet()) {
						counter++;
						int colIndex = 0;
						stmt.setLong(++colIndex, entry.getKey());
						stmt.setInt(++colIndex, Integer.valueOf(entry.getValue().split("-")[0]));
						stmt.setString(++colIndex, PRConstants.BATCH_USER);
						stmt.setString(++colIndex, entry.getValue().split("-")[1]);
						stmt.addBatch();
						if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
							stmt.executeBatch();
							stmt.clearBatch();
							counter = 0;
						}
					}
					if (counter > 0) {
						stmt.executeBatch();
						stmt.clearBatch();
					}
				}

			} catch (SQLException e) {
				logger.error("insertExportStatus() - Error inserting export status for items");
				throw new GeneralException("insertExportStatus() - Error inserting export status for items", e);
			} finally {
				PristineDBUtil.close(stmt);
			}
		}
	}

	public HashMap<Integer, List<PRItemDTO>> getItemListInAllLocations(Connection conn, String priceExportType,
			boolean emergencyInHardPart, boolean emergencyInSaleFloor) throws GeneralException {

		logger.debug("getting disapproved items from other locations..");

		HashMap<Integer, List<PRItemDTO>> disApprovedItemListInOtherLocations = new HashMap<Integer, List<PRItemDTO>>();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT QRI.PRODUCT_ID, OTHER_LOC.LOCATION_LEVEL_ID, OTHER_LOC.LOCATION_ID, QRI.REG_PRICE, ");
		sb.append(" QRI.OVERRIDE_REG_PRICE, QRI.OVERRIDE_REG_MULTIPLE, ");
		sb.append(" QRI.REG_MULTIPLE, IL.USER_ATTR_6 FROM PR_QUARTER_REC_ITEM QRI ");
		sb.append(" JOIN (SELECT PRH_OTHERS.LOCATION_ID, PRH_OTHERS.LOCATION_LEVEL_ID, ");
		sb.append(" MAX(PRH_OTHERS.RUN_ID) RUN_ID FROM PR_QUARTER_REC_HEADER PRH_OTHERS ");
		sb.append(" JOIN PR_QUARTER_REC_HEADER  PRH_BASE ON PRH_OTHERS.PRODUCT_LEVEL_ID = PRH_BASE.PRODUCT_LEVEL_ID ");
		sb.append(" AND PRH_OTHERS.PRODUCT_ID = PRH_BASE.PRODUCT_ID ");
		sb.append(" AND PRH_OTHERS.ACTUAL_START_CALENDAR_ID = PRH_BASE.ACTUAL_START_CALENDAR_ID ");
		sb.append(" AND PRH_OTHERS.ACTUAL_END_CALENDAR_ID = PRH_BASE.ACTUAL_END_CALENDAR_ID ");
		sb.append(" AND PRH_BASE.RUN_ID IN (SELECT DISTINCT RUN_ID FROM PR_PRICE_EXPORT) ");
		sb.append(" WHERE PRH_OTHERS.STATUS NOT in (").append(RecommendationStatusLookup.APPROVED.getStatusId())
				.append(",");
		sb.append(RecommendationStatusLookup.EMERGENCY_APPROVED.getStatusId()).append(",");
		sb.append(RecommendationStatusLookup.EXPORTED.getStatusId()).append(",")
				.append(RecommendationStatusLookup.EXPORTED_PARTIALLY.getStatusId()).append(") ");
		sb.append(" GROUP BY PRH_OTHERS.LOCATION_ID, PRH_OTHERS.LOCATION_LEVEL_ID) OTHER_LOC ON ");
		sb.append(" OTHER_LOC.RUN_ID = QRI.RUN_ID LEFT JOIN ITEM_LOOKUP IL ON QRI.PRODUCT_ID = IL.ITEM_CODE ");
		sb.append(
				" WHERE QRI.CAL_TYPE = 'Q' AND OTHER_LOC.LOCATION_ID <> (SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE GLOBAL_ZONE = 'Y')");

		if (priceExportType.equals(Constants.EMERGENCY_OR_HARDPART) && !emergencyInHardPart) {
			sb.append(" AND IL.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
		} else if (priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR) && !emergencyInSaleFloor) {
			sb.append(" AND IL.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
		}

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(sb.toString());
			// stmt.setString(1, itemType);
			logger.debug("Query for getItemListInAllLocations() - " + sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {

				List<PRItemDTO> priceList = new ArrayList<PRItemDTO>();
				PRItemDTO dto = new PRItemDTO();
				int itemCode = rs.getInt("PRODUCT_ID");
				MultiplePrice recommendedPrice = new MultiplePrice(rs.getInt("REG_MULTIPLE"),
						rs.getDouble("REG_PRICE"));
				dto.setRecommendedRegPrice(recommendedPrice);
				dto.setPriceZoneId(rs.getInt("LOCATION_ID"));
				dto.setChildLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
				MultiplePrice overridenPrice = new MultiplePrice(rs.getInt("OVERRIDE_REG_MULTIPLE"),
						rs.getDouble("OVERRIDE_REG_PRICE"));
				dto.setOverriddenRegularPrice(overridenPrice);

				if (disApprovedItemListInOtherLocations.containsKey(itemCode)) {
					priceList = disApprovedItemListInOtherLocations.get(itemCode);
				}

				priceList.add(dto);
				disApprovedItemListInOtherLocations.put(itemCode, priceList);

			}
		} catch (SQLException e) {
			logger.error("getItemListInAllLocations() - Error when getting items from all zones");
			throw new GeneralException(
					"getItemListInAllLocations() - Error when getting items from all zones - " + e.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}

		return disApprovedItemListInOtherLocations;
	}

	public HashMap<String, Integer> getZoneIdMap(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String chainId = PropertyManager.getProperty("PRESTO_SUBSCRIBER");
		HashMap<String, Integer> zoneIdMap = new HashMap<String, Integer>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID ");
			db.append(" IN (SELECT DISTINCT(PRICE_ZONE_ID) FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ")
					.append(chainId).append(")");
			stmt = conn.prepareStatement(db.toString());
			logger.debug("Query for getZoneIdMap() - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneIdMap.put((rs.getString("ZONE_NUM")), rs.getInt("PRICE_ZONE_ID"));
			}
		} catch (SQLException ex) {
			logger.error("getZoneIdMap() - Error when retrieving zone id map - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneIdMap;
	}
	
	
	public HashMap<String, Integer> getZonesPartOfGlobalZone(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String chainId = PropertyManager.getProperty("PRESTO_SUBSCRIBER");
		HashMap<String, Integer> zoneIdMap = new HashMap<String, Integer>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID ");
			db.append(" IN (SELECT DISTINCT(PRICE_ZONE_ID) FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ")
					.append(chainId)
					.append(" AND PRICE_ZONE_ID_3 = (SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE GLOBAL_ZONE = 'Y'))");
			stmt = conn.prepareStatement(db.toString());
			logger.debug("Query for getZoneIdMap() - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneIdMap.put((rs.getString("ZONE_NUM")), rs.getInt("PRICE_ZONE_ID"));
			}
		} catch (SQLException ex) {
			logger.error("getZoneIdMap() - Error when retrieving zone id map - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneIdMap;
	}

	public HashMap<String, Integer> getZoneIdMapForVirtualZone(Connection conn, String virtualZoneNum, List<String> excludeZones) {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		HashMap<String, Integer> zoneIdMap = new HashMap<String, Integer>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE GLOBAL_ZONE <> 'Y'");
			db.append(" AND ZONE_NUM <> '").append(virtualZoneNum).append("'");
			//filter added on 02/04/2021 - kirthi
			//to exclude Test zones and excluded zones of AZ for zone 30 price calculation.
			db.append(" AND ZONE_TYPE <> 'T' ");	
			db.append(" AND PRICE_ZONE_ID NOT IN (").append(String.join(",", excludeZones)).append(")");
			//added by kirthi 
			//to filter active zones
			db.append(" AND ACTIVE_INDICATOR = 'Y' ");

			stmt = conn.prepareStatement(db.toString());
			logger.debug("getZoneIdMapForVirtualZone() query - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneIdMap.put((rs.getString("ZONE_NUM")), rs.getInt("PRICE_ZONE_ID"));
			}
		} catch (SQLException ex) {
			logger.error("getZoneIdMapForVirtualZone() - Error when retrieving zone id map for virtual zone - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneIdMap;
	}

	public String getZoneNameForVirtualZone(Connection conn, String zoneNum) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		String zoneName = null;
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT name FROM RETAIL_PRICE_ZONE where zone_num = '").append(zoneNum).append("'");

			stmt = conn.prepareStatement(db.toString());
			logger.debug("Query for getZoneNameForVirtualZone() - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneName = rs.getString("name");
			}
		} catch (SQLException ex) {
			logger.error("getZoneNameForVirtualZone() - Error when getting zone name ");
			throw new GeneralException("getZoneNameForVirtualZone() - Error when getting zone name - ", ex);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneName;
	}

	public HashMap<String, Integer> getExcludedStoresFromItemList(Connection conn, int itemCode, int zoneId,
			boolean expiryOnFutureDate) {

		String priceCheckListIdStr = PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID");
		int priceCheckListId = Integer.parseInt(priceCheckListIdStr);

		// HashMap<String, List<String>> excludedStoreNumFromItemList = new
		// HashMap<String, List<String>>();

		HashMap<String, Integer> mergedExcludeStoreList = new HashMap<String, Integer>();

		HashMap<String, Integer> excludeStoreListOne = getExcludeStoresByItemsAndStoreLock(conn, itemCode, zoneId,
				priceCheckListId, expiryOnFutureDate);

		HashMap<String, Integer> excludeStoreListTwo = getExcludeStoresByItemsAndStoreLockAndLocationLevelId(conn,
				itemCode, zoneId, priceCheckListId, expiryOnFutureDate);

		// logger.debug("# of records in excludeStoreListTwo: " +
		// excludeStoreListTwo.size());

		if (excludeStoreListOne.size() > 0 && excludeStoreListTwo.size() == 0) {
			mergedExcludeStoreList.putAll(excludeStoreListOne);
			return mergedExcludeStoreList;
		}

		else if (excludeStoreListTwo.size() > 0 && excludeStoreListOne.size() == 0) {
			mergedExcludeStoreList.putAll(excludeStoreListTwo);
			return mergedExcludeStoreList;
		}

		else if (excludeStoreListOne.size() > 0 && excludeStoreListTwo.size() > 0) {

			excludeStoreListOne.forEach((store, itemcode) -> {
				mergedExcludeStoreList.put(store, itemcode);
			});
			excludeStoreListTwo.forEach((store, itemcode) -> {
				mergedExcludeStoreList.put(store, itemcode);
			});

		}
		return mergedExcludeStoreList;

	}

	private HashMap<String, Integer> getExcludeStoresByItemsAndStoreLockAndLocationLevelId(Connection conn,
			int itemCode, int zoneId, int priceCheckListId, boolean expiryOnFutureDate) {

		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, Integer> excludeStoreList = new HashMap<String, Integer>();
		try {
			StringBuffer sb = new StringBuffer();

			sb.append(
					" select cs.comp_str_no as store_num, pcli.item_code as item_code from price_check_list_items pcli ");
			sb.append(" left join price_check_list pcl on pcli.PRICE_CHECK_LIST_ID = pcl.ID ");
			sb.append(" left join location_group_relation lgr on pcl.location_level_id = lgr.location_level_id ");
			sb.append("  and pcl.location_id = lgr.location_id ");
			sb.append(" left join competitor_store cs on lgr.child_location_id = cs.comp_str_id ");
			sb.append(" where pcli.item_code in ( ").append(itemCode).append(")");
			sb.append(" and lgr.child_location_id in (select comp_str_id from competitor_store where price_zone_id = ")
					.append(zoneId).append(")");
			sb.append(" and pcl.PRICE_CHECK_LIST_TYPE_ID = ").append(priceCheckListId);
			sb.append(" and lgr.location_level_id = ").append(Constants.STORE_LIST_LEVEL_ID);

			if (expiryOnFutureDate) {
				sb.append(
						" and ((PCLI.END_DATE > TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy') ");
				sb.append(" or PCLI.END_DATE IS NULL) AND (PCLI.IS_EXPORTED = 'N' or PCLI.IS_EXPORTED IS NULL))");
			} 

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getExcludeStoresByItemsAndStoreLockAndLocationLevelId() - query: " + sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				excludeStoreList.put(rs.getString("store_num"), rs.getInt("item_code"));
			}
		} catch (SQLException ex) {
			logger.error(
					"getExcludeStoresByItemsAndStoreLockAndLocationLevelId() - Error when getting excluded stores from itemList - "
							+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return excludeStoreList;
	}

	private HashMap<String, Integer> getExcludeStoresByItemsAndStoreLock(Connection conn, int itemCode, int zoneId,
			int priceCheckListId, boolean expiryOnFutureDate) {

		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, Integer> excludeStoreList = new HashMap<String, Integer>();
		try {
			StringBuffer sb = new StringBuffer();

			sb.append(
					" select cs.comp_str_no as store_num, pcli.ITEM_CODE as ITEM_CODE, pcli.end_date from price_check_list_items pcli left join competitor_store cs ")
					.append(" on pcli.store_id = cs.comp_str_id where pcli.item_code in ('").append(itemCode)
					.append("')");
			sb.append(" and pcli.store_id in (select comp_str_id from competitor_store where price_zone_id = ")
					.append(zoneId).append(")");
			sb.append(
					" and pcli.PRICE_CHECK_LIST_ID in (select id from price_check_list where PRICE_CHECK_LIST_TYPE_ID = ")
					.append(priceCheckListId).append(") ");

			if (expiryOnFutureDate) {
				sb.append(
						" and ((PCLI.END_DATE > TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy') ");
				sb.append(
						" or PCLI.END_DATE IS NULL) AND (PCLI.IS_EXPORTED = 'N' or PCLI.IS_EXPORTED IS NULL)) ");
			}

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getExcludeStoresByItemsAndStoreLock() - Query: " + sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {

				excludeStoreList.put(rs.getString("store_num"), rs.getInt("ITEM_CODE"));

			}
		} catch (SQLException ex) {
			logger.error("getExcludeStoresByItemsAndStoreLock() - Error when getting excluded stores from itemList - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return excludeStoreList;
	}

	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		long factor = (long) Math.pow(10, places);
		value = value * factor;
		long tmp = Math.round(value);
		return (double) tmp / factor;
	}

	/**
	 * 
	 * @param conn
	 * @param runIds
	 * @param itemsFiltered
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Long, Integer> getNotExportedCountForRunIds(Connection conn, List<Long> runIds,
			List<PRItemDTO> itemsFiltered) throws GeneralException {
		HashMap<Long, Integer> notExportedCountMap = new HashMap<Long, Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			StringBuilder runIdStr = new StringBuilder();
			for (int i = 0; i < runIds.size(); i++) {
				long runId = runIds.get(i);
				if (i == runIds.size() - 1) {
					runIdStr.append(runId);
				} else {
					runIdStr.append(runId).append(",");
				}
			}

			StringBuilder sb = new StringBuilder();
			sb.append("SELECT RUN_ID, COUNT(*) AS COUNT FROM PR_PRICE_EXPORT WHERE ");
			sb.append(" RUN_ID IN (").append(runIdStr.toString()).append(") GROUP BY RUN_ID");
			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query of - getNotExportedCountForRunIds() : " + sb.toString());
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				notExportedCountMap.put(resultSet.getLong("RUN_ID"), resultSet.getInt("COUNT"));
			}

			for (long runId : runIds) {
				if (!notExportedCountMap.containsKey(runId)) {
					notExportedCountMap.put(runId, 0);
				}
			}
		} catch (SQLException e) {
			logger.error("getNotExportedCountForRunIds() - Error while getting export status of items", e);
			throw new GeneralException("getNotExportedCountForRunIds() - Error while getting export status of items",
					e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return notExportedCountMap;
	}
	
	/**
	 * 
	 * @param conn
	 * @param runIds
	 * @param itemsFiltered
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Long, Integer> getNotExportedCountForRunIdsV3(Connection conn, List<Long> runIds) throws GeneralException {
		HashMap<Long, Integer> notExportedCountMap = new HashMap<Long, Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			StringBuilder runIdStr = new StringBuilder();
			for (int i = 0; i < runIds.size(); i++) {
				long runId = runIds.get(i);
				if (i == runIds.size() - 1) {
					runIdStr.append(runId);
				} else {
					runIdStr.append(runId).append(",");
				}
			}

			StringBuilder sb = new StringBuilder();
			sb.append("SELECT RUN_ID, COUNT(*) AS COUNT FROM PR_PRICE_EXPORT WHERE ");
			sb.append(" RUN_ID IN (").append(runIdStr.toString()).append(") GROUP BY RUN_ID");
			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query of - getNotExportedCountForRunIds() : " + sb.toString());
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				notExportedCountMap.put(resultSet.getLong("RUN_ID"), resultSet.getInt("COUNT"));
			}

			for (long runId : runIds) {
				if (!notExportedCountMap.containsKey(runId)) {
					notExportedCountMap.put(runId, 0);
				}
			}
		} catch (SQLException e) {
			logger.error("getNotExportedCountForRunIds() - Error while getting export status of items", e);
			throw new GeneralException("getNotExportedCountForRunIds() - Error while getting export status of items",
					e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return notExportedCountMap;
	}

	public void updateExportLigItems(Connection conn, List<PRItemDTO> itemsFiltered) throws GeneralException {
		PreparedStatement statement = null;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PR_QUARTER_REC_ITEM SET IS_EXPORTED = 'Y' WHERE PRODUCT_ID = ?  ");
			sb.append(" AND PRODUCT_LEVEL_ID = ").append(Constants.PRODUCT_LEVEL_ID_LIG)
					.append(" AND RUN_ID = ? AND CAL_TYPE = 'Q'");
			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query for updateExportLigItems() - " + sb.toString());
			int counter = 0;
			HashMap<Integer, List<PRItemDTO>> groupByLIR = (HashMap<Integer, List<PRItemDTO>>) itemsFiltered.stream()
					.filter(p -> p.getRetLirId() > 0).collect(Collectors.groupingBy(PRItemDTO::getRetLirId));

			for (Map.Entry<Integer, List<PRItemDTO>> lirEntry : groupByLIR.entrySet()) {
				int lirId = lirEntry.getKey();
				long runId = lirEntry.getValue().get(0).getRunId();
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, lirId);
				statement.setLong(++colIndex, runId);
				statement.addBatch();
				if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					counter = 0;
				}
			}
			if (counter > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}

		} catch (SQLException e) {
			logger.error("updateExportLigItems() - Error updating export status for lig items");
			throw new GeneralException("updateExportLigItems() - Error updating export status for lig items");
		} finally {
			PristineDBUtil.close(statement);
		}

	}

	public void updateExportLigItemsV3(Connection conn, List<PriceExportDTO> itemsFiltered) throws GeneralException {
		PreparedStatement statement = null;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PR_QUARTER_REC_ITEM SET IS_EXPORTED = 'Y' WHERE PRODUCT_ID = ?  ");
			sb.append(" AND PRODUCT_LEVEL_ID = ").append(Constants.PRODUCT_LEVEL_ID_LIG)
					.append(" AND RUN_ID = ? AND CAL_TYPE = 'Q'");
			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query for updateExportLigItems() - " + sb.toString());
			int counter = 0;
			HashMap<Integer, List<PriceExportDTO>> groupByLIR = (HashMap<Integer, List<PriceExportDTO>>) itemsFiltered.stream()
					.filter(p -> p.getRetLirId() > 0).collect(Collectors.groupingBy(PriceExportDTO::getRetLirId));

			for (Map.Entry<Integer, List<PriceExportDTO>> lirEntry : groupByLIR.entrySet()) {
				int lirId = lirEntry.getKey();
				long runId = lirEntry.getValue().get(0).getRunId();
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, lirId);
				statement.setLong(++colIndex, runId);
				statement.addBatch();
				if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					counter = 0;
				}
			}
			if (counter > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}

		} catch (SQLException e) {
			logger.error("updateExportLigItems() - Error updating export status for lig items");
			throw new GeneralException("updateExportLigItems() - Error updating export status for lig items");
		} finally {
			PristineDBUtil.close(statement);
		}

	}

	
	public void deleteProcessedRunIds(Connection conn, List<PRItemDTO> itemsFiltered) throws GeneralException {
		Set<PRItemDTO> uniqueData = new HashSet<>();
		//List<PRItemDTO> distinctItems = new ArrayList<>();
		for (PRItemDTO data : itemsFiltered) {
			uniqueData.add(new PRItemDTO(data.getRunId(), data.getItemCode()));
			/*if (!uniqueData.contains(data.getRunId() + ";" + data.getItemCode())) {
				distinctItems.add(data);
				
			}*/
		}
		PreparedStatement statement = null;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" DELETE FROM PR_PRICE_EXPORT WHERE RUN_ID = ? AND ITEM_CODE = ? ");

			statement = conn.prepareStatement(sb.toString());
			logger.debug("deleteProcessedRunIds() - delete query: " + sb.toString());

			int counter = 0;
			
			for (PRItemDTO item : uniqueData) {
				int colIndex = 0;
				counter++;

				statement.setLong(++colIndex, item.getRunId());
				statement.setInt(++colIndex, item.getItemCode());
				statement.addBatch();
				if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					counter = 0;
				}
			}

			if (counter > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
			
			List<String> tempRunIdList = new ArrayList<>();
			for(Long runIds : itemsFiltered.stream().collect(Collectors.groupingBy(PRItemDTO :: getRunId)).keySet()) {
				tempRunIdList.add(String.valueOf(runIds));
			}
			List<String> tempItemList = new ArrayList<>();
			for(int items : itemsFiltered.stream().collect(Collectors.groupingBy(PRItemDTO :: getItemCode)).keySet()) {
				tempItemList.add(String.valueOf(items));
			}
			logger.debug("Run ids deleted: " + String.join(",",tempRunIdList));
			logger.debug("Items deleted: " + String.join(",",tempItemList));

		} catch (SQLException e) {
			logger.error("deleteProcessedRunIds() - Error deleting the items");
			throw new GeneralException("deleteProcessedRunIds() - Error deleting the items");
		} finally {
			PristineDBUtil.close(statement);
		}

	}
	
	public void deleteProcessedRunIdsV3(Connection conn, List<PriceExportDTO> itemsFiltered) throws GeneralException {
		Set<PriceExportDTO> uniqueData = new HashSet<>();
		//List<PRItemDTO> distinctItems = new ArrayList<>();
		for (PriceExportDTO data : itemsFiltered) {
			uniqueData.add(new PriceExportDTO(data.getRunId(), data.getItemCode()));
			/*if (!uniqueData.contains(data.getRunId() + ";" + data.getItemCode())) {
				distinctItems.add(data);
				
			}*/
		}
		PreparedStatement statement = null;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" DELETE FROM PR_PRICE_EXPORT WHERE RUN_ID = ? AND ITEM_CODE = ? ");

			statement = conn.prepareStatement(sb.toString());
			logger.debug("deleteProcessedRunIds() - delete query: " + sb.toString());

			int counter = 0;
			
			for (PriceExportDTO item : uniqueData) {
				int colIndex = 0;
				counter++;

				statement.setLong(++colIndex, item.getRunId());
				statement.setInt(++colIndex, item.getItemCode());
				statement.addBatch();
				if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					counter = 0;
				}
			}

			if (counter > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
			
			List<String> tempRunIdList = new ArrayList<>();
			for(Long runIds : itemsFiltered.stream().collect(Collectors.groupingBy(PriceExportDTO :: getRunId)).keySet()) {
				tempRunIdList.add(String.valueOf(runIds));
			}
			List<String> tempItemList = new ArrayList<>();
			for(int items : itemsFiltered.stream().collect(Collectors.groupingBy(PriceExportDTO :: getItemCode)).keySet()) {
				tempItemList.add(String.valueOf(items));
			}
			logger.debug("Run ids deleted: " + String.join(",",tempRunIdList));
			logger.debug("Items deleted: " + String.join(",",tempItemList));

		} catch (SQLException e) {
			logger.error("deleteProcessedRunIds() - Error deleting the items");
			throw new GeneralException("deleteProcessedRunIds() - Error deleting the items");
		} finally {
			PristineDBUtil.close(statement);
		}

	}

	public List<Long> getRunIdListForESandEHItems(Connection conn) throws GeneralException {
		List<Long> runIdList = new ArrayList<Long>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {

			StringBuffer sb = new StringBuffer();
			sb.append("SELECT DISTINCT(RUN_ID) FROM PR_PRICE_EXPORT WHERE PRICE_TYPE = 'N' ");
			logger.debug(sb.toString());
			stmt = conn.prepareStatement(sb.toString());

			rs = stmt.executeQuery();
			while (rs.next()) {
				runIdList.add(rs.getLong("RUN_ID"));
			}

		} catch (SQLException e) {
			throw new GeneralException(
					"getRunIdListForESandEHItems() - Error while getting runIds hardPart/salesfloor or emergency", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return runIdList;
	}

	public HashMap<Long, Integer> getRunIdWithStatusCode(Connection conn, List<Long> runIdList)
			throws GeneralException {
		HashMap<Long, Integer> runIdAndStatusCode = new HashMap<Long, Integer>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			/*
			 * StringBuilder runIdStr = new StringBuilder(); for (int i = 0; i <
			 * runIdList.size(); i++) { long runId = runIdList.get(i); if (i ==
			 * runIdList.size() - 1) { runIdStr.append(runId); } else {
			 * runIdStr.append(runId).append(","); } }
			 */
			for (int i = 0; i < runIdList.size(); i++) {
				long runId = runIdList.get(i);
				StringBuffer sb = new StringBuffer();
				sb.append(" SELECT RUN_ID, STATUS FROM PR_QUARTER_REC_HEADER ");
				sb.append(" WHERE RUN_ID = ").append(runId);
				logger.debug(sb.toString());
				stmt = conn.prepareStatement(sb.toString());

				rs = stmt.executeQuery();
				while (rs.next()) {
					runIdAndStatusCode.put(rs.getLong("RUN_ID"), rs.getInt("STATUS"));
				}
			}
		} catch (SQLException e) {
			throw new GeneralException("getRunIdWithStatusCode() - Error while getting runIds with status code", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}

		return runIdAndStatusCode;
	}

	public HashMap<Long, List<PRItemDTO>> getProductLocationDetail(Connection conn, List<Long> runIdList)
			throws GeneralException {
		HashMap<Long, List<PRItemDTO>> productLocationDetailForRunIds = new HashMap<Long, List<PRItemDTO>>();

		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			/*
			 * StringBuilder runIdStr = new StringBuilder(); for (int i = 0; i <
			 * runIdList.size(); i++) { long runId = runIdList.get(i); if (i ==
			 * runIdList.size() - 1) { runIdStr.append(runId); } else {
			 * runIdStr.append(runId).append(","); } }
			 */
			for (int i = 0; i < runIdList.size(); i++) {
				long runId = runIdList.get(i);
				StringBuffer sb = new StringBuffer();
				sb.append(" SELECT RUN_ID, PRODUCT_ID, PRODUCT_LEVEL_ID, LOCATION_ID, LOCATION_LEVEL_ID, STATUS FROM ");
				sb.append(" PR_QUARTER_REC_HEADER  WHERE RUN_ID = ").append(runId);
				logger.debug("Query for getProductLocationDetail() - " + sb.toString());
				stmt = conn.prepareStatement(sb.toString());
				rs = stmt.executeQuery();

				List<PRItemDTO> productAndLocationList = new ArrayList<PRItemDTO>();
				while (rs.next()) {
					PRItemDTO itemDto = new PRItemDTO();
					itemDto.setItemCode(rs.getInt("PRODUCT_ID"));
					itemDto.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
					itemDto.setPriceZoneId(rs.getInt("LOCATION_ID"));
					itemDto.setChildLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
					itemDto.setStatusCode(rs.getInt("STATUS"));
					if (productLocationDetailForRunIds.containsKey(rs.getLong("RUN_ID"))) {
						productAndLocationList = productLocationDetailForRunIds.get(rs.getLong("RUN_ID"));
					}
					productAndLocationList.add(itemDto);
					productLocationDetailForRunIds.put(rs.getLong("RUN_ID"), productAndLocationList);

				}
			}

		} catch (SQLException e) {
			logger.error(
					"getProductLocationDetail() - Error while getting product and location detail from header table");
			throw new GeneralException(
					"getProductLocationDetail() - Error while getting product and location detail from header table",
					e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return productLocationDetailForRunIds;
	}

	public HashMap<Integer, String> getZoneNoForZoneId(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		HashMap<Integer, String> zoneIdMap = new HashMap<Integer, String>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE");

			stmt = conn.prepareStatement(db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneIdMap.put((rs.getInt("PRICE_ZONE_ID")), rs.getString("ZONE_NUM"));
			}
		} catch (SQLException ex) {
			logger.error("getZoneNoForZoneNum() - Error when getting ZoneNoForZoneNum - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneIdMap;
	}
	
	public HashMap<Integer, String> getZoneNoForZoneIdWithoutTestZone(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		HashMap<Integer, String> zoneIdMap = new HashMap<Integer, String>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE ZONE_TYPE <> 'T'");

			stmt = conn.prepareStatement(db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneIdMap.put((rs.getInt("PRICE_ZONE_ID")), rs.getString("ZONE_NUM"));
			}
		} catch (SQLException ex) {
			logger.error("getZoneNoForZoneNum() - Error when getting ZoneNoForZoneNum - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneIdMap;
	}

	public void updateExpiryExportFlagForStoreList(Connection conn, List<PriceExportDTO> expiredStoreData)
			throws GeneralException {
		PreparedStatement statement = null;

		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE = ? ");
			sb.append(" AND PRICE_CHECK_LIST_ID = ? ");

			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query for updateExportFlagOfStoreLockItems() - " + sb.toString());

			int counter = 0;

			for (PriceExportDTO dto : expiredStoreData) {
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, dto.getItemCode());
				statement.setInt(++colIndex, dto.getPriceCheckListId());
				statement.addBatch();
				if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					counter = 0;
				}
			}

			if (counter > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
			PristineDBUtil.commitTransaction(conn, "Price export update status");	
			logger.debug("updateExpiryExportFlagForStoreList() - updating expiry items completed");
		} catch (SQLException e) {
			PristineDBUtil.rollbackTransaction(conn, "Price export update status");
			logger.error("updateExpiryExportFlagForStoreList() - Error updating export flag for store list expiry store lock items");
			throw new GeneralException(
					"updateExpiryExportFlagForStoreList()- Error updating export flag for store list expiry store lock items", e);
			
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	public void updateExpiryExportFlagForStoreListV3(Connection conn, List<PRItemDTO> expiredStoreData)
			throws GeneralException {
		PreparedStatement statement = null;

		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE = ? ");
			sb.append(" AND PRICE_CHECK_LIST_ID = ? ");

			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query for updateExportFlagOfStoreLockItems() - " + sb.toString());

			int counter = 0;

			for (PRItemDTO dto : expiredStoreData) {
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, dto.getItemCode());
				statement.setInt(++colIndex, dto.getPriceCheckListId());
				statement.addBatch();
				if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					counter = 0;
				}
			}

			if (counter > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
			PristineDBUtil.commitTransaction(conn, "Price export update status");	
			logger.debug("updateExpiryExportFlagForStoreList() - updating expiry items completed");
		} catch (SQLException e) {
			PristineDBUtil.rollbackTransaction(conn, "Price export update status");
			logger.error("updateExpiryExportFlagForStoreList() - Error updating export flag for store list expiry store lock items");
			throw new GeneralException(
					"updateExpiryExportFlagForStoreList()- Error updating export flag for store list expiry store lock items", e);
			
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public void updateExpiryExportFlagForRegularItemList(Connection conn, List<PriceExportDTO> expiredStoreData)
			throws GeneralException {
		PreparedStatement statement = null;

		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE = ? ");
			sb.append(" AND STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_STR_NO = ?) ");
			sb.append(" AND PRICE_CHECK_LIST_ID = ? ");

			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query for updateExportFlagOfStoreLockItems() - " + sb.toString());

			int counter = 0;

			for (PriceExportDTO dto : expiredStoreData) {
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, dto.getItemCode());
				statement.setString(++colIndex, dto.getStoreNo());
				statement.setInt(++colIndex, dto.getPriceCheckListId());
				statement.addBatch();
				if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					logger.debug("updateExportFlagOfStoreLockItems() - updated " + counter + " items");
					counter = 0;
				}
			}

			if (counter > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
			PristineDBUtil.commitTransaction(conn, "Price export update status");	
			logger.debug("updateExportFlagOfStoreLockItems() - updating expiry item completed");
		} catch (SQLException e) {
			PristineDBUtil.rollbackTransaction(conn, "Price export update status");
			logger.error("updateExpiryExportFlagForRegularItemList() - Error updating export flag for regular expiry store lock items");
			throw new GeneralException(
					"updateExpiryExportFlagForRegularItemList()- Error updating export flag for regular expiry store lock items", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	public void updateExpiryExportFlagForRegularItemListV3(Connection conn, List<PRItemDTO> expiredStoreData)
			throws GeneralException {
		PreparedStatement statement = null;

		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE = ? ");
			sb.append(" AND STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_STR_NO = ?) ");
			sb.append(" AND PRICE_CHECK_LIST_ID = ? ");

			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query for updateExportFlagOfStoreLockItems() - " + sb.toString());

			int counter = 0;

			for (PRItemDTO dto : expiredStoreData) {
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, dto.getItemCode());
				statement.setString(++colIndex, dto.getStoreNo());
				statement.setInt(++colIndex, dto.getPriceCheckListId());
				statement.addBatch();
				if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					logger.debug("updateExportFlagOfStoreLockItems() - updated " + counter + " items");
					counter = 0;
				}
			}

			if (counter > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
			PristineDBUtil.commitTransaction(conn, "Price export update status");	
			logger.debug("updateExportFlagOfStoreLockItems() - updating expiry item completed");
		} catch (SQLException e) {
			PristineDBUtil.rollbackTransaction(conn, "Price export update status");
			logger.error("updateExpiryExportFlagForRegularItemList() - Error updating export flag for regular expiry store lock items");
			throw new GeneralException(
					"updateExpiryExportFlagForRegularItemList()- Error updating export flag for regular expiry store lock items", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	
	public HashMap<Integer, List<PriceExportDTO>> setPriceForExpiryItems(boolean ExpiryOnCurrentDate, Connection conn, String priceExportType) {

		String priceCheckListIdStr = PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID");
		int priceCheckListId = Integer.parseInt(priceCheckListIdStr);
		
		// contains ==> <itemcode, List<storeNum,priceCheckListId>>
		Map<Integer, List<PriceExportDTO>> expiryItemMap = getExpiryItemsFromStoreLockV3(conn, priceCheckListId, priceExportType);
		Map<Integer, List<PriceExportDTO>> expiryItemMapStoreList = getExpiryItemsFromStoreLockFromLocation(conn,
				priceCheckListId, priceExportType);

		HashMap<Integer, List<PriceExportDTO>> mergedMap = new HashMap<>();

		if (expiryItemMap.size() > 0) {	
			
			// map1 containg All itemcode as key and empty list as value
			expiryItemMap.forEach((itemCode, storeList) -> {
				List<PriceExportDTO> storeListEmpty = new ArrayList<>();
				mergedMap.put(itemCode, storeListEmpty);
			});
		}

		if (expiryItemMapStoreList.size() > 0) {
			
			// map2 containg All itemcode as key and empty list as value
			expiryItemMapStoreList.forEach((itemCode, storeList) -> {
				List<PriceExportDTO> storeListEmpty = new ArrayList<>();				
				mergedMap.put(itemCode, storeListEmpty);
			});
		}
		mergedMap.forEach((itemCode, storeList) -> {
			
			if (expiryItemMap.containsKey(itemCode) && expiryItemMapStoreList.containsKey(itemCode)) {
				List<PriceExportDTO> expiryStores = expiryItemMap.get(itemCode);
				List<PriceExportDTO> expiryStoresFromStoreList = expiryItemMapStoreList.get(itemCode);
				Set<String> distinctStores = new HashSet<>();
				// HashMap<Integer, List<PRItemDTO>> commonMapWithUniquePCLId = new HashMap<>();
				// List<PRItemDTO> storeAndPCLId = new ArrayList<>();
				for (PriceExportDTO objects : expiryStores) {
					
					if (!distinctStores.contains(objects.getStoreNo())) {
						distinctStores.add(objects.getStoreNo());
						storeList.add(objects);
					}
				}

				for (PriceExportDTO storeFromStoreList : expiryStoresFromStoreList) {
					
					if (!distinctStores.contains(storeFromStoreList.getStoreNo())) {
						distinctStores.add(storeFromStoreList.getStoreNo());
						storeList.add(storeFromStoreList);
					}
				}
			} else if (expiryItemMap.containsKey(itemCode)) {
				
				List<PriceExportDTO> expiryStores = expiryItemMap.get(itemCode);
				storeList.addAll(expiryStores);
			} else if (expiryItemMapStoreList.containsKey(itemCode)) {
				
				List<PriceExportDTO> expiryStoresFromStoreList = expiryItemMapStoreList.get(itemCode);
				storeList.addAll(expiryStoresFromStoreList);
			}
		});

		return mergedMap;
	}
	
	
	public HashMap<Integer, List<PRItemDTO>> setPriceForExpiryItemsV3(boolean ExpiryOnCurrentDate, Connection conn, String priceExportType) {

		String priceCheckListIdStr = PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID");
		int priceCheckListId = Integer.parseInt(priceCheckListIdStr);
		
		// contains ==> <itemcode, List<storeNum,priceCheckListId>>
		//if item 1-store1 is in expiryItemMap and in expiryItemMapStoreList, we compare and merge. so that duplicate records are not present
		//similar logic needs to be applied for A records in populateStoreLockItemList
		//TODO
		HashMap<Integer, List<PRItemDTO>> expiryItemMap = getExpiryItemsFromStoreLock(conn, priceCheckListId, priceExportType);
		HashMap<Integer, List<PRItemDTO>> expiryItemMapStoreList = getExpiryItemsFromStoreLockFromLocationV3(conn,
				priceCheckListId, priceExportType);
		

		HashMap<Integer, List<PRItemDTO>> mergedMap = new HashMap<>();

		if (expiryItemMap.size() > 0) {	
			
			// map1 containg All itemcode as key and empty list as value
			expiryItemMap.forEach((itemCode, storeList) -> {
				List<PRItemDTO> storeListEmpty = new ArrayList<>();
				mergedMap.put(itemCode, storeListEmpty);
			});
		}

		if (expiryItemMapStoreList.size() > 0) {
			
			// map2 containg All itemcode as key and empty list as value
			expiryItemMapStoreList.forEach((itemCode, storeList) -> {
				List<PRItemDTO> storeListEmpty = new ArrayList<>();				
				mergedMap.put(itemCode, storeListEmpty);
			});
		}
		mergedMap.forEach((itemCode, storeList) -> {
			
			if (expiryItemMap.containsKey(itemCode) && expiryItemMapStoreList.containsKey(itemCode)) {
				List<PRItemDTO> expiryStores = expiryItemMap.get(itemCode);
				List<PRItemDTO> expiryStoresFromStoreList = expiryItemMapStoreList.get(itemCode);
				Set<String> distinctStores = new HashSet<>();
				// HashMap<Integer, List<PRItemDTO>> commonMapWithUniquePCLId = new HashMap<>();
				// List<PRItemDTO> storeAndPCLId = new ArrayList<>();
				for (PRItemDTO objects : expiryStores) {
					
					if (!distinctStores.contains(objects.getStoreNo())) {
						distinctStores.add(objects.getStoreNo());
						storeList.add(objects);
					}
				}

				for (PRItemDTO storeFromStoreList : expiryStoresFromStoreList) {
					
					if (!distinctStores.contains(storeFromStoreList.getStoreNo())) {
						distinctStores.add(storeFromStoreList.getStoreNo());
						storeList.add(storeFromStoreList);
					}
				}
			} else if (expiryItemMap.containsKey(itemCode)) {
				
				List<PRItemDTO> expiryStores = expiryItemMap.get(itemCode);
				storeList.addAll(expiryStores);
			} else if (expiryItemMapStoreList.containsKey(itemCode)) {
				
				List<PRItemDTO> expiryStoresFromStoreList = expiryItemMapStoreList.get(itemCode);
				storeList.addAll(expiryStoresFromStoreList);
			}
		});

		return mergedMap;
	}


	private Map<Integer, List<PriceExportDTO>> getExpiryItemsFromStoreLockFromLocation(Connection conn,
			int priceCheckListId, String priceExportType) {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		Map<Integer, List<PriceExportDTO>> expiryItemMapTwo = new HashMap<>();
		List<PriceExportDTO> expList = new ArrayList<PriceExportDTO>();
		int counter=0;
		try {
			StringBuffer sb = new StringBuffer();
			sb.append(
					" select cs.comp_str_no, pcli.item_code as item_code, pcli.price_check_list_id, cal.calendar_id, ");
			sb.append(" pcl.location_id from price_check_list_items pcli ");
			sb.append(" left join price_check_list pcl on pcli.PRICE_CHECK_LIST_ID = pcl.ID ");
			sb.append(" left join location_group_relation lgr on pcl.location_level_id = lgr.location_level_id ");
			sb.append(" and pcl.location_id = lgr.location_id ");
			sb.append(" left join competitor_store cs on lgr.child_location_id = cs.comp_str_id AND cs.active_indicator = 'Y' ");
			sb.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			sb.append(" left join retail_calendar cal on pcli.end_date BETWEEN CAL.START_DATE AND CAL.END_DATE AND CAL.ROW_TYPE = 'W' ");
			sb.append(" where pcl.PRICE_CHECK_LIST_TYPE_ID = ").append(priceCheckListId);
			sb.append(" and (pcli.IS_EXPORTED = 'N' or pcli.IS_EXPORTED IS NULL) ");
			sb.append(" and lgr.location_level_id = ").append(Constants.STORE_LIST_LEVEL_ID);
			sb.append(" and pcli.END_DATE <= TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy') "); 
			sb.append(" and pcli.END_DATE >=  TO_DATE('").append(DateUtil.getSpecificDate(-7)).append("','MM/dd/yyyy') "); 
			sb.append(" AND (CS.STORE_EXCLUSION_FLAG = 'N' OR CS.STORE_EXCLUSION_FLAG IS NULL)");
			//ADDED BY KIRTHI 01/03/2022, TO AVOID NULL STORE NUM IF STORE IS INACTIVE
			sb.append(" AND CS.ACTIVE_INDICATOR = 'Y' AND CS.PRICE_ZONE_ID IS NOT NULL");
			if (priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if (priceExportType.equals(Constants.HARD_PART_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}
			
			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getExpiryItemsFromStoreLockFromLocation() query - " + sb.toString());
			rs = stmt.executeQuery();
			//Added fetch size to solve the gc error
			rs.setFetchSize(2000000);
			while (rs.next()) {
				counter++;
			
				PriceExportDTO dto = new PriceExportDTO();
				dto.setLocationId(rs.getInt("location_id"));
				dto.setStoreNo(rs.getString("comp_str_no"));
				dto.setPriceCheckListId(rs.getInt("price_check_list_id"));
				dto.setCalendarId(rs.getInt("calendar_id"));
				dto.setStoreListExpiry(true);
				dto.setItemCode(rs.getInt("item_code"));
				expList.add(dto);
			}
		} catch (Exception ex) {
			logger.info("counter: "+ counter );
			logger.error("getExpiryItemsFromStoreLockFromLocation() - Error when getting expiry date item - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
			logger.debug("getExpiryItemsFromStoreLockFromLocation()  #TOTAL RECORDS IN LIST: "+ expList.size());
		}
		
		if (expList.size() > 0) {
			expiryItemMapTwo = expList.parallelStream().collect(Collectors.groupingBy(PriceExportDTO::getItemCode));
		}
		logger.debug("getExpiryItemsFromStoreLockFromLocation()  #total records : "+ expiryItemMapTwo.size());
		return expiryItemMapTwo;
	}

	private HashMap<Integer, List<PRItemDTO>> getExpiryItemsFromStoreLockFromLocationV3(Connection conn,
			int priceCheckListId, String priceExportType) {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		HashMap<Integer, List<PRItemDTO>> expiryItemMapTwo = new HashMap<>();

		try {
			StringBuffer sb = new StringBuffer();
			sb.append(
					" select cs.comp_str_no, pcli.item_code as item_code, pcli.price_check_list_id, cal.calendar_id, ");
			sb.append(" pcl.location_id from price_check_list_items pcli ");
			sb.append(" left join price_check_list pcl on pcli.PRICE_CHECK_LIST_ID = pcl.ID ");
			sb.append(" left join location_group_relation lgr on pcl.location_level_id = lgr.location_level_id ");
			sb.append(" and pcl.location_id = lgr.location_id ");
			sb.append(" left join competitor_store cs on lgr.child_location_id = cs.comp_str_id AND cs.active_indicator = 'Y' ");
			sb.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			sb.append(" left join retail_calendar cal on pcli.end_date BETWEEN CAL.START_DATE AND CAL.END_DATE AND CAL.ROW_TYPE = 'W' ");
			sb.append(" where pcl.PRICE_CHECK_LIST_TYPE_ID = ").append(priceCheckListId);
			sb.append(" and (pcli.IS_EXPORTED = 'N' or pcli.IS_EXPORTED IS NULL) ");
			sb.append(" and lgr.location_level_id = ").append(Constants.STORE_LIST_LEVEL_ID);
			sb.append(" and pcli.END_DATE <= TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy') "); 
			sb.append(" and pcli.END_DATE >=  TO_DATE('").append(DateUtil.getSpecificDate(-7)).append("','MM/dd/yyyy') "); 
			sb.append(" AND (CS.STORE_EXCLUSION_FLAG = 'N' OR CS.STORE_EXCLUSION_FLAG IS NULL)");
			// ADDED BY KIRTHI 01/03/2022, TO AVOID NULL STORE NUM IF STORE IS INACTIVE
			sb.append(" AND CS.ACTIVE_INDICATOR = 'Y' AND CS.PRICE_ZONE_ID IS NOT NULL");
			if (priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if (priceExportType.equals(Constants.HARD_PART_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}
			
			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getExpiryItemsFromStoreLockFromLocation() query - " + sb.toString());
			rs = stmt.executeQuery();
		
			while (rs.next()) {
			
				List<PRItemDTO> expList = new ArrayList<PRItemDTO>();
				PRItemDTO dto = new PRItemDTO();
				dto.setLocationId(rs.getInt("location_id"));
				dto.setStoreNo(rs.getString("comp_str_no"));
				dto.setPriceCheckListId(rs.getInt("price_check_list_id"));
				dto.setCalendarId(rs.getInt("calendar_id"));
				dto.setStoreListExpiry(true);
				dto.setItemCode(rs.getInt("item_code"));
				if (expiryItemMapTwo.containsKey(rs.getInt("item_code"))) {
					expList = expiryItemMapTwo.get(rs.getInt("item_code"));
				}
				expList.add(dto);
				expiryItemMapTwo.put((rs.getInt("item_code")), expList);
				
			}
		} catch (Exception ex) {
			logger.error("getExpiryItemsFromStoreLockFromLocation() - Error when getting expiry date item - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		return expiryItemMapTwo;
	}
	
		private Map<Integer, List<PriceExportDTO>> getExpiryItemsFromStoreLockV3(Connection conn, int priceCheckListId, String priceExportType) {

		PreparedStatement stmt = null;
		ResultSet rs = null;

		Map<Integer, List<PriceExportDTO>> expiryItemsMap = new HashMap<>();
		List<PriceExportDTO> expList = new ArrayList<PriceExportDTO>();
		try {
			StringBuffer db = new StringBuffer();
			db.append(" select distinct(pcli.item_code) as item_code, cs.comp_str_no, pcl.location_id, ");
			db.append(" pcli.price_check_list_id, cal.calendar_id from price_check_list_items pcli ");
			db.append(" left join competitor_store cs on pcli.store_id = cs.comp_str_id AND cs.active_indicator = 'Y' ");
			db.append(" left join price_check_list pcl on pcli.price_check_list_id = pcl.id ");
			db.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			db.append(" left join retail_calendar cal on pcli.end_date BETWEEN CAL.START_DATE AND CAL.END_DATE AND CAL.ROW_TYPE = 'W' ");
			db.append(" where (pcli.IS_EXPORTED = 'N' or pcli.IS_EXPORTED IS NULL)");
			db.append(" and pcli.PRICE_CHECK_LIST_ID in (select id from price_check_list where ");
			db.append(" PRICE_CHECK_LIST_TYPE_ID = ").append(priceCheckListId).append(") ");
			db.append(" and pcli.END_DATE <= TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy') "); 
			db.append(" and pcli.END_DATE >=  TO_DATE('").append(DateUtil.getSpecificDate(-7)).append("','MM/dd/yyyy') "); 
			db.append(" and pcli.store_id is not null AND (CS.STORE_EXCLUSION_FLAG = 'N' OR CS.STORE_EXCLUSION_FLAG IS NULL)");
			db.append(" and cs.price_zone_id is not null");
			if(priceExportType.equals(Constants.SALE_FLOOR_ITEMS) || priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
			db.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if(priceExportType.equals(Constants.HARD_PART_ITEMS) || priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				db.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}
			
			stmt = conn.prepareStatement(db.toString());
			logger.debug("getExpiryItemsFromStoreLockV3()  Query - " + db.toString());
			rs = stmt.executeQuery();
			//Added fetch size
			rs.setFetchSize(2000000);
			while (rs.next()) {
			
				PriceExportDTO dto = new PriceExportDTO();				
				dto.setLocationId(rs.getInt("location_id"));
				dto.setStoreNo(rs.getString("comp_str_no"));
				dto.setCalendarId(rs.getInt("calendar_id"));
				dto.setStoreListExpiry(false);
				dto.setPriceCheckListId(rs.getInt("price_check_list_id"));
				dto.setItemCode(rs.getInt("item_code"));
				expList.add(dto);
				/*
				 * if (expiryItemsMap.containsKey(rs.getInt("item_code"))) { expList =
				 * expiryItemsMap.get(rs.getInt("item_code")); } expList.add(dto);
				 * expiryItemsMap.put((rs.getInt("item_code")), expList);
				 */

			}
		} catch (Exception ex) {
			logger.error("getExpiryItemsFromStoreLockV3() - Error when getting expiry date item- " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		
		if (expList.size() > 0) {
			expiryItemsMap = expList.parallelStream().collect(Collectors.groupingBy(PriceExportDTO::getItemCode));
		}
		logger.debug("getExpiryItemsFromStoreLockFromLocation()  #total records : "+ expiryItemsMap.size());
		return expiryItemsMap;
	}

	private HashMap<Integer, List<PRItemDTO>> getExpiryItemsFromStoreLock(Connection conn, int priceCheckListId, String priceExportType) {

		PreparedStatement stmt = null;
		ResultSet rs = null;

		HashMap<Integer, List<PRItemDTO>> expiryItemMapOne = new HashMap<>();

		try {
			StringBuffer db = new StringBuffer();
			db.append(" select distinct(pcli.item_code) as item_code, cs.comp_str_no, pcl.location_id, ");
			db.append(" pcli.price_check_list_id, cal.calendar_id from price_check_list_items pcli ");
			db.append(" left join competitor_store cs on pcli.store_id = cs.comp_str_id AND cs.active_indicator = 'Y' ");
			db.append(" left join price_check_list pcl on pcli.price_check_list_id = pcl.id ");
			db.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			db.append(" left join retail_calendar cal on pcli.end_date BETWEEN CAL.START_DATE AND CAL.END_DATE AND CAL.ROW_TYPE = 'W' ");
			db.append(" where (pcli.IS_EXPORTED = 'N' or pcli.IS_EXPORTED IS NULL)");
			db.append(" and pcli.PRICE_CHECK_LIST_ID in (select id from price_check_list where ");
			db.append(" PRICE_CHECK_LIST_TYPE_ID = ").append(priceCheckListId).append(") ");
			db.append(" and pcli.END_DATE <= TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy') "); 
			db.append(" and pcli.END_DATE >=  TO_DATE('").append(DateUtil.getSpecificDate(-7)).append("','MM/dd/yyyy') "); 
			db.append(" and pcli.store_id is not null AND (CS.STORE_EXCLUSION_FLAG = 'N' OR CS.STORE_EXCLUSION_FLAG IS NULL)");
			db.append(" and cs.price_zone_id is not null");
			if(priceExportType.equals(Constants.SALE_FLOOR_ITEMS) || priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
			db.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if(priceExportType.equals(Constants.HARD_PART_ITEMS) || priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				db.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}
			
			stmt = conn.prepareStatement(db.toString());
			logger.debug("getExpiryItemsFromStoreLock()  Query - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				List<PRItemDTO> expList = new ArrayList<PRItemDTO>();
				PRItemDTO dto = new PRItemDTO();				
				dto.setLocationId(rs.getInt("location_id"));
				dto.setStoreNo(rs.getString("comp_str_no"));
				dto.setCalendarId(rs.getInt("calendar_id"));
				dto.setStoreListExpiry(false);
				dto.setPriceCheckListId(rs.getInt("price_check_list_id"));
				dto.setItemCode(rs.getInt("item_code"));
				if (expiryItemMapOne.containsKey(rs.getInt("item_code"))) {
					expList = expiryItemMapOne.get(rs.getInt("item_code"));
				}
				expList.add(dto);
				expiryItemMapOne.put((rs.getInt("item_code")), expList);

			}
		} catch (Exception ex) {
			logger.error("getExpiryItemsFromStoreLock() - Error when getting expiry date item- " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		return expiryItemMapOne;
	}

	
	public List<ReDTO> getNextWeekExpiredItems(Connection conn) {
		String priceCheckListIdStr = PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID");
		int priceCheckListId = Integer.parseInt(priceCheckListIdStr);
		PreparedStatement stmt = null;
		ResultSet rs = null;

		List<ReDTO> expiredItemsOnNextWeek = new ArrayList<ReDTO>();

		try {
			StringBuffer db = new StringBuffer();

			db.append(" select pcli.item_code, il.retailer_item_code, cs.comp_str_no, pcli.end_date, rpz.name, ");
			db.append(" rpz.zone_num from price_check_list_items pcli ");
			db.append(" left join competitor_store cs on pcli.store_id = cs.comp_str_id ");
			db.append(" left join retail_price_zone rpz on cs.price_zone_id = rpz.price_zone_id ");
			db.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			db.append(" where (pcli.IS_EXPORTED = 'N' or pcli.IS_EXPORTED IS NULL)");
			db.append(" and pcli.PRICE_CHECK_LIST_ID in (select id from price_check_list where ");
			db.append(" PRICE_CHECK_LIST_TYPE_ID = ").append(priceCheckListId).append(") ");
			//db.append(" and pcli.END_DATE > sysdate and pcli.END_DATE <=  sysdate +7 ");
			db.append(" and pcli.END_DATE > TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy') "); 
			db.append(" and pcli.END_DATE <=  TO_DATE('").append(DateUtil.getSpecificDate(7)).append("','MM/dd/yyyy') "); 

			stmt = conn.prepareStatement(db.toString());
			logger.debug("getNextWeekExpiredItems() - query: " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {

				ReDTO dto = new ReDTO();

				dto.setStoreNo(rs.getString("comp_str_no"));
				dto.setEndDate(rs.getString("end_date"));
				dto.setItemCode(rs.getInt("item_code"));
				dto.setRetailerItemCode(rs.getString("retailer_item_code"));
				dto.setZoneName(rs.getString("name"));
				dto.setZoneNo(rs.getString("zone_num"));

				expiredItemsOnNextWeek.add(dto);

			}
		} catch (SQLException ex) {
			logger.error(
					"getNextWeekExpiredItems() - Error when getting next week expiry date item - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return expiredItemsOnNextWeek;

	}

	public HashMap<Integer, String> getCategoryAndRecomUnitOfItem(Connection conn,
			HashMap<Integer, List<ReDTO>> expiredItemsOnNextWeek, int productLevelId) {

		HashMap<Integer, String> nameAndItemPair = new HashMap<Integer, String>();

		PreparedStatement stmt = null;
		ResultSet rs = null;

		for (Map.Entry<Integer, List<ReDTO>> itemObj : expiredItemsOnNextWeek.entrySet()) {

			try {
				StringBuffer sb = new StringBuffer();

				if (productLevelId == 4) {
					sb.append(" select pgc.name from product_group pg ");
				} else if (productLevelId == 7) {
					sb.append(" select pgcr.name from product_group pg ");
				}
				sb.append(
						" left join product_group_relation pgr on pgr.product_id = pg.product_id and pgr.product_level_id = pg.product_level_id ");
				sb.append(
						" left join product_group pgc on pgr.child_product_level_id = pgc.product_level_id and pgr.child_product_id = pgc.product_id ");

				sb.append(
						" left join product_group_relation_rec pgrr on pgrr.product_id = pg.product_id and pgrr.product_level_id = pg.product_level_id ");
				sb.append(
						" left join product_group pgcr on pgrr.child_product_level_id = pgcr.product_level_id and pgrr.child_product_id = pgcr.product_id ");

				if (productLevelId == 4) {
					sb.append(
							" where ( pgc.product_id in (SELECT product_id FROM (SELECT * FROM PRODUCT_GROUP_RELATION_REC PGR ");
					sb.append(" START WITH CHILD_PRODUCT_LEVEL_ID = 1 AND CHILD_PRODUCT_ID in (")
							.append(itemObj.getKey()).append(") ");
					sb.append(
							" CONNECT BY  PRIOR PRODUCT_ID = CHILD_PRODUCT_ID AND  PRIOR PRODUCT_LEVEL_ID = CHILD_PRODUCT_LEVEL_ID) ");
					sb.append(" WHERE PRODUCT_LEVEL_ID = ").append(productLevelId).append(" )) ");
				} else if (productLevelId == 7) {
					sb.append(
							" where ( pgcr.product_id in (SELECT product_id FROM (SELECT * FROM PRODUCT_GROUP_RELATION_REC PGR ");
					sb.append(" START WITH CHILD_PRODUCT_LEVEL_ID = 1 AND CHILD_PRODUCT_ID in (")
							.append(itemObj.getKey()).append(") ");
					sb.append(
							" CONNECT BY  PRIOR PRODUCT_ID = CHILD_PRODUCT_ID AND  PRIOR PRODUCT_LEVEL_ID = CHILD_PRODUCT_LEVEL_ID) ");
					sb.append(" WHERE PRODUCT_LEVEL_ID = ").append(productLevelId).append(" )) ");
				}

				stmt = conn.prepareStatement(sb.toString());
				rs = stmt.executeQuery();
				while (rs.next()) {
					if (productLevelId == 4) {
						nameAndItemPair.put(itemObj.getKey(), rs.getString("name"));
					} else if (productLevelId == 7) {
						nameAndItemPair.put(itemObj.getKey(), rs.getString("name"));
					}
				}

			} catch (SQLException ex) {
				logger.error(
						"getCategoryAndRecomUnitOfItem() - Error when getting Category/recommendation Unit of item - "
								+ ex.getMessage());
			} finally {
				PristineDBUtil.close(rs);
				PristineDBUtil.close(stmt);
			}
		}
		return nameAndItemPair;
	}

	public List<Long> getRunIdForItemsOfAllType(Connection conn) throws GeneralException {
		List<Long> runIdList = new ArrayList<Long>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {

			StringBuffer sb = new StringBuffer();
			sb.append("SELECT DISTINCT(RUN_ID) AS RUN_ID FROM PR_PRICE_EXPORT");
			logger.debug(sb.toString());
			stmt = conn.prepareStatement(sb.toString());

			rs = stmt.executeQuery();
			while (rs.next()) {
				runIdList.add(rs.getLong("RUN_ID"));
			}

		} catch (SQLException e) {
			throw new GeneralException("getRunIdForItemsOfAllType() - Error while getting all runIds", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return runIdList;
	}

	public HashMap<Integer, String> getZoneNameForZoneId(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		HashMap<Integer, String> zoneNameMap = new HashMap<Integer, String>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT NAME, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE ");

			stmt = conn.prepareStatement(db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneNameMap.put((rs.getInt("PRICE_ZONE_ID")), rs.getString("NAME"));
			}
		} catch (SQLException ex) {
			logger.error("getZoneNameForZoneId() - Error when retrieving zone name map - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneNameMap;
	}

	public <K, V> K getKey(Map<K, V> map, V value) {
		for (K key : map.keySet()) {
			if (value.equals(map.get(key))) {
				return key;
			}
		}
		return null;
	}

	public void insertExportTrackId(Connection conn, List<PRItemDTO> itemsFiltered) throws GeneralException {
		Set<String> uniqueData = new HashSet<>();
		for (PRItemDTO data : itemsFiltered) {
			uniqueData.add(data.getItemCode()+";"+data.getRunId());
		}
		PreparedStatement stmt = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" INSERT INTO PR_PRICE_EXPORT_TRACK (PRICE_EXPORT_TRACK_ID, RUN_ID, ");
			sb.append(" EXPORTED, ITEM_CODE)  VALUES (PRICE_EXPORT_TRACK_ID_SEQ.NEXTVAL, ?, SYSDATE, ?)");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("insertExportTrackId() Query- " + sb.toString());
			int itemCountInBatch = 0;

			for (String item : uniqueData) {				
				itemCountInBatch++;
				int colIndex = 0;
				stmt.setLong(++colIndex, new Long(item.split(";")[1]));
				stmt.setInt(++colIndex, Integer.parseInt(item.split(";")[0]));
				stmt.addBatch();

				if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					itemCountInBatch = 0;
				}
			}
			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("insertExportTrackId() - Error inserting export track Id");
			throw new GeneralException("insertExportTrackId() - Error inserting export track id", e);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}
	
	public void insertExportTrackIdV3(Connection conn, List<PriceExportDTO> itemsFiltered) throws GeneralException {
		Set<String> uniqueData = new HashSet<>();
		for (PriceExportDTO data : itemsFiltered) {
			uniqueData.add(data.getItemCode()+";"+data.getRunId());
		}
		PreparedStatement stmt = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" INSERT INTO PR_PRICE_EXPORT_TRACK (PRICE_EXPORT_TRACK_ID, RUN_ID, ");
			sb.append(" EXPORTED, ITEM_CODE)  VALUES (PRICE_EXPORT_TRACK_ID_SEQ.NEXTVAL, ?, SYSDATE, ?)");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("insertExportTrackId() Query- " + sb.toString());
			int itemCountInBatch = 0;

			for (String item : uniqueData) {				
				itemCountInBatch++;
				int colIndex = 0;
				stmt.setLong(++colIndex, new Long(item.split(";")[1]));
				stmt.setInt(++colIndex, Integer.parseInt(item.split(";")[0]));
				stmt.addBatch();

				if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					itemCountInBatch = 0;
				}
			}
			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("insertExportTrackId() - Error inserting export track Id");
			throw new GeneralException("insertExportTrackId() - Error inserting export track id", e);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}
	
	public List<Long> getRunIdsByType(Connection conn, String currWeekEndDate, String priceType) throws GeneralException {
		List<Long> runIdList = new ArrayList<Long>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			StringBuffer sb = new StringBuffer();
			
			sb.append("SELECT DISTINCT(PE.RUN_ID) FROM PR_PRICE_EXPORT PE ");
			sb.append(" LEFT JOIN PR_QUARTER_REC_ITEM PQI ON PE.RUN_ID = PQI.RUN_ID AND PQI.PRODUCT_LEVEL_ID = 1  AND PQI.CAL_TYPE = 'Q' ");
			sb.append(" WHERE PE.PRICE_TYPE = '"+priceType+"'");
			
			if(currWeekEndDate!="")
			sb.append(" AND TO_DATE(TO_CHAR(PQI.REG_EFF_DATE,'MM/dd/yyyy'),'MM/dd/yyyy')=TO_DATE('"+currWeekEndDate+"','MM/dd/yyyy') ");
			
			logger.debug(" getRunIdsByType() :- "+ sb.toString());
			stmt = conn.prepareStatement(sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				runIdList.add(rs.getLong("RUN_ID"));
			}

		} catch (SQLException e) {
			throw new GeneralException("getRunIdsByType() - Error while getting runIds ", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return runIdList;
	}

	
	/**
	 * TODO 
	 * @param conn
	 * @param itemCodes
	 * @param zones
	 * @param lockListTypeId
	 * @return stores from store lock list
	 */
	public HashMap<Integer, HashMap<Integer, Set<String>>> getExcludeStoresFromStoreLockListV3(Connection conn, Set<Integer> itemCodes, Set<Integer> zones,
			int lockListTypeId, int storeLockLocationLevelId, String priceExportType, Set<Long> runIds) {
		
		HashMap<Integer, HashMap<Integer, Set<String>>> storeLockMap = new HashMap<>();
		
		try {
			retrieveLockedStoresForItems(conn, priceExportType, lockListTypeId, zones, storeLockLocationLevelId, runIds);
		}
		catch(Exception e) {
			logger.info("getExcludeStoresFromStoreLockList() - Error while getting store lock items");
		}
		
		//COMMENTED ON 02/16/2022 BY KIRTHI
		//TO AVOID ITERATING IN 1000 BATCH COUNT
		//PRODUCTION ISSUE- QUERY FETCHED MORE THAN 2HRS
		//CHANGES DID - ADDED SUB QUERY
		/*List<Integer> itemCodeList = new ArrayList<Integer>();
		int limitcount = 0;
		HashMap<Integer, HashMap<Integer, Set<String>>> storeLockMapToExport = new HashMap<>();
		for (Integer itemCode : itemCodes) {
			itemCodeList.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.BATCH_UPDATE_COUNT == 0)) {
				Object[] values = itemCodeList.toArray();
				HashMap<Integer, HashMap<Integer, Set<String>>> storeLockMap = retrieveLockedStoresForItems(conn, priceExportType, lockListTypeId, zones, storeLockLocationLevelId,
						values);
				storeLockMapToExport.putAll(storeLockMap);
				itemCodeList.clear();
			}
		}

		if (itemCodeList.size() > 0) {
			Object[] values = itemCodeList.toArray();
			HashMap<Integer, HashMap<Integer, Set<String>>> storeLockMap = retrieveLockedStoresForItems(conn, priceExportType, lockListTypeId, zones, storeLockLocationLevelId,
					values);
			storeLockMapToExport.putAll(storeLockMap);
			itemCodeList.clear();
		}*/
		return storeLockMap;
	}
	
	public HashMap<Integer, HashMap<Integer, Set<String>>> getExcludeStoresFromStoreLockList(Connection conn, Set<Integer> itemCodes, Set<Integer> zones,
			int lockListTypeId, int storeLockLocationLevelId, String priceExportType) {
		
		/*HashMap<Integer, HashMap<Integer, Set<String>>> storeLockMap = new HashMap<>();
		
		try {
			retrieveLockedStoresForItems(conn, priceExportType, lockListTypeId, zones, storeLockLocationLevelId, runIds);
		}
		catch(Exception e) {
			logger.info("getExcludeStoresFromStoreLockList() - Error while getting store lock items");
		}*/
		
		//COMMENTED ON 02/16/2022 BY KIRTHI
		//TO AVOID ITERATING IN 1000 BATCH COUNT
		//PRODUCTION ISSUE- QUERY FETCHED MORE THAN 2HRS
		//CHANGES DID - ADDED SUB QUERY
		List<Integer> itemCodeList = new ArrayList<Integer>();
		int limitcount = 0;
		HashMap<Integer, HashMap<Integer, Set<String>>> storeLockMapToExport = new HashMap<>();
		for (Integer itemCode : itemCodes) {
			itemCodeList.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.BATCH_UPDATE_COUNT == 0)) {
				Object[] values = itemCodeList.toArray();
				HashMap<Integer, HashMap<Integer, Set<String>>> storeLockMap = retrieveLockedStoresForItems(conn, priceExportType, lockListTypeId, zones, storeLockLocationLevelId,values);
				storeLockMapToExport.putAll(storeLockMap);
				itemCodeList.clear();
			}
		}

		if (itemCodeList.size() > 0) {
			Object[] values = itemCodeList.toArray();
			HashMap<Integer, HashMap<Integer, Set<String>>> storeLockMap = retrieveLockedStoresForItems(conn, priceExportType, lockListTypeId, zones, storeLockLocationLevelId,values);
			storeLockMapToExport.putAll(storeLockMap);
			itemCodeList.clear();
		}
		return storeLockMapToExport;
	}
	
	public HashMap<Integer, HashMap<Integer,List<PriceExportDTO>>> getExcludeStoresFromStoreLockListV2(Connection conn, Set<Integer> itemCodes, Set<Integer> zones,
			int lockListTypeId, int storeLockLocationLevelId, String priceExportType) {
		List<Integer> itemCodeList = new ArrayList<Integer>();
		int limitcount = 0;
		HashMap<Integer, HashMap<Integer,List<PriceExportDTO>>> storeLockMapToExport = new HashMap<>();
		for (Integer itemCode : itemCodes) {
			itemCodeList.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.BATCH_UPDATE_COUNT == 0)) {
				Object[] values = itemCodeList.toArray();
				HashMap<Integer, HashMap<Integer,List<PriceExportDTO>>> storeLockMap = retrieveLockedStoresForItemsV2(conn, priceExportType, lockListTypeId, zones, storeLockLocationLevelId,
						values);
				storeLockMapToExport.putAll(storeLockMap);
				itemCodeList.clear();
			}
		}

		if (itemCodeList.size() > 0) {
			Object[] values = itemCodeList.toArray();
			HashMap<Integer, HashMap<Integer,List<PriceExportDTO>>> storeLockMap = retrieveLockedStoresForItemsV2(conn, priceExportType, lockListTypeId, zones, storeLockLocationLevelId,
					values);
			storeLockMapToExport.putAll(storeLockMap);
			itemCodeList.clear();
		}
		return storeLockMapToExport;
	}
	
	public HashMap<Integer, HashMap<Integer,List<PRItemDTO>>> getExcludeStoresFromStoreLockListV3(Connection conn, Set<Integer> itemCodes, Set<Integer> zones,
			int lockListTypeId, int storeLockLocationLevelId, String priceExportType) {
		List<Integer> itemCodeList = new ArrayList<Integer>();
		int limitcount = 0;
		HashMap<Integer, HashMap<Integer,List<PRItemDTO>>> storeLockMapToExport = new HashMap<>();
		for (Integer itemCode : itemCodes) {
			itemCodeList.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.BATCH_UPDATE_COUNT == 0)) {
				Object[] values = itemCodeList.toArray();
				HashMap<Integer, HashMap<Integer,List<PRItemDTO>>> storeLockMap = retrieveLockedStoresForItemsV3(conn, priceExportType, lockListTypeId, zones, storeLockLocationLevelId,
						values);
				storeLockMapToExport.putAll(storeLockMap);
				itemCodeList.clear();
			}
		}

		if (itemCodeList.size() > 0) {
			Object[] values = itemCodeList.toArray();
			HashMap<Integer, HashMap<Integer,List<PRItemDTO>>> storeLockMap = retrieveLockedStoresForItemsV3(conn, priceExportType, lockListTypeId, zones, storeLockLocationLevelId,
					values);
			storeLockMapToExport.putAll(storeLockMap);
			itemCodeList.clear();
		}
		return storeLockMapToExport;
	}

	/*
	 * private static final String GET_LOCKED_STORES_FROM_PRICE_CHECK_LIST =
	 * " SELECT CS.COMP_STR_NO AS STORE_NUM, " +
	 * " PCLI.STORE_ID, CS.PRICE_ZONE_ID, PCLI.ITEM_CODE AS ITEM_CODE, " +
	 * " PCLI.END_DATE FROM PRICE_CHECK_LIST_ITEMS PCLI LEFT JOIN COMPETITOR_STORE CS  ON PCLI.STORE_ID = CS.COMP_STR_ID WHERE PCLI.ITEM_CODE IN (%s) "
	 * + " AND PCLI.STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE " +
	 * " WHERE PRICE_ZONE_ID IN (%ZONES%) AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) "
	 * +
	 * " AND PCLI.PRICE_CHECK_LIST_ID IN (SELECT ID FROM PRICE_CHECK_LIST WHERE PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST%) "
	 * + " AND (PCLI.END_DATE > TO_DATE('"+DateUtil.getSpecificDate(0)
	 * +"','MM/dd/yyyy') " + " OR PCLI.END_DATE IS NULL) ";
	 */

	/*
	 * private static final String
	 * GET_LOCKED_STORES_FROM_PRICE_CHECK_LIST_AT_STORELIST =
	 * "SELECT CS.COMP_STR_NO AS STORE_NUM, " +
	 * " PCLI.ITEM_CODE AS ITEM_CODE, CS.PRICE_ZONE_ID FROM PRICE_CHECK_LIST_ITEMS PCLI "
	 * + " LEFT JOIN PRICE_CHECK_LIST PCL ON PCLI.PRICE_CHECK_LIST_ID = PCL.ID " +
	 * " LEFT JOIN LOCATION_GROUP_RELATION LGR ON PCL.LOCATION_LEVEL_ID = LGR.LOCATION_LEVEL_ID "
	 * + " AND PCL.LOCATION_ID = LGR.LOCATION_ID " +
	 * " LEFT JOIN COMPETITOR_STORE CS ON LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID " +
	 * " WHERE PCLI.ITEM_CODE IN (%s) AND LGR.CHILD_LOCATION_ID IN " +
	 * " (SELECT COMP_STR_ID FROM COMPETITOR_STORE " +
	 * " WHERE PRICE_ZONE_ID IN (%ZONES%)  AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) "
	 * +
	 * " AND PCL.PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST% AND LGR.LOCATION_LEVEL_ID = %LOC_LEVEL% "
	 * + " AND (PCLI.END_DATE > TO_DATE('"+DateUtil.getSpecificDate(0)
	 * +"','MM/dd/yyyy') " + "OR PCLI.END_DATE IS NULL)";
	 */

	/**
	 * TODO on conn
	 * @param lockListTypeId
	 * @param zones
	 * @param itemStoreLockMap
	 * @param values
	 */
//	private HashMap<Integer, HashMap<Integer, Set<String>>> retrieveLockedStoresForItems(Connection conn, String priceExportType, int lockListTypeId, Set<Integer> zones,
//			int storeLockLocationLevelId, Object... values) {
//		
//		HashMap<Integer, HashMap<Integer, Set<String>>> itemStoreLockMap = new HashMap<>();
//		
//		PreparedStatement stmt = null;
//		ResultSet rs = null;
//		try {
//			//for store level id
//			StringBuilder db = new StringBuilder();
//			db.append(" SELECT CS.COMP_STR_NO AS STORE_NUM, PCL.LOCATION_ID, ");
//			db.append(" PCLI.STORE_ID, CS.PRICE_ZONE_ID, PCLI.ITEM_CODE AS ITEM_CODE, ");
//			db.append(" PCLI.END_DATE FROM PRICE_CHECK_LIST_ITEMS PCLI LEFT JOIN COMPETITOR_STORE CS ON PCLI.STORE_ID = CS.COMP_STR_ID AND CS.ACTIVE_INDICATOR = 'Y' ");
//			db.append(" left join item_lookup il on pcli.item_code = il.item_code ");
//			db.append(" left join price_check_list pcl on pcli.price_check_list_id = pcl.id ");
//			db.append(" WHERE PCLI.ITEM_CODE IN (300680,	301401) ");
//			db.append(" AND PCLI.STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE ");
//			db.append(
//					" WHERE PRICE_ZONE_ID IN (%ZONES%) AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) ");
//			db.append(
//					" AND PCLI.PRICE_CHECK_LIST_ID IN (SELECT ID FROM PRICE_CHECK_LIST WHERE PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST%) ");
//			//db.append(" AND (PCLI.END_DATE > SYSDATE OR PCLI.END_DATE IS NULL) ");
//			db.append(" AND (PCLI.END_DATE > TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy')");
//			db.append(" OR PCLI.END_DATE IS NULL) ");
//			if (priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
//					|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
//				db.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
//			}
//			if (priceExportType.equals(Constants.HARD_PART_ITEMS)
//					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
//				db.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
//			}
//			db.append(" ");
//			
//			//for store list level id
//			StringBuilder sb = new StringBuilder();
//			sb.append(" SELECT CS.COMP_STR_NO AS STORE_NUM, PCL.LOCATION_ID, ");
//			sb.append(" PCLI.ITEM_CODE AS ITEM_CODE, CS.PRICE_ZONE_ID FROM PRICE_CHECK_LIST_ITEMS PCLI ");
//			sb.append(" left join item_lookup il on pcli.item_code = il.item_code ");
//			sb.append(" LEFT JOIN PRICE_CHECK_LIST PCL ON PCLI.PRICE_CHECK_LIST_ID = PCL.ID ");
//			sb.append(" LEFT JOIN LOCATION_GROUP_RELATION LGR ON PCL.LOCATION_LEVEL_ID = LGR.LOCATION_LEVEL_ID ");
//			sb.append(" AND PCL.LOCATION_ID = LGR.LOCATION_ID ");
//			sb.append(" LEFT JOIN COMPETITOR_STORE CS ON LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID ");
//			sb.append(" WHERE PCLI.ITEM_CODE IN (%s) AND LGR.CHILD_LOCATION_ID IN ");
//			sb.append(" (SELECT COMP_STR_ID FROM COMPETITOR_STORE ");
//			sb.append(" WHERE PRICE_ZONE_ID IN (%ZONES%)  AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) ");
//			sb.append(" AND PCL.PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST% AND LGR.LOCATION_LEVEL_ID = %LOC_LEVEL% ");
//			sb.append(" AND (PCLI.END_DATE > TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy')");
//			sb.append(" OR PCLI.END_DATE IS NULL)");
//			if (priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
//					|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
//				sb.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
//			}
//			if (priceExportType.equals(Constants.HARD_PART_ITEMS)
//					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
//				sb.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
//			}
//			
//			if (storeLockLocationLevelId == Constants.STORE_LEVEL_ID) {
//				String sql = db.toString()
//						.replaceAll("%ZONES%", PRCommonUtil.getCommaSeperatedStringFromIntSet(zones))
//						.replaceAll("%CHECK_LIST%", String.valueOf(lockListTypeId));
////						.replaceAll("%RUN_IDS%", PRCommonUtil.getCommaSeperatedStringFromLongSet(runIds));
//				
//				//stmt = conn.prepareStatement(sql)/*String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)))*/;
//				stmt = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
//				logger.debug("sql of store lock items at store level: " + sql);
//				PristineDBUtil.setValues(stmt, values);
//			} else if (storeLockLocationLevelId == Constants.STORE_LIST_LEVEL_ID) {
//				String sql = sb.toString()
//						.replaceAll("%ZONES%", PRCommonUtil.getCommaSeperatedStringFromIntSet(zones))
//						.replaceAll("%CHECK_LIST%", String.valueOf(lockListTypeId))
//						.replaceAll("%LOC_LEVEL%", String.valueOf(Constants.STORE_LIST_LEVEL_ID));
////						.replaceAll("%RUN_IDS%", PRCommonUtil.getCommaSeperatedStringFromLongSet(runIds));
//				stmt = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
//				logger.debug("sql of store lock items at store list level: "  + sql);
//				PristineDBUtil.setValues(stmt, values);
//			}
//
//			stmt.setFetchSize(50000);
//			rs = stmt.executeQuery();
//
//			while (rs.next()) {
//				int itemCode = rs.getInt("ITEM_CODE");
//				int priceZoneId = rs.getInt("PRICE_ZONE_ID");
//				String storeNo = rs.getString("STORE_NUM");
//				int locationId = rs.getInt("LOCATION_ID");	
//				
//				if (itemStoreLockMap.containsKey(itemCode)) {
//					HashMap<Integer, Set<String>> zoneStoreMap = itemStoreLockMap.get(itemCode);
//					if (zoneStoreMap.containsKey(priceZoneId)) {
//						Set<String> stores = zoneStoreMap.get(priceZoneId);
//						stores.add(storeNo+";"+locationId);
//						zoneStoreMap.put(priceZoneId, stores);
//					} else {
//						Set<String> stores = new HashSet<>();
//						stores.add(storeNo+";"+locationId);
//						zoneStoreMap.put(priceZoneId, stores);
//					}
//					itemStoreLockMap.put(itemCode, zoneStoreMap);
//				} else {
//					HashMap<Integer, Set<String>> zoneStoreMap = new HashMap<>();
//					Set<String> stores = new HashSet<>();
//					stores.add(storeNo+";"+locationId);
//					zoneStoreMap.put(priceZoneId, stores);
//					itemStoreLockMap.put(itemCode, zoneStoreMap);					
//				}
//			}
//		} catch (SQLException ex) {
//			logger.error("getExcludeStoresByItemsAndStoreLock() - Error when getting excluded stores from itemList - "
//					+ ex.getMessage());
//		} finally {
//			PristineDBUtil.close(rs);
//			PristineDBUtil.close(stmt);
//		}
//		return itemStoreLockMap;
//	}
	
	private HashMap<Integer, HashMap<Integer, Set<String>>> retrieveLockedStoresForItems(Connection conn, String priceExportType, int lockListTypeId, Set<Integer> zones,
			int storeLockLocationLevelId, Object... values) {
		
		HashMap<Integer, HashMap<Integer, Set<String>>> itemStoreLockMap = new HashMap<>();
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			//for store level id
			StringBuilder db = new StringBuilder();
			db.append(" SELECT CS.COMP_STR_NO AS STORE_NUM, PCL.LOCATION_ID, ");
			db.append(" PCLI.STORE_ID, CS.PRICE_ZONE_ID, PCLI.ITEM_CODE AS ITEM_CODE, ");
			db.append(" PCLI.END_DATE FROM PRICE_CHECK_LIST_ITEMS PCLI LEFT JOIN COMPETITOR_STORE CS ON PCLI.STORE_ID = CS.COMP_STR_ID AND CS.ACTIVE_INDICATOR = 'Y' ");
			db.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			db.append(" left join price_check_list pcl on pcli.price_check_list_id = pcl.id ");
			db.append(" WHERE PCLI.ITEM_CODE IN (%s) ");
			db.append(" AND PCLI.STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE ");
			db.append(
					" WHERE PRICE_ZONE_ID IN (%ZONES%) AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) ");
			db.append(
					" AND PCLI.PRICE_CHECK_LIST_ID IN (SELECT ID FROM PRICE_CHECK_LIST WHERE PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST%) ");
			//db.append(" AND (PCLI.END_DATE > SYSDATE OR PCLI.END_DATE IS NULL) ");
			db.append(" AND (PCLI.END_DATE > TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy')");
			db.append(" OR PCLI.END_DATE IS NULL) ");
			if (priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
				db.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if (priceExportType.equals(Constants.HARD_PART_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				db.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}
			db.append(" ");
			
			//for store list level id
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT CS.COMP_STR_NO AS STORE_NUM, PCL.LOCATION_ID, ");
			sb.append(" PCLI.ITEM_CODE AS ITEM_CODE, CS.PRICE_ZONE_ID FROM PRICE_CHECK_LIST_ITEMS PCLI ");
			sb.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			sb.append(" LEFT JOIN PRICE_CHECK_LIST PCL ON PCLI.PRICE_CHECK_LIST_ID = PCL.ID ");
			sb.append(" LEFT JOIN LOCATION_GROUP_RELATION LGR ON PCL.LOCATION_LEVEL_ID = LGR.LOCATION_LEVEL_ID ");
			sb.append(" AND PCL.LOCATION_ID = LGR.LOCATION_ID ");
			sb.append(" LEFT JOIN COMPETITOR_STORE CS ON LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID ");
			sb.append(" WHERE PCLI.ITEM_CODE IN (%s) AND LGR.CHILD_LOCATION_ID IN ");
			sb.append(" (SELECT COMP_STR_ID FROM COMPETITOR_STORE ");
			sb.append(" WHERE PRICE_ZONE_ID IN (%ZONES%)  AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) ");
			sb.append(" AND PCL.PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST% AND LGR.LOCATION_LEVEL_ID = %LOC_LEVEL% ");
			sb.append(" AND (PCLI.END_DATE > TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy')");
			sb.append(" OR PCLI.END_DATE IS NULL)");
			if (priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if (priceExportType.equals(Constants.HARD_PART_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}
			
			if (storeLockLocationLevelId == Constants.STORE_LEVEL_ID) {
				String sql = db.toString()
						.replaceAll("%ZONES%", PRCommonUtil.getCommaSeperatedStringFromIntSet(zones))
						.replaceAll("%CHECK_LIST%", String.valueOf(lockListTypeId));
				
				stmt = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
				logger.debug("sql of store lock items at store level: " + sql);
				PristineDBUtil.setValues(stmt, values);
			} else if (storeLockLocationLevelId == Constants.STORE_LIST_LEVEL_ID) {
				String sql = sb.toString()
						.replaceAll("%ZONES%", PRCommonUtil.getCommaSeperatedStringFromIntSet(zones))
						.replaceAll("%CHECK_LIST%", String.valueOf(lockListTypeId))
						.replaceAll("%LOC_LEVEL%", String.valueOf(Constants.STORE_LIST_LEVEL_ID));
				stmt = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
				logger.debug("sql of store lock items at store list level: "  + sql);
				PristineDBUtil.setValues(stmt, values);
			}

			stmt.setFetchSize(50000);
			rs = stmt.executeQuery();

			while (rs.next()) {
				int itemCode = rs.getInt("ITEM_CODE");
				int priceZoneId = rs.getInt("PRICE_ZONE_ID");
				String storeNo = rs.getString("STORE_NUM");
				int locationId = rs.getInt("LOCATION_ID");	
				
				if (itemStoreLockMap.containsKey(itemCode)) {
					HashMap<Integer, Set<String>> zoneStoreMap = itemStoreLockMap.get(itemCode);
					if (zoneStoreMap.containsKey(priceZoneId)) {
						Set<String> stores = zoneStoreMap.get(priceZoneId);
						stores.add(storeNo+";"+locationId);
						zoneStoreMap.put(priceZoneId, stores);
					} else {
						Set<String> stores = new HashSet<>();
						stores.add(storeNo+";"+locationId);
						zoneStoreMap.put(priceZoneId, stores);
					}
					itemStoreLockMap.put(itemCode, zoneStoreMap);
				} else {
					HashMap<Integer, Set<String>> zoneStoreMap = new HashMap<>();
					Set<String> stores = new HashSet<>();
					stores.add(storeNo+";"+locationId);
					zoneStoreMap.put(priceZoneId, stores);
					itemStoreLockMap.put(itemCode, zoneStoreMap);
				}
			}
		} catch (SQLException ex) {
			logger.error("getExcludeStoresByItemsAndStoreLock() - Error when getting excluded stores from itemList - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return itemStoreLockMap;
	}
	
	private HashMap<Integer, HashMap<Integer,List<PriceExportDTO>>> retrieveLockedStoresForItemsV2(Connection conn, String priceExportType, int lockListTypeId, Set<Integer> zones,
			int storeLockLocationLevelId, Object... values) {
		
		HashMap<Integer, HashMap<Integer,List<PriceExportDTO>>> itemStoreLockMap = new HashMap<>();
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			//for store level id
			StringBuilder db = new StringBuilder();
			db.append(" SELECT CS.COMP_STR_NO AS STORE_NUM, PCL.LOCATION_ID, ");
			db.append(" PCLI.STORE_ID, CS.PRICE_ZONE_ID, PCLI.ITEM_CODE AS ITEM_CODE, ");
			db.append(" PCLI.END_DATE FROM PRICE_CHECK_LIST_ITEMS PCLI LEFT JOIN COMPETITOR_STORE CS ON PCLI.STORE_ID = CS.COMP_STR_ID AND CS.ACTIVE_INDICATOR = 'Y' ");
			db.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			db.append(" left join price_check_list pcl on pcli.price_check_list_id = pcl.id ");
			db.append(" WHERE PCLI.ITEM_CODE IN (%s) ");
			db.append(" AND PCLI.STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE ");
			db.append(
					" WHERE PRICE_ZONE_ID IN (%ZONES%) AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) ");
			db.append(
					" AND PCLI.PRICE_CHECK_LIST_ID IN (SELECT ID FROM PRICE_CHECK_LIST WHERE PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST%) ");
			//db.append(" AND (PCLI.END_DATE > SYSDATE OR PCLI.END_DATE IS NULL) ");
			db.append(" AND (PCLI.END_DATE > TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy')");
			db.append(" OR PCLI.END_DATE IS NULL) ");
			if (priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
				db.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if (priceExportType.equals(Constants.HARD_PART_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				db.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}
			db.append(" ");
			
			//for store list level id
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT CS.COMP_STR_NO AS STORE_NUM, PCL.LOCATION_ID, ");
			sb.append(" PCLI.ITEM_CODE AS ITEM_CODE, CS.PRICE_ZONE_ID FROM PRICE_CHECK_LIST_ITEMS PCLI ");
			sb.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			sb.append(" LEFT JOIN PRICE_CHECK_LIST PCL ON PCLI.PRICE_CHECK_LIST_ID = PCL.ID ");
			sb.append(" LEFT JOIN LOCATION_GROUP_RELATION LGR ON PCL.LOCATION_LEVEL_ID = LGR.LOCATION_LEVEL_ID ");
			sb.append(" AND PCL.LOCATION_ID = LGR.LOCATION_ID ");
			sb.append(" LEFT JOIN COMPETITOR_STORE CS ON LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID ");
			sb.append(" WHERE PCLI.ITEM_CODE IN (%s) AND LGR.CHILD_LOCATION_ID IN ");
			sb.append(" (SELECT COMP_STR_ID FROM COMPETITOR_STORE ");
			sb.append(" WHERE PRICE_ZONE_ID IN (%ZONES%)  AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) ");
			sb.append(" AND PCL.PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST% AND LGR.LOCATION_LEVEL_ID = %LOC_LEVEL% ");
			sb.append(" AND (PCLI.END_DATE > TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy')");
			sb.append(" OR PCLI.END_DATE IS NULL)");
			if (priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if (priceExportType.equals(Constants.HARD_PART_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}
			
			if (storeLockLocationLevelId == Constants.STORE_LEVEL_ID) {
				String sql = db.toString()
						.replaceAll("%ZONES%", PRCommonUtil.getCommaSeperatedStringFromIntSet(zones))
						.replaceAll("%CHECK_LIST%", String.valueOf(lockListTypeId));
				
				stmt = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
				logger.debug("retrieveLockedStoresForItemsV2()-sql of store lock items at store level: " + sql);
				PristineDBUtil.setValues(stmt, values);
			} else if (storeLockLocationLevelId == Constants.STORE_LIST_LEVEL_ID) {
				String sql = sb.toString()
						.replaceAll("%ZONES%", PRCommonUtil.getCommaSeperatedStringFromIntSet(zones))
						.replaceAll("%CHECK_LIST%", String.valueOf(lockListTypeId))
						.replaceAll("%LOC_LEVEL%", String.valueOf(Constants.STORE_LIST_LEVEL_ID));
				stmt = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
				logger.debug("retrieveLockedStoresForItemsV2()- sql of store lock items at store list level: "  + sql);
				PristineDBUtil.setValues(stmt, values);
			}

			stmt.setFetchSize(2000000);
			rs = stmt.executeQuery();

			while (rs.next()) {
				PriceExportDTO dto = new PriceExportDTO();
				dto.setItemCode(rs.getInt("ITEM_CODE"));
				dto.setPriceZoneId(rs.getInt("PRICE_ZONE_ID"));
				dto.setStoreNo(rs.getString("STORE_NUM"));
				dto.setLocationId(rs.getInt("LOCATION_ID"));	
				
				if (itemStoreLockMap.containsKey(dto.getItemCode())) {
					HashMap<Integer, List<PriceExportDTO>> zoneStoreMap = itemStoreLockMap.get(dto.getItemCode());
					if (zoneStoreMap.containsKey(dto.getPriceZoneId())) {
						List<PriceExportDTO> storeData = zoneStoreMap.get(dto.getPriceZoneId());
						Set<String> stores = storeData.get(0).getStoreNums();
						stores.add(dto.getStoreNo());
						dto.setStoreNums(stores);
						List<PriceExportDTO> tempList = new ArrayList<>();
						tempList.add(dto);
						zoneStoreMap.put(dto.getPriceZoneId(), tempList);
					} else {						
						Set<String> stores = new HashSet<>();
						stores.add(dto.getStoreNo());
						dto.setStoreNums(stores);
						List<PriceExportDTO> tempList = new ArrayList<>();
						tempList.add(dto);
						zoneStoreMap.put(dto.getPriceZoneId(), tempList);
					}
					itemStoreLockMap.put(dto.getItemCode(), zoneStoreMap);
				} else {
					HashMap<Integer, List<PriceExportDTO>> zoneStoreMap = new HashMap<>();
					List<PriceExportDTO> storeData = new ArrayList<>();
					Set<String> stores = new HashSet<>();
					stores.add(dto.getStoreNo());
					dto.setStoreNums(stores);
					storeData.add(dto);
					zoneStoreMap.put(dto.getPriceZoneId(), storeData);
					itemStoreLockMap.put(dto.getItemCode(), zoneStoreMap);					
				}
			}
		} catch (SQLException ex) {
			logger.error("getExcludeStoresByItemsAndStoreLock() - Error when getting excluded stores from itemList - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return itemStoreLockMap;
	}
	
	private HashMap<Integer, HashMap<Integer,List<PRItemDTO>>> retrieveLockedStoresForItemsV3(Connection conn, String priceExportType, int lockListTypeId, Set<Integer> zones,
			int storeLockLocationLevelId, Object... values) {
		
		HashMap<Integer, HashMap<Integer,List<PRItemDTO>>> itemStoreLockMap = new HashMap<>();
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			//for store level id
			StringBuilder db = new StringBuilder();
			db.append(" SELECT CS.COMP_STR_NO AS STORE_NUM, PCL.LOCATION_ID, ");
			db.append(" PCLI.STORE_ID, CS.PRICE_ZONE_ID, PCLI.ITEM_CODE AS ITEM_CODE, ");
			db.append(" PCLI.END_DATE FROM PRICE_CHECK_LIST_ITEMS PCLI LEFT JOIN COMPETITOR_STORE CS ON PCLI.STORE_ID = CS.COMP_STR_ID AND CS.ACTIVE_INDICATOR = 'Y' ");
			db.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			db.append(" left join price_check_list pcl on pcli.price_check_list_id = pcl.id ");
			db.append(" WHERE PCLI.ITEM_CODE IN (%s) ");
			db.append(" AND PCLI.STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE ");
			db.append(
					" WHERE PRICE_ZONE_ID IN (%ZONES%) AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) ");
			db.append(
					" AND PCLI.PRICE_CHECK_LIST_ID IN (SELECT ID FROM PRICE_CHECK_LIST WHERE PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST%) ");
			//db.append(" AND (PCLI.END_DATE > SYSDATE OR PCLI.END_DATE IS NULL) ");
			db.append(" AND (PCLI.END_DATE > TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy')");
			db.append(" OR PCLI.END_DATE IS NULL) ");
			if (priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
				db.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if (priceExportType.equals(Constants.HARD_PART_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				db.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}
			db.append(" ");
			
			//for store list level id
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT CS.COMP_STR_NO AS STORE_NUM, PCL.LOCATION_ID, ");
			sb.append(" PCLI.ITEM_CODE AS ITEM_CODE, CS.PRICE_ZONE_ID FROM PRICE_CHECK_LIST_ITEMS PCLI ");
			sb.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			sb.append(" LEFT JOIN PRICE_CHECK_LIST PCL ON PCLI.PRICE_CHECK_LIST_ID = PCL.ID ");
			sb.append(" LEFT JOIN LOCATION_GROUP_RELATION LGR ON PCL.LOCATION_LEVEL_ID = LGR.LOCATION_LEVEL_ID ");
			sb.append(" AND PCL.LOCATION_ID = LGR.LOCATION_ID ");
			sb.append(" LEFT JOIN COMPETITOR_STORE CS ON LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID ");
			sb.append(" WHERE PCLI.ITEM_CODE IN (%s) AND LGR.CHILD_LOCATION_ID IN ");
			sb.append(" (SELECT COMP_STR_ID FROM COMPETITOR_STORE ");
			sb.append(" WHERE PRICE_ZONE_ID IN (%ZONES%)  AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) ");
			sb.append(" AND PCL.PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST% AND LGR.LOCATION_LEVEL_ID = %LOC_LEVEL% ");
			sb.append(" AND (PCLI.END_DATE > TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy')");
			sb.append(" OR PCLI.END_DATE IS NULL)");
			if (priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if (priceExportType.equals(Constants.HARD_PART_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}
			
			if (storeLockLocationLevelId == Constants.STORE_LEVEL_ID) {
				String sql = db.toString()
						.replaceAll("%ZONES%", PRCommonUtil.getCommaSeperatedStringFromIntSet(zones))
						.replaceAll("%CHECK_LIST%", String.valueOf(lockListTypeId));
				
				stmt = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
				logger.debug("sql of store lock items at store level: " + sql);
				PristineDBUtil.setValues(stmt, values);
			} else if (storeLockLocationLevelId == Constants.STORE_LIST_LEVEL_ID) {
				String sql = sb.toString()
						.replaceAll("%ZONES%", PRCommonUtil.getCommaSeperatedStringFromIntSet(zones))
						.replaceAll("%CHECK_LIST%", String.valueOf(lockListTypeId))
						.replaceAll("%LOC_LEVEL%", String.valueOf(Constants.STORE_LIST_LEVEL_ID));
				stmt = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
				logger.debug("sql of store lock items at store list level: "  + sql);
				PristineDBUtil.setValues(stmt, values);
			}

			stmt.setFetchSize(50000);
			rs = stmt.executeQuery();

			while (rs.next()) {
				PRItemDTO dto = new PRItemDTO();
				dto.setItemCode(rs.getInt("ITEM_CODE"));
				dto.setPriceZoneId(rs.getInt("PRICE_ZONE_ID"));
				dto.setStoreNo(rs.getString("STORE_NUM"));
				dto.setLocationId(rs.getInt("LOCATION_ID"));	
				
				if (itemStoreLockMap.containsKey(dto.getItemCode())) {
					HashMap<Integer, List<PRItemDTO>> zoneStoreMap = itemStoreLockMap.get(dto.getItemCode());
					if (zoneStoreMap.containsKey(dto.getPriceZoneId())) {
						List<PRItemDTO> storeData = zoneStoreMap.get(dto.getPriceZoneId());
						Set<String> stores = storeData.get(0).getStoreNums();
						stores.add(dto.getStoreNo());
						dto.setStoreNums(stores);
						List<PRItemDTO> tempList = new ArrayList<>();
						tempList.add(dto);
						zoneStoreMap.put(dto.getPriceZoneId(), tempList);
					} else {						
						Set<String> stores = new HashSet<>();
						stores.add(dto.getStoreNo());
						dto.setStoreNums(stores);
						List<PRItemDTO> tempList = new ArrayList<>();
						tempList.add(dto);
						zoneStoreMap.put(dto.getPriceZoneId(), tempList);
					}
					itemStoreLockMap.put(dto.getItemCode(), zoneStoreMap);
				} else {
					HashMap<Integer, List<PRItemDTO>> zoneStoreMap = new HashMap<>();
					List<PRItemDTO> storeData = new ArrayList<>();
					Set<String> stores = new HashSet<>();
					stores.add(dto.getStoreNo());
					dto.setStoreNums(stores);
					storeData.add(dto);
					zoneStoreMap.put(dto.getPriceZoneId(), storeData);
					itemStoreLockMap.put(dto.getItemCode(), zoneStoreMap);					
				}
			}
		} catch (SQLException ex) {
			logger.error("getExcludeStoresByItemsAndStoreLock() - Error when getting excluded stores from itemList - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return itemStoreLockMap;
	}
/**
 * Gathers items from all currently active price check lists of price check list type either emergency(28) or clearance(29)
 * @param conn java.sql.Connection object to access the database
 * @return List of PriceExportDTO objects. Each PriceExportDTO object contains data for either emergency or clearance approvals of items 
 * for the current week.
 */
	public List<PriceExportDTO> getEmergencyAndClearanceItems(Connection conn) {
		List<PriceExportDTO> clearanceItemlist = new ArrayList<>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
		StringBuilder sb = new StringBuilder();
		sb.append(" SELECT PCLI.ITEM_CODE, PCLI.EC_RETAIL, TO_CHAR(PCLI.START_DATE, 'MM/DD/YYYY HH24:MI:SS') START_DATE, TO_CHAR(PCLI.END_DATE, 'MM/DD/YYYY HH24:MI:SS') END_DATE, PCLI.PRICE_CHECK_LIST_ID, IL.RETAILER_ITEM_CODE,");
		sb.append(" PCL.CREATE_USER_ID, PCL.PRICE_CHECK_LIST_TYPE_ID, CS.COMP_STR_NO, CS.COMP_STR_ID, PCLI.ZONE_NUM, PCLI.DEFINED_BY, UD.FIRST_NAME, UD.LAST_NAME, ");
		sb.append(" IL.USER_ATTR_6, IL.USER_ATTR_3");
		sb.append(" FROM PRICE_CHECK_LIST_ITEMS PCLI LEFT JOIN PRICE_CHECK_LIST PCL ON PCLI.PRICE_CHECK_LIST_ID = PCL.ID ");
		sb.append(" LEFT JOIN COMPETITOR_STORE CS ON PCLI.STORE_ID = CS.COMP_STR_ID AND CS.ACTIVE_INDICATOR = 'Y' ");
		sb.append(" LEFT JOIN RETAIL_PRICE_ZONE ZN ON PCLI.ZONE_NUM = ZN.ZONE_NUM AND ZN.ACTIVE_INDICATOR = 'Y' ");
		sb.append(" LEFT JOIN ITEM_LOOKUP IL ON PCLI.ITEM_CODE = IL.ITEM_CODE ");
		sb.append(" LEFT JOIN USER_DETAILS UD ON PCLI.DEFINED_BY = UD.USER_ID ");
		sb.append(" WHERE PCL.PRICE_CHECK_LIST_TYPE_ID IN (").append(Integer.parseInt(Constants.CLEARANCE_LIST_TYPE));
		sb.append(",").append(Integer.parseInt(Constants.EMERGENCY_LIST_TYPE)).append(")");
		sb.append(" AND ((PCLI.START_DATE >= TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy')");
		sb.append(" AND PCLI.START_DATE <= TO_DATE('").append(DateUtil.getSpecificDate(7)).append("','MM/dd/yyyy')");
		sb.append(" ) OR PCLI.START_DATE IS NULL)");
		sb.append(" AND (PCLI.IS_EXPORTED = 'N' OR PCLI.IS_EXPORTED IS NULL) ");
	
		
		logger.debug("getEmergencyAndClearanceItems() - query: " +sb.toString());
		
		stmt = conn.prepareStatement(sb.toString());

		rs = stmt.executeQuery();
		while (rs.next()) {			
			PriceExportDTO dto = new PriceExportDTO();
			dto.setItemCode(rs.getInt("ITEM_CODE"));
			dto.setPartNumber(rs.getString("USER_ATTR_3"));
			dto.setItemType(rs.getString("USER_ATTR_6"));
			dto.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
			dto.setECRetail(rs.getDouble("EC_RETAIL"));
			dto.setStartDate(rs.getString("START_DATE"));
			dto.setEndDate(rs.getString("END_DATE"));
			dto.setPriceCheckListId(rs.getInt("PRICE_CHECK_LIST_ID"));
			dto.setPriceCheckListTypeId(rs.getInt("PRICE_CHECK_LIST_TYPE_ID"));
			dto.setApprovedBy(rs.getString("CREATE_USER_ID"));
			dto.setStoreNo(rs.getString("COMP_STR_NO"));
			dto.setStoreId(rs.getString("COMP_STR_ID"));
			dto.setPriceZoneNo(rs.getString("ZONE_NUM"));
			dto.setApprovedBy(rs.getString("DEFINED_BY"));
			if(dto.getApprovedBy() == null) {
				dto.setApprovedBy("");
			}
			if(rs.getString("FIRST_NAME") == null) {
				dto.setApproverName("");
			}else {
			dto.setApproverName(rs.getString("FIRST_NAME") + " " +rs.getString("LAST_NAME"));
			}
			clearanceItemlist.add(dto);
			
		}
		}catch(Exception ex) {
			logger.error("getEmergencyAndClearanceItems() - Error when getting Emergency and Clearance Items from itemList - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		return clearanceItemlist;
	}

	public List<PRItemDTO> getEmergencyAndClearanceItemsV3(Connection conn) {
		List<PRItemDTO> clearanceItemlist = new ArrayList<>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
		StringBuilder sb = new StringBuilder();
		sb.append(" SELECT PCLI.ITEM_CODE, PCLI.EC_RETAIL, TO_CHAR(PCLI.START_DATE, 'MM/DD/YYYY HH24:MI:SS') START_DATE, TO_CHAR(PCLI.END_DATE, 'MM/DD/YYYY HH24:MI:SS') END_DATE, PCLI.PRICE_CHECK_LIST_ID, IL.RETAILER_ITEM_CODE,");
		sb.append(" PCL.CREATE_USER_ID, PCL.PRICE_CHECK_LIST_TYPE_ID, CS.COMP_STR_NO, CS.COMP_STR_ID, PCLI.ZONE_NUM, PCLI.DEFINED_BY, UD.FIRST_NAME, UD.LAST_NAME, ");
		sb.append(" IL.USER_ATTR_6, IL.USER_ATTR_3");
		sb.append(" FROM PRICE_CHECK_LIST_ITEMS PCLI LEFT JOIN PRICE_CHECK_LIST PCL ON PCLI.PRICE_CHECK_LIST_ID = PCL.ID ");
		sb.append(" LEFT JOIN COMPETITOR_STORE CS ON PCLI.STORE_ID = CS.COMP_STR_ID AND CS.ACTIVE_INDICATOR = 'Y' ");
		sb.append(" LEFT JOIN RETAIL_PRICE_ZONE ZN ON PCLI.ZONE_NUM = ZN.ZONE_NUM AND ZN.ACTIVE_INDICATOR = 'Y' ");
		sb.append(" LEFT JOIN ITEM_LOOKUP IL ON PCLI.ITEM_CODE = IL.ITEM_CODE ");
		sb.append(" LEFT JOIN USER_DETAILS UD ON PCLI.DEFINED_BY = UD.USER_ID ");
		sb.append(" WHERE PCL.PRICE_CHECK_LIST_TYPE_ID IN (").append(Integer.parseInt(Constants.CLEARANCE_LIST_TYPE));
		sb.append(",").append(Integer.parseInt(Constants.EMERGENCY_LIST_TYPE)).append(")");
		sb.append(" AND ((PCLI.START_DATE >= TO_DATE('").append(DateUtil.getSpecificDate(0)).append("','MM/dd/yyyy')");
		sb.append(" AND PCLI.START_DATE <= TO_DATE('").append(DateUtil.getSpecificDate(7)).append("','MM/dd/yyyy')");
		sb.append(" ) OR PCLI.START_DATE IS NULL)");
		sb.append(" AND (PCLI.IS_EXPORTED = 'N' OR PCLI.IS_EXPORTED IS NULL) ");
	
		
		logger.debug("getEmergencyAndClearanceItems() - query: " +sb.toString());
		
		stmt = conn.prepareStatement(sb.toString());

		rs = stmt.executeQuery();
		while (rs.next()) {			
			PRItemDTO dto = new PRItemDTO();
			dto.setItemCode(rs.getInt("ITEM_CODE"));
			dto.setPartNumber(rs.getString("USER_ATTR_3"));
			dto.setItemType(rs.getString("USER_ATTR_6"));
			dto.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
			dto.setECRetail(rs.getDouble("EC_RETAIL"));
			dto.setStartDate(rs.getString("START_DATE"));
			dto.setEndDate(rs.getString("END_DATE"));
			dto.setPriceCheckListId(rs.getInt("PRICE_CHECK_LIST_ID"));
			dto.setPriceCheckListTypeId(rs.getInt("PRICE_CHECK_LIST_TYPE_ID"));
			dto.setApprovedBy(rs.getString("CREATE_USER_ID"));
			if(rs.getString("COMP_STR_NO") == null) {
				dto.setStoreNo("");
			}
			else {
				dto.setStoreNo(rs.getString("COMP_STR_NO"));
			}
			dto.setStoreId(rs.getString("COMP_STR_ID"));
			
			if(rs.getString("ZONE_NUM") == null) {
				dto.setPriceZoneNo("");
			}
			else {
				dto.setPriceZoneNo(rs.getString("ZONE_NUM"));
			}
			dto.setApprovedBy(rs.getString("DEFINED_BY"));
			if(dto.getApprovedBy() == null) {
				dto.setApprovedBy("");
			}
			if(rs.getString("FIRST_NAME") == null) {
				dto.setApproverName("");
			}else {
			dto.setApproverName(rs.getString("FIRST_NAME") + " " +rs.getString("LAST_NAME"));
			}
			clearanceItemlist.add(dto);
			
		}
		}catch(Exception ex) {
			logger.error("getEmergencyAndClearanceItems() - Error when getting Emergency and Clearance Items from itemList - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		return clearanceItemlist;
	}
	
	/**This method creates and executes one query per entry of the clearanceItems set.
	 * A batch of 1000 such queries is created and then executed before moving to the next 1000 set entries.
	 * Effectively this results in clearanceItems.size() number of queries being executed that update the PRICE_CHECK_LIST_ITEMS table.
	 * 
	 * @param conn A java.sql.Connection instance for accessing the connection (session) with the database.
	 * @param clearanceItems A set of strings where the strings are of the format intem_code;price_check_list_id
	 * */
	public void updateClearanceItemsStatus(Connection conn, Set<String> clearanceItems) throws GeneralException {
	
		PreparedStatement stmt = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE = ? ");
			sb.append(" AND PRICE_CHECK_LIST_ID = ? ");
			
			stmt = conn.prepareStatement(sb.toString());
			logger.debug("updateClearanceItemsStatus() Query- " + sb.toString()  +" Total Items to be updated " + clearanceItems.size());
			
			String[] tempItemEntrySplit;
			int colIndex, itemCountInBatch = 0;
			for (String itemEntry : clearanceItems) {
				itemCountInBatch++;
				colIndex = 0;
				tempItemEntrySplit = itemEntry.split(";");
				stmt.setInt(++colIndex, Integer.parseInt(tempItemEntrySplit[0]));
				stmt.setInt(++colIndex, Integer.parseInt(tempItemEntrySplit[1]));
				stmt.addBatch();
			
				if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					itemCountInBatch = 0;
				}
			}

			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}			
			
		} catch (SQLException e) {
			logger.error("updateClearanceItemsStatus() - Error updating clearance Items");
			throw new GeneralException("updateClearanceItemsStatus() - Error updating clearance Items", e);
		} catch (NumberFormatException nfe) {
			logger.error("updateClearanceItemsStatus() - Error while parsing item code or price check list ID!");
			throw new GeneralException("updateClearanceItemsStatus() - Error while parsing item code or price check list ID!", nfe);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}
	
	/**
	 * This method updates the PRICE_CHECK_LIST_ITEMS table for item codes in each of the price check lists.
	 * It does this by creating a price-check-list-wise batch of queries such that each query tries to update no more than 1000 items.
	 * In the worst case scenario this will create a batch of [no. of price check lists] X {[max(sizes of check lists) / 1000] + 1} queries 
	 * where [no. of price check lists] X [max(sizes of check lists) / 1000] queries will update 1000 items 
	 * and [no. of price check lists] queries will update a least 1 and at most 999 items.
	 * For eg.: If we have 15 price check lists of type emergency or clearance with 6789 items each then the batch will have 105 queries.
	 * 15 X 6 = 90 queries that update 1000 items each and 15 queries that update 789 items each.
	 * @param conn A java.sql.Connection instance for accessing the connection (session) with the database.
	 * @param clearanceItems A set of strings where the strings are of the format intem_code;price_check_list_id
	 */
	public int[] updateClearanceItemsStatusInBatches(Connection conn, Set<String> clearanceItems) throws GeneralException {
		logger.debug("updateClearanceItemsStatusInBatches() - Total item_code-price_check_list_ID combinations to be updated: " + clearanceItems.size());
		Map<String, List<String>> priceCheckListIDToItemCodesMap = new HashMap<>();
		String[] tempClearanceItemSplit;
		List<String> tempItemCodesList;
		for(String clearanceItem : clearanceItems) {
			tempClearanceItemSplit = clearanceItem.split(";");
			if(priceCheckListIDToItemCodesMap.containsKey(tempClearanceItemSplit[1])) {
				priceCheckListIDToItemCodesMap.get(tempClearanceItemSplit[1])
				.add(tempClearanceItemSplit[0]);
			}
			else {
				tempItemCodesList = new ArrayList<String>();
				tempItemCodesList.add(tempClearanceItemSplit[0]);
				priceCheckListIDToItemCodesMap.put(tempClearanceItemSplit[1], tempItemCodesList);
			}
		}
		try {
			PreparedStatement stmt = conn.prepareStatement("UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE IN (?) AND PRICE_CHECK_LIST_ID = ?");
			boolean atLeastOnePriceCheckListHasMoreItems;
			int multipleOf1000 = 1;
			int startInclusive, endExclusive, totalItemCodes;
			String commaJoinedItemCoeds;
			do{
				atLeastOnePriceCheckListHasMoreItems = false;
				for(Map.Entry<String, List<String>> priceCheckListIDToItemCode : priceCheckListIDToItemCodesMap.entrySet()) {
					totalItemCodes = priceCheckListIDToItemCode.getValue().size();
					startInclusive = 1000 * (multipleOf1000 - 1);
					if(totalItemCodes<=startInclusive)
						break;
					endExclusive = 1000 * multipleOf1000;
					if(totalItemCodes<=endExclusive)
						endExclusive = totalItemCodes;
					else
						atLeastOnePriceCheckListHasMoreItems = true;
					tempItemCodesList = priceCheckListIDToItemCode.getValue().subList(startInclusive, endExclusive);
					commaJoinedItemCoeds = String.join(",", tempItemCodesList);
					stmt.setString(1, commaJoinedItemCoeds);
					stmt.setString(2, priceCheckListIDToItemCode.getKey());
					logger.debug("updateClearanceItemsStatusInBatches() - UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE IN ("
					+ commaJoinedItemCoeds +
					") AND PRICE_CHECK_LIST_ID = "
					+ priceCheckListIDToItemCode.getKey());
					stmt.addBatch();
				}
				multipleOf1000++;
			} while(atLeastOnePriceCheckListHasMoreItems);
			int[]result = stmt.executeBatch();
			StringBuilder sb = new StringBuilder((2*result.length)-1);
			sb.append(result[0]);
			for(int i = 1 ; i<result.length ; i++)
				sb.append(",").append(result[i]);
			logger.debug("updateClearanceItemsStatus() - Query results are one of these integer values: No. of rows updated by successful execution of the query, value of SUCCESS_NO_INFO");
			logger.debug(sb.toString());
			return result;
		} catch (SQLException e) {
			logger.error("updateClearanceItemsStatus() - Error updating clearance Items");
			throw new GeneralException("updateClearanceItemsStatus() - Error updating clearance Items", e);
		}
	}

	public void clearRetailOfClearanceItems(Connection conn, List<PRItemDTO> ECItem) throws GeneralException {
		List<PRItemDTO> clearanceItem = ECItem.stream().filter(e -> e.getPriceCheckListTypeId() == 
				Integer.parseInt(Constants.CLEARANCE_LIST_TYPE)).collect(Collectors.toList());
		if(clearanceItem.size() > 0) {
		logger.debug("Clearing the retail of clearance Items..");
		PreparedStatement stmt = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET EC_RETAIL = ? WHERE IS_EXPORTED = 'Y' AND ITEM_CODE = ? ");
			sb.append(" AND PRICE_CHECK_LIST_ID = ?");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("clearRetailOfClearanceItems() Query- " + sb.toString());
			int itemCountInBatch = 0;

				for (PRItemDTO dto : clearanceItem) {
					itemCountInBatch++;
					int colIndex = 0;
					stmt.setNull(++colIndex, Types.NULL);
					stmt.setInt(++colIndex, dto.getItemCode());
					stmt.setInt(++colIndex, dto.getPriceCheckListId());
					// stmt.setString(++colIndex, dto.getEndDate());
					stmt.addBatch();

					if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						stmt.executeBatch();
						stmt.clearBatch();
						itemCountInBatch = 0;
					}
				}

			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("clearRetailOfClearanceItems() - Error clearing retail of clearance Items");
			throw new GeneralException("clearRetailOfClearanceItems() - Error clearing retail of clearance Items", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		}
	}

	public void insertECDataToHeader(Connection conn, Set<String> ECItems) throws GeneralException {
		PreparedStatement stmt = null;
		//HashMap<Integer, List<PRItemDTO>> mapByPriceCheckListId = (HashMap<Integer, List<PRItemDTO>>) ECItems.stream()
				//.collect(Collectors.groupingBy(PRItemDTO :: getPriceCheckListId));
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" INSERT INTO PR_EC_EXPORT_HEADER (EC_EXPORT_HEADER_ID, ITEM_LIST_ID, EXPORTED, EXPORTED_BY) ");
			sb.append(" VALUES (PR_EC_EXPORT_SEQ.NEXTVAL, ?, SYSDATE, ?) ");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("insertECDataToHeader() Query- " + sb.toString());
			int itemCountInBatch = 0;

			List<String> checkList = new ArrayList<>();

			/*
			 * for (Map.Entry<Integer, List<PRItemDTO>> entry :
			 * mapByPriceCheckListId.entrySet()) { List<PRItemDTO> valuesOfPriceCheckListId
			 * = entry.getValue(); if (!checkList.contains(entry.getKey() + ";" +
			 * valuesOfPriceCheckListId.get(0).getApprovedBy())) {
			 * checkList.add(entry.getKey() + ";" +
			 * valuesOfPriceCheckListId.get(0).getApprovedBy()); } }
			 */
			for (String dto : checkList) {
				itemCountInBatch++;
				int colIndex = 0;
				stmt.setInt(++colIndex, Integer.valueOf(dto.split(";")[0]));
				stmt.setString(++colIndex, dto.split(";")[1]);
				stmt.addBatch();
				
				if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					itemCountInBatch = 0;
				}

			}
			
			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("insertECDataToHeader() - Error updating clearance Items in to header");
			throw new GeneralException("insertECDataToHeader() - Error inserting clearance Items in to header", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	public void insertECDataToDetail(Connection conn, Set<String> clearanceItem) throws GeneralException, ParseException {
		
		List<PRItemDTO> dataFromItemList = getItemListDetail(conn, clearanceItem);
		
		List<PRItemDTO> ecTemp = new ArrayList<>();
		for (PRItemDTO dto : dataFromItemList) {
			ecTemp.add(dto);
			if (ecTemp.size() % 5000 == 0) {

				List<PRItemDTO> emptyStartDateList = ecTemp.stream()
						.filter(e -> e.getStartDate().length() < 0).filter(e -> e.getStartDate().isEmpty())
						.collect(Collectors.toList());
				List<PRItemDTO> startDateList = ecTemp.stream().filter(e -> e.getStartDate().length() > 0)
						.collect(Collectors.toList());

				if (emptyStartDateList.size() > 0) {
					insertECDataWithoutStartDate(conn, emptyStartDateList);
				}
				if (startDateList.size() > 0) {
					insertECDataWithStartDate(conn, startDateList);
				}
			}
		}
		if(ecTemp.size() > 0) {

			List<PRItemDTO> emptyStartDateList = ecTemp.stream()
					.filter(e -> e.getStartDate().length() < 0).filter(e -> e.getStartDate().isEmpty())
					.collect(Collectors.toList());
			List<PRItemDTO> startDateList = ecTemp.stream().filter(e -> e.getStartDate().length() > 0)
					.collect(Collectors.toList());

			if (emptyStartDateList.size() > 0) {
				insertECDataWithoutStartDate(conn, emptyStartDateList);
			}
			if (startDateList.size() > 0) {
				insertECDataWithStartDate(conn, startDateList);
			}
		}
	}

	private void insertECDataWithStartDate(Connection conn, List<PRItemDTO> startDateList) throws GeneralException, ParseException {
		PreparedStatement stmt = null;
		try {
		stmt = conn.prepareStatement(GET_SQL_FOR_DATA_WITH_START_DATE);
		logger.debug("insertECDataWithStartDate() Query: " + GET_SQL_FOR_DATA_WITH_START_DATE);
		int itemCountInBatch = 0;
			for (PRItemDTO dto : startDateList) {
				itemCountInBatch++;
				int colIndex = 0;
				logger.debug("header id: " + dto.getItemListHeaderId());
				logger.debug("checklist id: " + dto.getPriceCheckListId());
				logger.debug("item id: " + dto.getItemCode());
				logger.debug("store id: " + dto.getStoreId());
				logger.debug("comment: " + dto.getItemListComments());
				
				stmt.setInt(++colIndex, dto.getItemListHeaderId());
				stmt.setInt(++colIndex, dto.getPriceCheckListId());
				stmt.setInt(++colIndex, dto.getItemCode());
				stmt.setString(++colIndex, (dto.getStoreId() == null) ? null : dto.getStoreId());
				stmt.setString(++colIndex, (dto.getItemListComments() == null) ? null : dto.getItemListComments());
				stmt.setString(++colIndex, dto.getStartDate());
				try {
					String retail = String.valueOf(dto.getECRetail());
					if (!retail.isEmpty()) {
						stmt.setString(++colIndex, retail);
					}
				} catch (Exception e) {
					stmt.setNull(++colIndex, Types.NULL);
				}
				if (dto.getPriceZoneNo() == null) {
					stmt.setNull(++colIndex, Types.NULL);
				} else {
					stmt.setString(++colIndex, dto.getPriceZoneNo());
					logger.debug("zone num: " + dto.getPriceZoneNo());
				}
				stmt.addBatch();

				if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					itemCountInBatch = 0;
				}

			}
		if (itemCountInBatch > 0) {
			stmt.executeBatch();
			stmt.clearBatch();
		}
		
	} catch (SQLException e) {
		logger.error("insertECDataWithStartDate() - Error updating clearance Items in to Detail Table: " + e);
		throw new GeneralException("insertECDataWithStartDate() - Error inserting clearance Items in to Detail Table", e);
	} finally {
		PristineDBUtil.close(stmt);
	}
		
	}

	private void insertECDataWithoutStartDate(Connection conn, List<PRItemDTO> emptyStartDateList) throws GeneralException {
		PreparedStatement stmt = null;
		try {
		stmt = conn.prepareStatement(GET_SQL_FOR_EMPTY_START_DATE);
		logger.debug("insertECDataWithoutStartDate qry: "+ GET_SQL_FOR_EMPTY_START_DATE);
		int itemCountInBatch = 0;
			for (PRItemDTO dto : emptyStartDateList) {
				itemCountInBatch++;
				int colIndex = 0;
				stmt.setInt(++colIndex, dto.getItemListHeaderId());
				stmt.setInt(++colIndex, dto.getPriceCheckListId());
				stmt.setInt(++colIndex, dto.getItemCode());
				stmt.setString(++colIndex, (dto.getStoreId() == null) ? null : dto.getStoreId());
				stmt.setString(++colIndex, (dto.getItemListComments() == null) ? null : dto.getItemListComments());

				try {
					String retail = String.valueOf(dto.getECRetail());
					stmt.setString(++colIndex, retail);
				} catch (Exception e) {
					stmt.setNull(++colIndex, Types.NULL);
				}
				if (dto.getPriceZoneNo() == null) {
					stmt.setNull(++colIndex, Types.NULL);
				} else {
					stmt.setString(++colIndex, dto.getPriceZoneNo());
				}
				stmt.addBatch();

				if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					itemCountInBatch = 0;
				}
			}
			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		
	} catch (SQLException e) {
		logger.error("insertECDataWithoutStartDate() - Error inserting clearance Items in to Detail Table: " + e);
		throw new GeneralException("insertECDataWithoutStartDate() - Error inserting clearance Items in to Detail Table", e);
	} finally {
		PristineDBUtil.close(stmt);
	}
		
	}

	private List<PRItemDTO> getItemListDetail(Connection conn, Set<String> clearanceItems) {
		List<PRItemDTO> listItems = new ArrayList<PRItemDTO>();
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String[] tempClearanceItemSplit;
		for(String clearanceItem : clearanceItems) {
			tempClearanceItemSplit = clearanceItem.split(";");
			try {
				StringBuilder sb = new StringBuilder();
				sb.append(" SELECT PCLI.PRICE_CHECK_LIST_ID, PCLI.ITEM_CODE, PCLI.STORE_ID, PCLI.COMMENTS, TO_CHAR(PCLI.END_DATE, 'MM/DD/YYYY') AS END_DATE, TO_CHAR(PCLI.START_DATE, 'MM/DD/YYYY') AS START_DATE, ")
				.append(" PCLI.MIN_PRICE, PCLI.MAX_PRICE, PCLI.LOCKED_RETAIL, PCLI.IS_EXPORTED, PCLI.EC_RETAIL, PCLI.ZONE_NUM, ECH.EC_EXPORT_HEADER_ID ")
				.append(" FROM PRICE_CHECK_LIST_ITEMS PCLI LEFT JOIN PR_EC_EXPORT_HEADER ECH ON PCLI.PRICE_CHECK_LIST_ID = ECH.ITEM_LIST_ID ")
				.append(" WHERE PCLI.ITEM_CODE = ")
				.append(tempClearanceItemSplit[0])
				.append(" AND PCLI.PRICE_CHECK_LIST_ID = ")
				.append(tempClearanceItemSplit[1]);
				
				//logger.debug("getClearanceItems() - query: " +sb.toString());
				stmt = conn.prepareStatement(sb.toString());
		
				rs = stmt.executeQuery();
				while (rs.next()) {			
					PRItemDTO dto = new PRItemDTO();			
					dto.setItemCode(rs.getInt("ITEM_CODE"));
					dto.setECRetail(rs.getDouble("EC_RETAIL"));
					dto.setStartDate(rs.getString("START_DATE"));
					if(dto.getStartDate() == null)
						dto.setStartDate("");
					dto.setEndDate(rs.getString("END_DATE"));
					dto.setPriceCheckListId(rs.getInt("PRICE_CHECK_LIST_ID"));
					dto.setStoreId(rs.getString("STORE_ID"));
					dto.setItemListComments(rs.getString("COMMENTS"));
					dto.setMinRetail(rs.getDouble("MIN_PRICE"));
					dto.setMaxRetail(rs.getDouble("MAX_PRICE"));
					dto.setLockedRetail(rs.getDouble("LOCKED_RETAIL"));
					dto.setItemListHeaderId(rs.getInt("EC_EXPORT_HEADER_ID"));
					dto.setECRetail(rs.getDouble("EC_RETAIL"));
					listItems.add(dto);
				}
			}catch(Exception ex) {
				logger.error("getItemListDetail() - Error when getting itemList detail - "
						+ ex.getMessage());
			} finally {
				PristineDBUtil.close(rs);
				PristineDBUtil.close(stmt);
			}
		}
		return listItems;
	}

	public void updateEmergencyItemsStatus(Connection conn, List<PRItemDTO> emergencyItems) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE = ?");
			sb.append(" AND PRICE_CHECK_LIST_ID = ?");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("updateEmergencyItemsStatus() Query- " + sb.toString());
			int itemCountInBatch = 0;

			for (PRItemDTO dto : emergencyItems) {
				itemCountInBatch++;
				int colIndex = 0;
				stmt.setInt(++colIndex, dto.getItemCode());
				stmt.setInt(++colIndex, dto.getPriceCheckListId());
				stmt.addBatch();

				if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					itemCountInBatch = 0;
				}
			}
			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("updateEmergencyItemsStatus() - Error updating emergency Items");
			throw new GeneralException("updateEmergencyItemsStatus() - Error updating emergency Items", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	//Changes by Karishma on 03/29/2022
	//Modified the query ,added  RET_LIR_ID  column and filed in DTO for solving the issue of LIG row's reg eff date not updated
	public List<PriceExportDTO> getHardpartItemLeftoutV3(Connection conn, List<Long> runIdList) throws GeneralException {
		List<PriceExportDTO> hardpartItems = new ArrayList<PriceExportDTO>();
		
		StringBuilder runIdStr = new StringBuilder();
		for (int i = 0; i < runIdList.size(); i++) {
			long runId = runIdList.get(i);
			if (i == runIdList.size() - 1) {
				runIdStr.append(runId);
			} else {
				runIdStr.append(runId).append(",");
			}
		}
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT DISTINCT(EX.RUN_ID) AS RUN_ID, EX.PRICE_TYPE AS PRICE_TYPE, EX.ITEM_CODE AS PRODUCT_ID, RI.IS_EXPORTED, ");
			sb.append(" TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY') AS REG_EFF_DATE,RI.RET_LIR_ID ");
			sb.append(" FROM PR_PRICE_EXPORT EX LEFT JOIN PR_QUARTER_REC_ITEM RI ");
			sb.append(" ON RI.RUN_ID = EX.RUN_ID AND RI.PRODUCT_ID = EX.ITEM_CODE AND RI.PRODUCT_LEVEL_ID = 1");
			sb.append(" LEFT JOIN ITEM_LOOKUP IL ON RI.PRODUCT_ID = IL.ITEM_CODE ");
			sb.append(" WHERE RI.CAL_TYPE='Q' AND RI.PRODUCT_LEVEL_ID = 1 AND IL.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			sb.append(" AND (RI.IS_EXPORTED IS NULL OR RI.IS_EXPORTED = 'N') AND TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY') <= '");
			sb.append(DateUtil.getSpecificDate(0)).append("'");			
			sb.append(" AND EX.PRICE_TYPE = 'N' ");
			stmt = conn.prepareStatement(sb.toString());
			
			logger.debug("getHardpartItemLeftout() Query - " + sb.toString());

		rs = stmt.executeQuery();
		while (rs.next()) {			
			PriceExportDTO item = new PriceExportDTO();
			item.setRunId(rs.getLong("RUN_ID"));
			item.setPriceExportType(rs.getString("PRICE_TYPE"));
			item.setItemCode(rs.getInt("PRODUCT_ID"));
			item.setRegEffDate(rs.getString("REG_EFF_DATE"));
			item.setRetLirId(rs.getInt("RET_LIR_ID"));
			hardpartItems.add(item);
		}
		
		}catch(Exception ex) {
			logger.error("getHardpartItemLeftoutV3() - Error when getting leftout hardpart items in export queue - "
					+ ex.getMessage());
			throw new GeneralException("getHardpartItemLeftoutV3() - Error when getting leftout HP items in export queue -", ex);
		
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		return hardpartItems;
	}
	
	public List<PRItemDTO> getHardpartItemLeftout(Connection conn, List<Long> runIdList) {
		List<PRItemDTO> hardpartItems = new ArrayList<PRItemDTO>();
		
		StringBuilder runIdStr = new StringBuilder();
		for (int i = 0; i < runIdList.size(); i++) {
			long runId = runIdList.get(i);
			if (i == runIdList.size() - 1) {
				runIdStr.append(runId);
			} else {
				runIdStr.append(runId).append(",");
			}
		}
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT DISTINCT(EX.RUN_ID) AS RUN_ID, EX.PRICE_TYPE AS PRICE_TYPE, EX.ITEM_CODE AS PRODUCT_ID, RI.IS_EXPORTED, ");
			sb.append(" TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY') AS REG_EFF_DATE ");
			sb.append(" FROM PR_PRICE_EXPORT EX LEFT JOIN PR_QUARTER_REC_ITEM RI ");
			sb.append(" ON RI.RUN_ID = EX.RUN_ID AND RI.PRODUCT_ID = EX.ITEM_CODE AND RI.PRODUCT_LEVEL_ID = 1");
			sb.append(" LEFT JOIN ITEM_LOOKUP IL ON RI.PRODUCT_ID = IL.ITEM_CODE ");
			sb.append(" WHERE RI.CAL_TYPE='Q' AND RI.PRODUCT_LEVEL_ID = 1 AND IL.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			sb.append(" AND (RI.IS_EXPORTED IS NULL OR RI.IS_EXPORTED = 'N') AND TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY') <= '");
			sb.append(DateUtil.getSpecificDate(0)).append("'");			
			sb.append(" AND EX.PRICE_TYPE = 'N' ");
			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getHardpartItemLeftout() Query - " + sb.toString());

		rs = stmt.executeQuery();
		while (rs.next()) {			
			PRItemDTO item = new PRItemDTO();
			item.setRunId(rs.getLong("RUN_ID"));
			item.setPriceExportType(rs.getString("PRICE_TYPE"));
			item.setItemCode(rs.getInt("PRODUCT_ID"));
			item.setRegEffDate("REG_EFF_DATE");
			hardpartItems.add(item);
		}
		
		}catch(Exception ex) {
			logger.error("getHardpartItemLeftout() - Error when getting leftout hardpart items in export queue - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		return hardpartItems;
	}

	public void changeEffectiveDate(Connection conn, List<PRItemDTO> leftoutHardPartItems, int countOfIncrementingDays) throws GeneralException {
		PreparedStatement stmt = null;
		
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PR_QUARTER_REC_ITEM SET REG_EFF_DATE = SYSDATE + ").append(countOfIncrementingDays);
			sb.append(" WHERE PRODUCT_ID = ? AND CAL_TYPE='Q' AND PRODUCT_LEVEL_ID = 1 AND RUN_ID = ? ");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("changeEffectiveDate() Query- " + sb.toString());
			int itemCountInBatch = 0;

			for (PRItemDTO dto : leftoutHardPartItems) {
				itemCountInBatch++;
				int colIndex = 0;
				stmt.setInt(++colIndex, dto.getItemCode());
				stmt.setLong(++colIndex, dto.getRunId());
				stmt.addBatch();

				if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					itemCountInBatch = 0;
				}
			}
			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
			PristineDBUtil.commitTransaction(conn, "Price export update status");	
		} catch (SQLException e) {
			logger.error("changeEffectiveDate() - Error while changing effective date for hard part Items");
			throw new GeneralException("changeEffectiveDate() - Error while changing effective date for hard part Items", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		
	}
	
	public void changeEffectiveDateV3(Connection conn, List<PriceExportDTO> leftoutHardPartItems, int countOfIncrementingDays) throws GeneralException {
		PreparedStatement stmt = null;
	
		try {
			
			//Added code to set the postDated date in correct format by Karishma
			SimpleDateFormat formatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
			String postdatedDate = DateUtil.localDateToString(
					DateUtil.stringToLocalDate(formatter.format(new Date()), Constants.APP_DATE_FORMAT)
							.plusDays(countOfIncrementingDays),
					Constants.APP_DATE_FORMAT);

			java.util.Date javaDate = DateUtil.toSQLDate(postdatedDate);

			StringBuilder sb = new StringBuilder();
			
			sb.append(" UPDATE PR_QUARTER_REC_ITEM SET REG_EFF_DATE = ?");
			sb.append(" WHERE PRODUCT_ID = ? AND CAL_TYPE='Q' AND PRODUCT_LEVEL_ID = 1 AND RUN_ID = ? ");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("changeEffectiveDate() Query- " + sb.toString());
			int itemCountInBatch = 0;

			for (PriceExportDTO dto : leftoutHardPartItems) {
				itemCountInBatch++;
				int colIndex = 0;
				//Added code to set the postDated date in correct format in Database by Karishma
				stmt.setDate(++colIndex, (java.sql.Date) javaDate);
				stmt.setInt(++colIndex, dto.getItemCode());
				stmt.setLong(++colIndex, dto.getRunId());
				stmt.addBatch();

				if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					itemCountInBatch = 0;
					
				}
			}
			
			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}

			PristineDBUtil.commitTransaction(conn, "Price export update status");	
		} catch (SQLException e) {
			logger.error("changeEffectiveDate() - Error while changing effective date for hard part Items");
			throw new GeneralException("changeEffectiveDate() - Error while changing effective date for hard part Items", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		
	}

	public List<PRItemDTO> getSalesFloorItemsLeftout(Connection conn, List<PRItemDTO> salesFloorFiltered) {
		List<PRItemDTO> leftoutSalesfloorItems = new ArrayList<PRItemDTO>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT DISTINCT(EX.RUN_ID) AS RUN_ID, EX.PRICE_TYPE AS PRICE_TYPE, EX.ITEM_CODE AS PRODUCT_ID, RI.IS_EXPORTED,");
			sb.append(" TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY') AS REG_EFF_DATE ");
			sb.append(" FROM PR_PRICE_EXPORT EX LEFT JOIN PR_QUARTER_REC_ITEM RI ");
			sb.append(" ON RI.RUN_ID = EX.RUN_ID AND RI.PRODUCT_ID = EX.ITEM_CODE AND RI.PRODUCT_LEVEL_ID = 1");
			sb.append(" LEFT JOIN ITEM_LOOKUP IL ON RI.PRODUCT_ID = IL.ITEM_CODE ");
			sb.append(" WHERE IL.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("' AND (RI.IS_EXPORTED IS NULL OR RI.IS_EXPORTED = 'N')");
			sb.append(" AND RI.CAL_TYPE='Q' AND RI.PRODUCT_LEVEL_ID = 1 AND TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY') <= '");
			sb.append(DateUtil.getSpecificDate(6)).append("'");			
			sb.append(" AND EX.PRICE_TYPE = 'N' ");
			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getSalesFloorItemsLeftout() Query - " + sb.toString());

		rs = stmt.executeQuery();
		while (rs.next()) {			
			PRItemDTO item = new PRItemDTO();
			item.setRunId(rs.getLong("RUN_ID"));
			item.setPriceExportType(rs.getString("PRICE_TYPE"));
			item.setItemCode(rs.getInt("PRODUCT_ID"));
			item.setRegEffDate("REG_EFF_DATE");
			leftoutSalesfloorItems.add(item);
		}
		
		}catch(Exception ex) {
			logger.error("getHardpartItemLeftout() - Error when getting leftout hardpart items in export queue - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return leftoutSalesfloorItems;
	}
	
	//Changes by Karishma on 03/29/2022
	//Modified the query ,added  RET_LIR_ID  column and filed in DTO for solving the issue of LIG row's reg eff date not updated
	public List<PriceExportDTO> getSalesFloorItemsLeftoutV3(Connection conn, List<PriceExportDTO> salesFloorFiltered) throws GeneralException {
		List<PriceExportDTO> leftoutSalesfloorItems = new ArrayList<>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT DISTINCT(EX.RUN_ID) AS RUN_ID, EX.PRICE_TYPE AS PRICE_TYPE, EX.ITEM_CODE AS PRODUCT_ID, RI.IS_EXPORTED,");
			sb.append(" TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY') AS REG_EFF_DATE,RI.RET_LIR_ID ");
			sb.append(" FROM PR_PRICE_EXPORT EX LEFT JOIN PR_QUARTER_REC_ITEM RI ");
			sb.append(" ON RI.RUN_ID = EX.RUN_ID AND RI.PRODUCT_ID = EX.ITEM_CODE AND RI.PRODUCT_LEVEL_ID = 1");
			sb.append(" LEFT JOIN ITEM_LOOKUP IL ON RI.PRODUCT_ID = IL.ITEM_CODE ");
			sb.append(" WHERE IL.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("' AND (RI.IS_EXPORTED IS NULL OR RI.IS_EXPORTED = 'N')");
			sb.append(" AND RI.CAL_TYPE='Q' AND RI.PRODUCT_LEVEL_ID = 1 AND TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY') <= '");
			sb.append(DateUtil.getSpecificDate(6)).append("'");			
			sb.append(" AND EX.PRICE_TYPE = 'N' ");
			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getSalesFloorItemsLeftout() Query - " + sb.toString());

		rs = stmt.executeQuery();
			while (rs.next()) {
				PriceExportDTO item = new PriceExportDTO();
				item.setRunId(rs.getLong("RUN_ID"));
				item.setPriceExportType(rs.getString("PRICE_TYPE"));
				item.setItemCode(rs.getInt("PRODUCT_ID"));
				item.setRegEffDate(rs.getString("REG_EFF_DATE"));
				item.setRetLirId(rs.getInt("RET_LIR_ID"));
				leftoutSalesfloorItems.add(item);
			}
		
		}catch(Exception ex) {
			logger.error("getSalesFloorItemsLeftoutV3() - Error when getting leftout salesFloor items in export queue - "
					+ ex.getMessage());
			throw new GeneralException("getSalesFloorItemsLeftoutV3() - Error when getting leftout salesFloor items in export queue -", ex);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return leftoutSalesfloorItems;
	}
	
	//added by Bhargavi to update the max eff date for all the LIG items
	public List<PriceExportDTO> getAllSalesFloorItemsV3(Connection conn, List<PriceExportDTO> salesFloorFiltered) throws GeneralException {
		List<PriceExportDTO> allSalesfloorItems = new ArrayList<>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT DISTINCT(EX.RUN_ID) AS RUN_ID, EX.PRICE_TYPE AS PRICE_TYPE, EX.ITEM_CODE AS PRODUCT_ID, RI.IS_EXPORTED,");
			sb.append(" TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY') AS REG_EFF_DATE,RI.RET_LIR_ID ");
			sb.append(" FROM PR_PRICE_EXPORT EX LEFT JOIN PR_QUARTER_REC_ITEM RI ");
			sb.append(" ON RI.RUN_ID = EX.RUN_ID AND RI.PRODUCT_ID = EX.ITEM_CODE AND RI.PRODUCT_LEVEL_ID = 1");
			sb.append(" LEFT JOIN ITEM_LOOKUP IL ON RI.PRODUCT_ID = IL.ITEM_CODE ");
			sb.append(" WHERE IL.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("' AND (RI.IS_EXPORTED IS NULL OR RI.IS_EXPORTED = 'N')");
			sb.append(" AND RI.CAL_TYPE='Q' AND RI.PRODUCT_LEVEL_ID = 1 ");		
			sb.append(" AND EX.PRICE_TYPE = 'N' ");
			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getAllSalesFloorItems Query - " + sb.toString());

		rs = stmt.executeQuery();
			while (rs.next()) {
				PriceExportDTO item = new PriceExportDTO();
				item.setRunId(rs.getLong("RUN_ID"));
				item.setPriceExportType(rs.getString("PRICE_TYPE"));
				item.setItemCode(rs.getInt("PRODUCT_ID"));
				item.setRegEffDate(rs.getString("REG_EFF_DATE"));
				item.setRetLirId(rs.getInt("RET_LIR_ID"));
				allSalesfloorItems.add(item);
			}
		
		}catch(Exception ex) {
			logger.error("getSalesFloorItemsLeftoutV3() - Error when getting leftout salesFloor items in export queue - "
					+ ex.getMessage());
			throw new GeneralException("getSalesFloorItemsLeftoutV3() - Error when getting leftout salesFloor items in export queue -", ex);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return allSalesfloorItems;
	}

	public String getRecomName(Connection conn, int itemcode, int productLevelId) {
				
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String recName = "";
		try {
				StringBuilder sb = new StringBuilder();
				sb.append(" select pgcr.name from product_group pg ");
				sb.append(
						" left join product_group_relation_rec pgrr on pgrr.product_id = pg.product_id and pgrr.product_level_id = pg.product_level_id ");
				sb.append(
						" left join product_group pgcr on pgrr.child_product_level_id = pgcr.product_level_id and pgrr.child_product_id = pgcr.product_id ");
				sb.append(
						" where ( pgcr.product_id in (SELECT product_id FROM (SELECT * FROM PRODUCT_GROUP_RELATION_REC PGR ");
				sb.append(" START WITH CHILD_PRODUCT_LEVEL_ID = 1 AND CHILD_PRODUCT_ID in (")
						.append(itemcode).append(") ");
				sb.append(
						" CONNECT BY  PRIOR PRODUCT_ID = CHILD_PRODUCT_ID AND  PRIOR PRODUCT_LEVEL_ID = CHILD_PRODUCT_LEVEL_ID) ");
				sb.append(" WHERE PRODUCT_LEVEL_ID = ").append(productLevelId).append(" )) ");

				stmt = conn.prepareStatement(sb.toString());
				rs = stmt.executeQuery();
				while (rs.next()) {
					recName = rs.getString("name");
				}
			
		} catch (SQLException ex) {
			logger.info("getCategoryAndRecomUnitOfItem() - Error when getting Category/recommendation Unit of item - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return recName;
	}

	public HashMap<String, List<Long>> separateTestZoneAndRegularZoneRunIds(Connection conn, List<Long> runIdList) {
		
		HashMap<String, List<Long>> runIdMap = new HashMap<>();
		List<Long> testZoneRunIdList = new ArrayList<>();
		List<Long> normalRunIds = new ArrayList<>();
		List<String> stringConvertedRunIds = new ArrayList<String>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {			
			runIdList.forEach(runId -> {
				stringConvertedRunIds.add(String.valueOf(runId));
			});
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT PQRH.RUN_ID, RPZ.ZONE_TYPE FROM PR_QUARTER_REC_HEADER PQRH LEFT JOIN RETAIL_PRICE_ZONE RPZ ON PQRH.LOCATION_ID = RPZ.PRICE_ZONE_ID ");
			sb.append(" WHERE PQRH.RUN_ID IN (");
			sb.append(String.join(",", stringConvertedRunIds));
			sb.append(" )");
			
			logger.debug("separateTestZoneAndRegularZoneRunIds() QRY -:"+ sb.toString());
			stmt = conn.prepareStatement(sb.toString());
			rs = stmt.executeQuery();
			
			while(rs.next()) {
				Long runId = rs.getLong("RUN_ID");
				String zoneType = rs.getString("ZONE_TYPE");
				if("W".equalsIgnoreCase(zoneType) || "I".equalsIgnoreCase(zoneType)) {
					normalRunIds.add(runId);
				}
				else if("T".equalsIgnoreCase(zoneType)) {
					testZoneRunIdList.add(runId);
				}
				
			}
			runIdList.clear();
			//runIdList.addAll(normalRunIds);
			
			runIdMap.put("N", normalRunIds);//zn12,13,5
			runIdMap.put("T", testZoneRunIdList);//test zones
			
			logger.info("separateTestZoneAndRegularZoneRunIds ()- # of Test Zone runIds: " + testZoneRunIdList.size());								
			logger.info("separateTestZoneAndRegularZoneRunIds ()- # of Regular Zone runIds: " + normalRunIds.size());	
			
		}catch(Exception ex) {
			logger.error("separateTestZoneAndRegularZoneRunIds() - Error while separating Run Ids for Test Zones and Normal Zones - "+ex.getMessage());
		}
		finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return runIdMap;
		
	}

	public HashMap<Integer, List<PriceExportDTO>> getTestZoneStoreCombinationsDictionary(Connection conn, List<Long> testZoneRunIdList) 
	{
		HashMap<Integer, List<PriceExportDTO>> output = new HashMap<>();

		try {
			List<String> stringConvertedRunIds = new ArrayList<String>();
			testZoneRunIdList.forEach(runId -> {
				stringConvertedRunIds.add(String.valueOf(runId));
			});
			
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT DISTINCT PQRH.RUN_ID, LGR.CHILD_LOCATION_ID AS STORE_ID, PR.LOCATION_ID AS ZONE_ID, CS.COMP_STR_NO");
			sb.append(" ,RPZ.PRICE_ZONE_ID,RPZ.NAME,RPZ.ZONE_NUM");
			sb.append(" FROM LOCATION_GROUP_RELATION LGR ");
			sb.append(" LEFT JOIN PR_PRICE_TEST_REQUEST PR ");
			sb.append(" ON PR.SL_LEVEL_ID = LGR.LOCATION_LEVEL_ID");
			sb.append(" AND PR.SL_LOCATION_ID=LGR.LOCATION_ID");
			sb.append(" LEFT JOIN LOCATION_GROUP LG");
			sb.append(" ON LG.LOCATION_LEVEL_ID =LGR.LOCATION_LEVEL_ID");
			sb.append(" AND LG.LOCATION_ID      =LGR.LOCATION_ID");
		    sb.append(" LEFT JOIN PR_QUARTER_REC_HEADER PQRH");
			sb.append(" ON PR.LOCATION_ID = PQRH.LOCATION_ID");
			sb.append(" AND PR.LOCATION_LEVEL_ID=PQRH.LOCATION_LEVEL_ID");
			sb.append(" AND PR.PRODUCT_LEVEL_ID =PQRH.PRODUCT_LEVEL_ID");
			sb.append(" AND PR.PRODUCT_ID       =PQRH.PRODUCT_ID ");
			sb.append(" LEFT JOIN COMPETITOR_STORE CS ON ");
			sb.append(" LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID");
			sb.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID ");
			sb.append(" WHERE PQRH.RUN_ID IN ("+String.join(",", stringConvertedRunIds)+") AND PR.ACTIVE = 'Y'");
			logger.debug(sb.toString());
			PreparedStatement stmt = null;
			ResultSet rs = null;
			stmt = conn.prepareStatement("getTestZoneStoreCombinationsDictionary() qry: "+ sb.toString());
			rs = stmt.executeQuery();
			
			while(rs.next()) {
				Long RunId = rs.getLong("RUN_ID");
				String StoreNum = rs.getString("COMP_STR_NO");
				String StoreId = rs.getString("STORE_ID");
				int ZoneId = rs.getInt("PRICE_ZONE_ID");
				int TestZoneId = rs.getInt("ZONE_ID");
				String ZoneName = rs.getString("NAME");
				String ZoneNum = rs.getString("ZONE_NUM");
				
				PriceExportDTO temp = new PriceExportDTO();
				temp.setRunId(RunId);
				temp.setStoreNo(StoreNum);
				temp.setStoreId(StoreId);
				temp.setPriceZoneId(ZoneId);
				temp.setZoneName(ZoneName);
				temp.setPriceZoneNo(ZoneNum);
				if(output.containsKey(TestZoneId))
				{
					output.get(TestZoneId).add(temp);
				}else {
					List<PriceExportDTO> tempList = new ArrayList<>();
					tempList.add(temp);
					output.put(TestZoneId, tempList);
				}
			}
			
			
		}catch(Exception ex) {
			logger.error("getTestZoneStoreCombinationsDictionary() - Error in " + ex);
		}
		
		return output;
	}
	
	public HashMap<Integer, List<PRItemDTO>> getTestZoneStoreCombinationsDictionaryV3(Connection conn, List<Long> testZoneRunIdList) 
	{
		HashMap<Integer, List<PRItemDTO>> output = new HashMap<>();

		try {
			List<String> stringConvertedRunIds = new ArrayList<String>();
			testZoneRunIdList.forEach(runId -> {
				stringConvertedRunIds.add(String.valueOf(runId));
			});
			
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT DISTINCT PQRH.RUN_ID, LGR.CHILD_LOCATION_ID AS STORE_ID, PR.LOCATION_ID AS ZONE_ID, CS.COMP_STR_NO");
			sb.append(" ,RPZ.PRICE_ZONE_ID,RPZ.NAME,RPZ.ZONE_NUM");
			sb.append(" FROM LOCATION_GROUP_RELATION LGR ");
			sb.append(" LEFT JOIN PR_PRICE_TEST_REQUEST PR ");
			sb.append(" ON PR.SL_LEVEL_ID = LGR.LOCATION_LEVEL_ID");
			sb.append(" AND PR.SL_LOCATION_ID=LGR.LOCATION_ID");
			sb.append(" LEFT JOIN LOCATION_GROUP LG");
			sb.append(" ON LG.LOCATION_LEVEL_ID =LGR.LOCATION_LEVEL_ID");
			sb.append(" AND LG.LOCATION_ID      =LGR.LOCATION_ID");
		    sb.append(" LEFT JOIN PR_QUARTER_REC_HEADER PQRH");
			sb.append(" ON PR.LOCATION_ID = PQRH.LOCATION_ID");
			sb.append(" AND PR.LOCATION_LEVEL_ID=PQRH.LOCATION_LEVEL_ID");
			sb.append(" AND PR.PRODUCT_LEVEL_ID =PQRH.PRODUCT_LEVEL_ID");
			sb.append(" AND PR.PRODUCT_ID       =PQRH.PRODUCT_ID ");
			sb.append(" LEFT JOIN COMPETITOR_STORE CS ON ");
			sb.append(" LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID");
			sb.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID ");
			sb.append(" WHERE PQRH.RUN_ID IN ("+String.join(",", stringConvertedRunIds)+") AND PR.ACTIVE = 'Y'");
			logger.debug(sb.toString());
			PreparedStatement stmt = null;
			ResultSet rs = null;
			stmt = conn.prepareStatement(sb.toString());
			rs = stmt.executeQuery();
			
			while(rs.next()) {
				Long RunId = rs.getLong("RUN_ID");
				String StoreNum = rs.getString("COMP_STR_NO");
				String StoreId = rs.getString("STORE_ID");
				int ZoneId = rs.getInt("PRICE_ZONE_ID");
				int TestZoneId = rs.getInt("ZONE_ID");
				String ZoneName = rs.getString("NAME");
				String ZoneNum = rs.getString("ZONE_NUM");
				
				PRItemDTO temp = new PRItemDTO();
				temp.setRunId(RunId);
				temp.setStoreNo(StoreNum);
				temp.setStoreId(StoreId);
				temp.setPriceZoneId(ZoneId);
				temp.setZoneName(ZoneName);
				temp.setPriceZoneNo(ZoneNum);
				if(output.containsKey(TestZoneId))
				{
					output.get(TestZoneId).add(temp);
				}else {
					List<PRItemDTO> tempList = new ArrayList<>();
					tempList.add(temp);
					output.put(TestZoneId, tempList);
				}
			}
			
			
		}catch(Exception ex) {
			logger.error("getTestZoneStoreCombinationsDictionary() - Error in " + ex);
		}
		
		return output;
	}

	public String getCurrWeekEndDate(Connection conn) {
	String output ="";
		try {
			String TodaysDate ="";
			 DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM/dd/yyyy");  
			   LocalDateTime now = LocalDateTime.now();  
			   TodaysDate = dtf.format(now); 
				StringBuilder sb = new StringBuilder();
				sb.append("SELECT TO_CHAR(END_DATE,'MM/dd/yyyy') AS END_DATE ");
				sb.append("FROM RETAIL_CALENDAR WHERE START_DATE <= TO_DATE('"+TodaysDate+"','MM/dd/yyyy') ");
				sb.append("AND END_DATE >= TO_DATE('"+TodaysDate+"','MM/dd/yyyy') AND ROW_TYPE = 'W' ");
				logger.debug("Query for getCurrWeekEndDate(): "+sb.toString());
				PreparedStatement stmt = null;
				ResultSet rs = null;
				stmt = conn.prepareStatement(sb.toString());
				rs = stmt.executeQuery();
				while(rs.next()) {
					output = rs.getString("END_DATE");
				}
				
			   
		}
		catch(Exception ex) {
			logger.error("getCurrWeekEndDate() - Error in "+ex.getMessage());
		}
		
		return output;
	}

	public HashMap<String, String> getStoreIdForNum(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String chainId = PropertyManager.getProperty("PRESTO_SUBSCRIBER");
		HashMap<String, String> storeIdMap = new HashMap<String, String>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT COMP_STR_NO, COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = " + chainId);
			
			stmt = conn.prepareStatement(db.toString());
			logger.debug("Query for getStoreIdForNum() - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				storeIdMap.put((rs.getString("COMP_STR_NO")), rs.getString("COMP_STR_ID"));
			}
		} catch (SQLException ex) {
			logger.error("getStoreIdForNum() - Error when retrieving storeId For store Num - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return storeIdMap;
	}

	public List<PRItemDTO> getStoresOfZones(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String chainId = PropertyManager.getProperty("PRESTO_SUBSCRIBER");
		List<PRItemDTO> storeNumZoneId = new ArrayList<>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT COMP_STR_NO, PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = " + chainId);
			
			stmt = conn.prepareStatement(db.toString());
			logger.debug("Query for getStoreIdForNum() - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				PRItemDTO dto = new PRItemDTO();
				dto.setPriceZoneId(rs.getInt("PRICE_ZONE_ID"));
				dto.setStoreNo(rs.getString("COMP_STR_NO"));
				storeNumZoneId.add(dto);
			}
		} catch (SQLException ex) {
			logger.error("getStoreIdForNum() - Error when retrieving storeId For store Num - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return storeNumZoneId;
	}

	public List<PRItemDTO> getEffectiveDateOfItems(Connection conn, List<Long> normalRunIdList,
			List<Integer> approvedItemcodes) {
		List<PRItemDTO> approvedItems = new ArrayList<>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<String> itemCodeStr = new ArrayList<>();
		approvedItemcodes.forEach(item -> {
			itemCodeStr.add(String.valueOf(item));
		});
		
		List<String> stringConvertedRunIds = new ArrayList<String>();
		normalRunIdList.forEach(runId -> {
			stringConvertedRunIds.add(String.valueOf(runId));
		});
		
		try {
			StringBuffer sb = new StringBuffer();
			sb.append(" SELECT EX.ITEM_CODE AS PRODUCT_ID, RI.REC_REG_PRICE , RI.IS_EXPORTED,");
			sb.append(" TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY HH24:MI:SS') AS REG_EFF_DATE");
			sb.append(" FROM PR_PRICE_EXPORT EX LEFT JOIN PR_QUARTER_REC_ITEM RI ");
			sb.append(" ON RI.RUN_ID = EX.RUN_ID AND RI.PRODUCT_ID = EX.ITEM_CODE AND RI.PRODUCT_LEVEL_ID = 1");
			sb.append(" LEFT JOIN PR_QUARTER_REC_HEADER RH ON RI.RUN_ID = RH.RUN_ID  ");
			sb.append(" WHERE RI.CAL_TYPE='Q' AND RI.PRODUCT_LEVEL_ID = 1 ");			
			sb.append(" AND RI.RUN_ID IN (").append(String.join(",", stringConvertedRunIds)).append(")");
			sb.append(" AND EX.ITEM_CODE IN (").append(String.join(",", itemCodeStr)).append(")");
			
			stmt = conn.prepareStatement(sb.toString());
			logger.debug("Query for getEffectiveDateOfItems() - " + sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				PRItemDTO dto = new PRItemDTO();
				dto.setItemCode(rs.getInt("PRODUCT_ID"));
				dto.setRegEffDate(rs.getString("REG_EFF_DATE"));
				approvedItems.add(dto);
			}
		} catch (SQLException ex) {
			logger.error("getEffectiveDateOfItems() - Error when getting effective dates of items - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return approvedItems;
	}

	public List<ZoneDTO> getZoneData(Connection conn, boolean isGlobalZone) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<ZoneDTO> storeNumZoneId = new ArrayList<>();
		String chainId = PropertyManager.getProperty("PRESTO_SUBSCRIBER");
		try {
			StringBuffer db = new StringBuffer();
			db.append(" SELECT PRICE_ZONE_ID, ZONE_NUM, NAME, ZONE_TYPE, GLOBAL_ZONE FROM RETAIL_PRICE_ZONE ");
			db.append(" WHERE ACTIVE_INDICATOR = 'Y' ");
			if(isGlobalZone) {
				db.append(" AND PRICE_ZONE_ID IN (SELECT DISTINCT(PRICE_ZONE_ID) FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ").append(chainId);
				db.append(" AND PRICE_ZONE_ID_3 = (SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE GLOBAL_ZONE = 'Y')) ");
			}
			stmt = conn.prepareStatement(db.toString());
			logger.debug("Query for getZoneData() - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				ZoneDTO dto = new ZoneDTO();
				dto.setZnId(rs.getInt("PRICE_ZONE_ID"));
				dto.setZnName(rs.getString("NAME"));
				dto.setZnNo(rs.getString("ZONE_NUM"));
				dto.setZoneType(rs.getString("ZONE_TYPE"));
				dto.setGlobalZn(rs.getString("GLOBAL_ZONE"));
				storeNumZoneId.add(dto);
			}
		} catch (SQLException ex) {
			logger.error("getZoneData() - Error while getting zone data - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return storeNumZoneId;
	}

	public List<StoreDTO> getStoreData(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String chainId = PropertyManager.getProperty("PRESTO_SUBSCRIBER");
		List<StoreDTO> storeNumZoneId = new ArrayList<>();
		try {
			StringBuffer db = new StringBuffer();
			db.append(" SELECT COMP_STR_NO, COMP_STR_ID, PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = " + chainId);
			db.append(" AND ACTIVE_INDICATOR = 'Y' ");
			
			stmt = conn.prepareStatement(db.toString());
			logger.debug("Query for getStoreData() - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				StoreDTO dto = new StoreDTO();
				dto.setZoneId(rs.getInt("PRICE_ZONE_ID"));
				dto.strNum = rs.getString("COMP_STR_NO");
				dto.strId = rs.getInt("COMP_STR_ID");
				storeNumZoneId.add(dto);
			}
		} catch (SQLException ex) {
			logger.error("getStoreData() - Error when retrieving storeId data - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return storeNumZoneId;
	}

	public List<PRItemDTO> getSFItemsFromApprovedRecommendations(Connection conn, List<PRItemDTO> SFitems, String currWeekEndDate) {
	
		Set<Integer> itemCodes = new HashSet<>();
		Set<String> runIds = new HashSet<>();
		for(PRItemDTO sfData : SFitems) {
			itemCodes.add(sfData.getItemCode());
			runIds.add(String.valueOf(sfData.getRunId()));
		}
		
		List<PRItemDTO> sfItemsInRankingOrder = getSFItemsInRankingOrder(conn, itemCodes, runIds, currWeekEndDate);

		return sfItemsInRankingOrder;
	
	}

	private List<PRItemDTO> getSFItemsInRankingOrder(Connection conn, Set<Integer> itemCodes, Set<String> runIds,String currWeekEndDate) {
		
		List<Integer> itemCodeList = new ArrayList<Integer>();
		int limitcount = 0;
		List<PRItemDTO> sfList = new ArrayList<>();
		for (Integer itemCode : itemCodes) {
			itemCodeList.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.BATCH_UPDATE_COUNT == 0)) {
				Object[] values = itemCodeList.toArray();
				List<PRItemDTO> sfDataList = retrieveLockedSFItems(conn, runIds, currWeekEndDate, values);
				sfList.addAll(sfDataList);
				itemCodeList.clear();
			}
		}

		if (itemCodeList.size() > 0) {
			Object[] values = itemCodeList.toArray();
			List<PRItemDTO> sfDataList = retrieveLockedSFItems(conn, runIds, currWeekEndDate, values);
			sfList.addAll(sfDataList);
			itemCodeList.clear();
		}		
		return sfList;		
	}

	private List<PRItemDTO> retrieveLockedSFItems(Connection conn, Set<String> runIds, String currWeekEndDate,
			Object... values) {
		
		List<PRItemDTO> sfBaseList = new ArrayList<>();
		List<PRItemDTO> sfnonCandidateItemList = new ArrayList<>();
		
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT CALENDAR_ID, RUN_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, LOCATION_LEVEL_ID, LOCATION_ID, REC_REG_PRICE, ");
		sb.append(" CORE_RETAIL, RET_LIR_ID, RETAILER_ITEM_CODE, IS_EXPORTED, REG_EFF_DATE, ITEM_TYPE, RU_NAME, PART_NAME, REG_PRICE, ");
		sb.append(" REG_MULTIPLE, PREDICTED_BY, PRICE_ZONE_ID, ZONE_NAME, PRICE_TYPE,");
		//commented for AI #19-b
		//taking exact impact instead of abs impact
		//sb.append(" OVERRIDE_REG_PRICE, UPDATED_BY, FIRST_NAME, LAST_NAME, ZONE_NUM, REC_REG_MULTIPLE, PREDICTED, APPROVED_ON, ABS(TOTAL_IMPACT) TOT_IMPACT, ");
		sb.append(" OVERRIDE_REG_PRICE, UPDATED_BY, FIRST_NAME, LAST_NAME, ZONE_NUM, REC_REG_MULTIPLE, PREDICTED, APPROVED_ON, TOTAL_IMPACT, ");
		sb.append(" PRC_CHANGE_IMPACT, OVERRIDE_REG_MULTIPLE, DENSE_RANK() OVER (ORDER BY CALENDAR_ID) WEEK_RANK, ");
		sb.append(" DENSE_RANK() OVER (PARTITION BY CALENDAR_ID ORDER BY TOTAL_IMPACT DESC) IMPACT_RANK, FAMILY FROM  ( ");
		sb.append(" SELECT CAL.CALENDAR_ID, IL.USER_ATTR_14 FAMILY, EX.RUN_ID, EX.PRICE_TYPE, RI.PRODUCT_LEVEL_ID, EX.ITEM_CODE AS PRODUCT_ID, RH.LOCATION_LEVEL_ID, ");
		sb.append(" RH.LOCATION_ID, RI.REC_REG_PRICE , RI.CORE_RETAIL, RI.RET_LIR_ID, IL.RETAILER_ITEM_CODE, ");
		sb.append(" RI.IS_EXPORTED,TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY HH24:MI:SS') AS REG_EFF_DATE, IL.USER_ATTR_6 ITEM_TYPE, PG.NAME RU_NAME, ");
		sb.append(" IL.USER_ATTR_3 PART_NAME, RI.REG_PRICE, RI.REG_MULTIPLE, RH.PREDICTED_BY, ");
		sb.append(" RPZ.PRICE_ZONE_ID, RPZ.NAME AS ZONE_NAME, RI.OVERRIDE_REG_PRICE, QRS.UPDATED_BY, ");
		sb.append(" UD.FIRST_NAME, UD.LAST_NAME, RPZ.ZONE_NUM,  RI.REC_REG_MULTIPLE, ");
		sb.append(" TO_CHAR(RH.PREDICTED, 'MM/DD/YYYY HH24:MI:SS') AS PREDICTED, SUM(PRC_CHANGE_IMPACT) OVER(PARTITION BY PG.NAME) TOTAL_IMPACT, ");
		sb.append(" RI.PRC_CHANGE_IMPACT, RI.OVERRIDE_REG_MULTIPLE, TO_CHAR(RH.APPROVED, 'MM/DD/YYYY HH24:MI:SS') AS APPROVED_ON ");
		sb.append(" FROM PR_PRICE_EXPORT EX LEFT JOIN PR_QUARTER_REC_ITEM RI ");
		sb.append(" ON RI.RUN_ID = EX.RUN_ID AND RI.PRODUCT_ID = EX.ITEM_CODE AND RI.PRODUCT_LEVEL_ID = 1");
		sb.append(" LEFT JOIN PR_QUARTER_REC_HEADER RH ON RI.RUN_ID = RH.RUN_ID  ");
		sb.append(" LEFT JOIN RETAIL_CALENDAR cal ON RH.APPROVED BETWEEN CAL.START_DATE AND CAL.END_DATE AND CAL.ROW_TYPE = 'W'  ");
		sb.append(" LEFT JOIN PRODUCT_GROUP PG ON RH.PRODUCT_ID = PG.PRODUCT_ID LEFT JOIN RETAIL_PRICE_ZONE RPZ ");
		sb.append(" ON RH.LOCATION_ID = RPZ.PRICE_ZONE_ID LEFT JOIN ITEM_LOOKUP IL ");
		sb.append(" ON RI.PRODUCT_ID = IL.ITEM_CODE ").append(" LEFT JOIN PR_QUARTER_REC_STATUS QRS ");
		sb.append(" ON RI.RUN_ID = QRS.RUN_ID LEFT JOIN USER_DETAILS UD ON UD.USER_ID = QRS.UPDATED_BY ");
		sb.append(" WHERE RI.CAL_TYPE='Q' AND RI.PRODUCT_LEVEL_ID = 1 ");
		sb.append(" AND QRS.STATUS IN ( ").append(RecommendationStatusLookup.APPROVED.getStatusId()).append(" , ");
		sb.append(RecommendationStatusLookup.EMERGENCY_APPROVED.getStatusId()).append(")");
		sb.append(" AND RI.RUN_ID IN (").append(String.join(",", runIds)).append(")");
		sb.append(" AND IL.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
		sb.append(" AND IL.ITEM_CODE IN (%s) ");
		if(currWeekEndDate!="") {
			sb.append(" AND TO_DATE(TO_CHAR(RI.REG_EFF_DATE,'MM/dd/yyyy'),'MM/dd/yyyy')=TO_DATE('"+currWeekEndDate+"','MM/dd/yyyy') ");
		}
		sb.append(" )");
		
		logger.debug(sb.toString());
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(String.format(sb.toString(), PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(stmt, values);
			stmt.setFetchSize(50000);
			rs = stmt.executeQuery();
			while (rs.next()) {
				PRItemDTO item = new PRItemDTO();

				item.setZoneType("R");
				item.setRunId(rs.getLong("RUN_ID"));
				item.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
				item.setItemCode(rs.getInt("PRODUCT_ID"));
				item.setChildLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
				item.setChildLocationId(rs.getInt("LOCATION_ID"));
				item.setItemType(rs.getString("ITEM_TYPE"));
				item.setRetLirId(rs.getInt("RET_LIR_ID"));
				item.setPriceZoneId(rs.getInt("PRICE_ZONE_ID"));
				item.setPriceZoneNo(rs.getString("ZONE_NUM"));
				item.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));

				MultiplePrice recommendedPrice = new MultiplePrice(rs.getInt("REC_REG_MULTIPLE"),rs.getDouble("REC_REG_PRICE"));
				item.setRecommendedRegPrice(recommendedPrice);

				MultiplePrice currentPrice = new MultiplePrice(rs.getInt("REG_MULTIPLE"),rs.getDouble("REG_PRICE"));
				item.setCurrentRegPrice(currentPrice);

				item.setRegEffDate(rs.getString("REG_EFF_DATE"));
				item.setPriceExportType(rs.getString("PRICE_TYPE"));

				item.setApprovedOn(rs.getString("APPROVED_ON"));
				item.setApprovedBy(rs.getString("UPDATED_BY"));
				item.setApproverName(rs.getString("FIRST_NAME") + " " + rs.getString("LAST_NAME"));

				item.setVdpRetail(item.getRecommendedRegPrice().getUnitPrice());
				item.setCoreRetail(rs.getDouble("CORE_RETAIL"));
				item.setImpact(rs.getDouble("PRC_CHANGE_IMPACT"));
				// item.setPredicted(rs.getString("PREDICTED"));
				item.setPredicted(rs.getString("PREDICTED"));

				item.setPartNumber(rs.getString("PART_NAME"));
				item.setOverrideRegMultiple(rs.getInt("OVERRIDE_REG_MULTIPLE"));
				item.setOverrideRegPrice(rs.getDouble("OVERRIDE_REG_PRICE"));
				
				item.setSF_RU_rank(rs.getInt("IMPACT_RANK"));
				item.setSF_week_rank(rs.getInt("WEEK_RANK"));
				//commented for AI #19-b
				//taking exact impact instead of abs impact
				item.setTotal_Impact(rs.getDouble("TOTAL_IMPACT"));
				//item.setTotal_Impact(rs.getDouble("TOT_IMPACT"));
				item.setFamilyName(rs.getString("FAMILY"));

				MultiplePrice overridePrice = new MultiplePrice(rs.getInt("OVERRIDE_REG_MULTIPLE"),	rs.getDouble("OVERRIDE_REG_PRICE"));
				item.setOverriddenRegularPrice(overridePrice);

				if (overridePrice.price > 0) {
					item.setDiffRetail(overridePrice.getUnitPrice() - currentPrice.getUnitPrice());
					double diffInPrice = round(item.getDiffRetail(), 2);
					item.setDiffRetail(diffInPrice);
					item.setVdpRetail(overridePrice.getUnitPrice());
				} else {
					item.setDiffRetail(recommendedPrice.getUnitPrice() - currentPrice.getUnitPrice());
					double diffInPrice = round(item.getDiffRetail(), 2);
					item.setDiffRetail(diffInPrice);
					item.setVdpRetail(recommendedPrice.getUnitPrice());
				}

				item.setZoneName(rs.getString("ZONE_NAME"));
				item.setRecommendationUnit(rs.getString("RU_NAME"));
				
				boolean isItemRecommended = true;
				if (overridePrice.price > 0) {
					if (overridePrice.equals(currentPrice)) {
						isItemRecommended = false;
					}
				}else {
					if (recommendedPrice.equals(currentPrice)) {
						isItemRecommended = false;
					}
				}
				
				if (isItemRecommended) {
					sfBaseList.add(item);
				}
				else {
					sfnonCandidateItemList.add(item);
				}
			}
		} catch (Exception e) {
			e.getStackTrace();
			logger.info("Exception: " + e);
			//throw new GeneralException("getBaseDataForExport() - Error getting recommneded items for runids", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return sfBaseList;
	}
	
	public List<PriceExportDTO> getRUofItems(Connection conn) throws GeneralException{
		List<PriceExportDTO> itemsAndRU = new ArrayList<>();
		
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT ITEM_CODE, RETAILER_ITEM_CODE, PG_RU.NAME RU_NAME, PG_RU.PRODUCT_ID RU_ID FROM ITEM_LOOKUP IL ");
		sb.append(" LEFT JOIN (SELECT CHILD_PRODUCT_ID,PRODUCT_ID SEGMENT_ID FROM PRODUCT_GROUP_RELATION_REC "); 
		sb.append(" WHERE CHILD_PRODUCT_LEVEL_ID = 1 AND PRODUCT_LEVEL_ID=2 ) SEGMENT "); 
		sb.append(" ON SEGMENT.CHILD_PRODUCT_ID = IL.ITEM_CODE LEFT JOIN PRODUCT_GROUP PG_SEG  ON PG_SEG.PRODUCT_ID ");  
		sb.append(" = SEGMENT.SEGMENT_ID  AND PG_SEG.PRODUCT_LEVEL_ID = 2 "); 
		sb.append(" LEFT JOIN (SELECT CHILD_PRODUCT_ID,PRODUCT_ID SUB_CATEGORY_ID FROM PRODUCT_GROUP_RELATION_REC "); 
		sb.append(" WHERE CHILD_PRODUCT_LEVEL_ID = 2 AND PRODUCT_LEVEL_ID=3 ) SUB_CATS  ");
		sb.append(" ON SUB_CATS.CHILD_PRODUCT_ID = SEGMENT.SEGMENT_ID LEFT JOIN PRODUCT_GROUP PG_SC  ON PG_SC.PRODUCT_ID  "); 
		sb.append(" = SUB_CATS.SUB_CATEGORY_ID  AND PG_SC.PRODUCT_LEVEL_ID = 3 "); 
		sb.append(" LEFT JOIN (SELECT CHILD_PRODUCT_ID,PRODUCT_ID RU_ID FROM PRODUCT_GROUP_RELATION_REC "); 
		sb.append(" WHERE CHILD_PRODUCT_LEVEL_ID = 3 AND PRODUCT_LEVEL_ID=7 ) RU "); 
		sb.append(" ON RU.CHILD_PRODUCT_ID = SUB_CATS.SUB_CATEGORY_ID LEFT JOIN PRODUCT_GROUP PG_RU ");  
		sb.append(" ON PG_RU.PRODUCT_ID  = RU.RU_ID  AND PG_RU.PRODUCT_LEVEL_ID = 7 "); 
		sb.append(" LEFT JOIN (SELECT CHILD_PRODUCT_ID,PRODUCT_ID CATEGORY_ID FROM PRODUCT_GROUP_RELATION_REC "); 
		sb.append(" WHERE CHILD_PRODUCT_LEVEL_ID = 7 AND PRODUCT_LEVEL_ID= 4 ) CATS "); 
		sb.append(" ON CATS.CHILD_PRODUCT_ID = RU.RU_ID LEFT JOIN PRODUCT_GROUP PG_C  ON PG_C.PRODUCT_ID  = CATS.CATEGORY_ID ");  
		sb.append(" AND PG_C.PRODUCT_LEVEL_ID = 4 "); 
		sb.append(" LEFT JOIN (SELECT CHILD_PRODUCT_ID,PRODUCT_ID DEPARTMENT_ID FROM PRODUCT_GROUP_RELATION_REC "); 
		sb.append(" WHERE CHILD_PRODUCT_LEVEL_ID = 4 AND PRODUCT_LEVEL_ID= 5 ) DEPTS "); 
		sb.append(" ON DEPTS.CHILD_PRODUCT_ID = CATS.CATEGORY_ID LEFT JOIN PRODUCT_GROUP PG_D  ON PG_D.PRODUCT_ID ");  
		sb.append(" = DEPTS.DEPARTMENT_ID  AND PG_D.PRODUCT_LEVEL_ID = 5 ");
		
		logger.info("getRUofItems :"+ sb );
		CachedRowSet rs = PristineDBUtil.executeQuery(conn, sb, "getItemDetails");
		logger.debug("getRUofItems : Query Executed.");
		int numberOfRowsReturned = rs.size();
		logger.debug("Number of rows returned = " + numberOfRowsReturned);
		PriceExportDTO item = null;

		int numberOfIterations = 0;
		try {
			while (rs.next()) {
				item = new PriceExportDTO();
				numberOfIterations++;
				item.setItemCode(rs.getInt("ITEM_CODE"));
				item.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
				item.setRecommendationUnit(rs.getString("RU_NAME"));
				item.setRecommendationUnitId(rs.getInt("RU_ID"));
				itemsAndRU.add(item);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new GeneralException("Cached Rowset Access Exception", e);
		} finally {
			logger.debug("Number of PriceExportDTO objects created = " + numberOfIterations);
			logger.debug("Size of itemsAndRU list = " + itemsAndRU.size());
		}

		
		return itemsAndRU;
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param runIds
	 * @param itemsFiltered
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Long, Integer> getItemCountByRunIdFromExportQueue(Connection conn) throws GeneralException {
		HashMap<Long, Integer> runIdandItemCountMap = new HashMap<Long, Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append("SELECT RUN_ID, COUNT(*) AS COUNT FROM PR_PRICE_EXPORT GROUP BY RUN_ID ");
			statement = conn.prepareStatement(sb.toString());
			
			logger.debug("Query of - GROUP BY RUN_ID() : " + sb.toString());
			
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				runIdandItemCountMap.put(resultSet.getLong("RUN_ID"), resultSet.getInt("COUNT"));
			}

		} catch (SQLException e) {
			logger.error("GetItemCountByRunIdFromExportQueue() - Error while getting items from export queue", e);
			throw new GeneralException(
					"GetItemCountByRunIdFromExportQueue() - Error while getting items from export queue", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return runIdandItemCountMap;
	}


/**
 * 
 * @param conn
 * @param ligItemMap
 * @throws GeneralException
 */
	public void updateLIGItemRegEffectiveDate(Connection conn, Map<String, String> ligItemMap) throws GeneralException {

		PreparedStatement stmt = null;

		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PR_QUARTER_REC_ITEM SET REG_EFF_DATE = ?");
			sb.append(" WHERE RET_LIR_ID = ? AND CAL_TYPE='Q' AND PRODUCT_LEVEL_ID = 11 AND RUN_ID = ? ");
			stmt = conn.prepareStatement(sb.toString());
			int itemCountInBatch = 0;

			for (Map.Entry<String, String> entry : ligItemMap.entrySet()) 
				{
					itemCountInBatch++;
				int colIndex = 0;
				try {
					stmt.setDate(++colIndex, DateUtil.toSQLDate(entry.getValue()));
					stmt.setInt(++colIndex, Integer.valueOf(entry.getKey().split(" - ")[0]));
					stmt.setLong(++colIndex, Long.valueOf(entry.getKey().split(" - ")[1]));
					stmt.addBatch();
				} catch (Exception e) {
					logger.error("updateLIGItemRegEffectiveDate Exception for :" + entry.getKey() + " value: "
							+ entry.getValue());
				}

					if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						stmt.executeBatch();
						stmt.clearBatch();
						itemCountInBatch = 0;
					}
				}
				if (itemCountInBatch > 0) {
					stmt.executeBatch();
					stmt.clearBatch();
				}
				PristineDBUtil.commitTransaction(conn, "updateLIGItemRegEffectiveDate");
			

		} catch (SQLException e) {
			logger.error("updateLIGItemRegEffectiveDate() - Error while changing effective date for LIG Items"+ e);
			throw new GeneralException(
					"updateLIGItemRegEffectiveDate() - Error while changing effective date for LIG Items : " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}
	
	public List<PriceExportDTO> getLIGItemsPostDated(Connection conn, List<PriceExportDTO> itemsList)
			throws GeneralException {

		List<PriceExportDTO> LIGMembersPostDated = new ArrayList<>();

		Set<Long> runIds = new HashSet<>();
		List<Integer> itemCodes = new ArrayList<>();
		List<Long> runIdList = new ArrayList<>();
		int limitcount = 0;
		for (PriceExportDTO dto : itemsList) {
			itemCodes.add(dto.getItemCode());
			limitcount++;
			if (!runIds.contains(dto.getRunId())) {
				runIdList.add(dto.getRunId());
			}
			runIds.add(dto.getRunId());

			if (limitcount > 0 && (limitcount ==Constants.LIMIT_COUNT)) {

				retrievePostDatedItems(conn, LIGMembersPostDated, runIdList, itemCodes);
				runIdList.clear();
				itemCodes.clear();
				runIds.clear();
				limitcount=0;
			}
		}

		if (itemCodes.size() > 0) {
			retrievePostDatedItems(conn, LIGMembersPostDated, runIdList, itemCodes);
			runIdList.clear();
			itemCodes.clear();
			runIds.clear();
		}
		return LIGMembersPostDated;
	}
	
	private void retrievePostDatedItems(Connection conn, List<PriceExportDTO> ligMembersList, List<Long> runIdList,
			List<Integer> itemCodes) throws GeneralException {

		PreparedStatement statement = null;
		ResultSet rs = null;

		StringBuilder runIdStr = new StringBuilder();
		StringBuilder itemStr = new StringBuilder();

		for (int i = 0; i < runIdList.size(); i++) {
			long runId = runIdList.get(i);
			if (i == runIdList.size() - 1) {
				runIdStr.append(runId);
			} else {
				runIdStr.append(runId).append(",");
			}
		}

		for (int i = 0; i < itemCodes.size(); i++) {
			int itemC = itemCodes.get(i);
			if (i == itemCodes.size() - 1) {
				itemStr.append(itemC);
			} else {
				itemStr.append(itemC).append(",");
			}
		}

		StringBuilder sb = new StringBuilder();

		try {

			//changed the query to add is_exported = null
			sb.append(
					" SELECT PR.PRODUCT_ID,PR.RET_LIR_ID,IL.USER_ATTR_6,TO_CHAR(PR.REG_EFF_DATE,'MM/dd/yyyy') AS REG_EFF_DATE,PR.RUN_ID");
			sb.append(
					" FROM PR_QUARTER_REC_ITEM PR LEFT JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE=PR.PRODUCT_ID AND PR.CAL_TYPE='Q'");
			sb.append(" WHERE PR.PRODUCT_ID IN( ").append(itemStr.toString());
			sb.append(" )AND PR.RUN_ID IN (").append(runIdStr.toString())
					.append(") AND PR.CAL_TYPE='Q' AND PR.IS_EXPORTED IS NULL ORDER BY RUN_ID ");
			statement = conn.prepareStatement(sb.toString());

			logger.debug("retrievePostDatedItems() qry : " + sb.toString());

			rs = statement.executeQuery();

			while (rs.next()) {
				PriceExportDTO priceExportDTO = new PriceExportDTO();
				priceExportDTO.setItemCode(rs.getInt("PRODUCT_ID"));
				priceExportDTO.setRetLirId(rs.getInt("RET_LIR_ID"));
				priceExportDTO.setItemType(rs.getString("USER_ATTR_6"));
				priceExportDTO.setRegEffDate(rs.getString("REG_EFF_DATE"));
				priceExportDTO.setRunId(rs.getLong("RUN_ID"));
				ligMembersList.add(priceExportDTO);
			}

		} catch (SQLException e) {
			logger.error("Error while executing GET_ITEM_CODE");
			throw new GeneralException("Error while executing GET_ITEM_CODE", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}

	}

	public long insertInPRExportRunHeader (Connection con, PRExportRunHeader prExportRunHeader) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		long exportId = -1;
		try {
			stmt = con.prepareStatement(GET_EXPORT_ID);
			rs = stmt.executeQuery();
			if (rs.next()) {
				exportId = rs.getLong("EXPORT_ID");
			}
		} catch (SQLException e) {
			logger.error("Error occured while geneating an EXPORT_ID for the price export!",e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		int counter = 0;
		try {
			stmt = con.prepareStatement(INSERT_EXPORT_HEADER);//(EXPORT_ID, USER_ID, START_TIME, STATUS, RUN_TYPE, EXPORT_TYPE, EFFECTIVE_DATE, SF_THRESHOLD)
			stmt.setLong(++counter, exportId);
			stmt.setString(++counter, prExportRunHeader.getUserId());
			stmt.setInt(++counter, prExportRunHeader.getStatus());
			stmt.setString(++counter, String.valueOf(prExportRunHeader.getRunType()));
			stmt.setString(++counter, prExportRunHeader.getExportType());
			stmt.setString(++counter, prExportRunHeader.getEffectiveDate());
			stmt.setInt(++counter, prExportRunHeader.getSfThreshold());
			stmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("Error occured while creating a new entry in PR EXPORT RUN HEADER for the price export !",e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		return exportId;
	}
	
	public void updateExportRunHeaderStatus(Connection con, long exportId, int runStatus) {
		PreparedStatement status = null;
		try {
			status = con.prepareStatement(UPDATE_EXPORT_HEADER_STATUS);
			status.setInt(1, runStatus);
			status.setLong(2, exportId);
			status.executeUpdate();
		} catch (SQLException e) {
			logger.error("Error when updating export run header - ",e);
		} finally {
			PristineDBUtil.close(status);
		}
	}
	
	public void updateExportRunHeaderEndDate(Connection con, long exportId) {
		PreparedStatement endDate = null;
		try {
			endDate = con.prepareStatement(UPDATE_EXPORT_HEADER_END_DATE);
			endDate.setLong(1,exportId);
			endDate.executeUpdate();
		} catch (SQLException e) {
			logger.error("Error when updating export run header - ",e);
		} finally {
			PristineDBUtil.close(endDate);
		}
	}
	
	public void updateExportRunHeaderFilepath(Connection con, long exportId, String filePath) {
		PreparedStatement endDate = null;
		try {
			endDate = con.prepareStatement(UPDATE_EXPORT_HEADER_END_DATE);
			endDate.setString(1,filePath);
			endDate.setLong(2,exportId);
			endDate.executeUpdate();
		} catch (SQLException e) {
			logger.error("Error when updating export run header - ",e);
		} finally {
			PristineDBUtil.close(endDate);
		}
	}
	
}


