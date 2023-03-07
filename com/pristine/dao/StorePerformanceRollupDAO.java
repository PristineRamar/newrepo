package com.pristine.dao;

import java.sql.Connection;
import java.util.ArrayList;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import com.pristine.dto.StorePerformanceRollupSummaryDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class StorePerformanceRollupDAO implements IDAO
{
	private static Logger logger = Logger.getLogger("StorePerformanceRollupDAO");

	public enum PRODUCT_LEVEL_ENUM {
		PRODUCT_STORE_LEVEL(1), PRODUCT_DEPT_LEVEL(2), PRODUCT_CATEGORY_LEVEL(3)
		, PRODUCT_SUBCATEGORY_LEVEL(4), PRODUCT_SEGMENT_LEVEL(5);

		private final int index;

		PRODUCT_LEVEL_ENUM(int index) {
			this.index = index;
		}

		public int index() {
			return index;
		}
	}

	public static enum LEVEL_TYPE_ENUM {
		CHAIN(1), BANNER(2), REGION(3), DISTRICT(4);

		private final int index;

		LEVEL_TYPE_ENUM(int index) {
			this.index = index;
		}

		public int index() {
			return index;
		}
	};

	public StorePerformanceRollupDAO(Connection conn) throws GeneralException {

	}

	public ArrayList<StorePerformanceRollupSummaryDTO> getBanner(Connection conn)
			throws GeneralException {

		ArrayList<StorePerformanceRollupSummaryDTO> storeIDList = new ArrayList<StorePerformanceRollupSummaryDTO>();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT ID, NAME FROM RETAIL_DIVISION ");
		sb.append("	ORDER BY ID");

		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "getBannerID");

		try {
			while (crs.next()) {
				StorePerformanceRollupSummaryDTO listData = new StorePerformanceRollupSummaryDTO();
				listData.setLevelID(crs.getInt("ID"));

				storeIDList.add(listData);
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return storeIDList;

	}
	

	public ArrayList<StorePerformanceRollupSummaryDTO> getRegions(Connection conn,
			int bannerID) throws GeneralException {

		ArrayList<StorePerformanceRollupSummaryDTO> storeIDList = new ArrayList<StorePerformanceRollupSummaryDTO>();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT ID, NAME FROM RETAIL_REGION ");
		if (bannerID != 0) {
			sb.append(" WHERE ");			
			sb.append("DIVISION_ID = " + bannerID);
		}
		sb.append("	ORDER BY ID");

		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "getRegionID");

		try {
			while (crs.next()) {
				StorePerformanceRollupSummaryDTO districtData = new StorePerformanceRollupSummaryDTO();
				districtData.setLevelID(crs.getInt("ID"));

				storeIDList.add(districtData);
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return storeIDList;

	}

	
	public ArrayList<StorePerformanceRollupSummaryDTO> getDistrictID(Connection conn,
			int RegionID) throws GeneralException {

		ArrayList<StorePerformanceRollupSummaryDTO> storeIDList = new ArrayList<StorePerformanceRollupSummaryDTO>();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT ID, NAME FROM RETAIL_DISTRICT ");
		if (RegionID != 0) {
			sb.append(" WHERE");
			sb.append(" REGION_ID = " + RegionID);
		}
		sb.append("	ORDER BY ID");

		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "getStoreID");

		try {
			while (crs.next()) {
				StorePerformanceRollupSummaryDTO districtData = new StorePerformanceRollupSummaryDTO();
				districtData.setLevelID(crs.getInt("ID"));
				storeIDList.add(districtData);
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return storeIDList;

	}

	public String getStoreID(Connection conn, String districtID)
			throws GeneralException {

		StringBuilder storeIDList = new StringBuilder();
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT DISTINCT CS.CITY ||'  '|| CS.ADDR_LINE1 ||'  '|| CS.COMP_STR_NO AS STORES,");
		sb.append("		CS.COMP_STR_ID AS STOREID ");
		sb.append("FROM COMPETITOR_STORE CS  ");
		sb.append("WHERE CS.DISTRICT_ID IN (").append(districtID).append(")");

		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "getStoreID");

		try {
			while (crs.next()) {
				// logger.info("Store ID :" + crs.getInt("STOREID"));

				storeIDList
						.append(String.format("%1s,", crs.getInt("STOREID")));

			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}

		return storeIDList.substring(0, storeIDList.length() - 1);

	}

	public ArrayList<StorePerformanceRollupSummaryDTO> getSummaryInformation(
			Connection conn, String storeList, String endDate,
			PRODUCT_LEVEL_ENUM productRollupLevel) throws GeneralException {

		ArrayList<StorePerformanceRollupSummaryDTO> storeIDList = new ArrayList<StorePerformanceRollupSummaryDTO>();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT END_DATE,DEPT_ID,CATEGORY_ID,SUB_CATEGORY_ID,SEGMENT_ID,");
		sb.append(" SUM(VISIT_REVENUE_AVG * VISIT_COUNT) AS VISIT_REVENUE_TOTAL,");
		sb.append(" SUM(VISIT_COUNT) AS TOTAL_VISIT_COUNT,");
		sb.append(" SUM(REVENUE_TOTAL) as REVENUE_TOTAL ,");
		sb.append(" SUM(REVENUE_REGULAR) AS REVENUE_REGULAR, SUM(REVENUE_SALE) AS REVENUE_SALE,");
		sb.append(" SUM(FINAL_COST) AS FINAL_COST,SUM(MARGIN) AS MARGIN ");
		sb.append(" FROM SUMMARY_WEEKLY A,SCHEDULE B WHERE A.SCHEDULE_ID =B.SCHEDULE_ID AND");
		sb.append(" A.COMP_STR_ID IN (" + storeList + ")");
		sb.append(String.format(
				" AND B.END_DATE = TO_DATE('%1s','MM/DD/YYYY') ", endDate));
		if (productRollupLevel == PRODUCT_LEVEL_ENUM.PRODUCT_DEPT_LEVEL)
			sb.append(" AND A.DEPT_ID IS NOT NULL ");
		else if (productRollupLevel == PRODUCT_LEVEL_ENUM.PRODUCT_CATEGORY_LEVEL)
			sb.append(" AND A.DEPT_ID IS NOT NULL AND A.CATEGORY_ID IS NOT NULL ");
		else if (productRollupLevel == PRODUCT_LEVEL_ENUM.PRODUCT_SUBCATEGORY_LEVEL)
			sb.append(" AND A.DEPT_ID IS NOT NULL AND A.CATEGORY_ID IS NOT NULL  AND A.SUB_CATEGORY_ID IS NOT NULL ");
		else if (productRollupLevel == PRODUCT_LEVEL_ENUM.PRODUCT_SEGMENT_LEVEL)
			sb.append(" AND A.DEPT_ID IS NOT NULL AND A.CATEGORY_ID IS NOT NULL  AND A.SUB_CATEGORY_ID IS NOT NULL AND A.SEGMENT_ID IS NOT NULL");
		sb.append("GROUP BY END_DATE,DEPT_ID,CATEGORY_ID,SUB_CATEGORY_ID,SEGMENT_ID");

		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb,
				"getSummaryInformation");
		//logger.info("Summary Information:" + sb.toString());

		try {
			while (crs.next()) {
				StorePerformanceRollupSummaryDTO performanceData = new StorePerformanceRollupSummaryDTO();
				performanceData.setLevelTypeID(4);
				performanceData.setWeekEndDate(endDate);
				performanceData.setDeptID(crs.getInt("DEPT_ID"));
				performanceData.setCategoryID(crs.getInt("CATEGORY_ID"));
				performanceData.setSubCategoryID(crs.getInt("SUB_CATEGORY_ID"));
				performanceData.setSegmentID(crs.getInt("SEGMENT_ID"));
				performanceData.setRevenueRegular(crs.getDouble("REVENUE_REGULAR"));
				performanceData.setRevenueSale(crs.getDouble("REVENUE_SALE"));
				performanceData.setRevenueTotal(crs.getDouble("REVENUE_TOTAL"));
				performanceData.setFinalCost(crs.getDouble("FINAL_COST"));
				performanceData.setMargin(crs.getDouble("MARGIN"));
				performanceData.setVisitCount(crs.getDouble("TOTAL_VISIT_COUNT"));
				
				if (crs.getDouble("TOTAL_VISIT_COUNT") != 0){
					double visitedAvg = crs.getDouble("VISIT_REVENUE_TOTAL") / crs.getDouble("TOTAL_VISIT_COUNT");
					performanceData.setVisitRevenueAvg(visitedAvg);
				}
				
				double marginPct = 0;
				if (crs.getDouble("REVENUE_REGULAR") != 0) {
					marginPct = (performanceData.getRevenueTotal() - performanceData.getFinalCost()) * 100 / performanceData.getRevenueTotal();
					if (marginPct > 999.99) 
						performanceData.setMarginPCT(999.99);
					else if (marginPct < -999.99)
						performanceData.setMarginPCT(-999.99);
					else
					performanceData.setMarginPCT(marginPct);
				}
				
				performanceData.setWeekEndDate(crs.getString("END_DATE"));

				storeIDList.add(performanceData);
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return storeIDList;

	}

	public static float Round(double marginPCT, int Rpl) {
		float p = (float) Math.pow(10, Rpl);
		marginPCT = marginPCT * p;
		float tmp = Math.round(marginPCT);
		return (float) tmp / p;
	}

	public ArrayList<StorePerformanceRollupSummaryDTO> getSummaryRollupInformation(
			Connection conn, LEVEL_TYPE_ENUM levelTypeID, String levelID,
			String endDate, PRODUCT_LEVEL_ENUM productRollupLevel)
			throws GeneralException {

		ArrayList<StorePerformanceRollupSummaryDTO> storeIDList = new ArrayList<StorePerformanceRollupSummaryDTO>();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT WEEK_END_DATE,DEPT_ID,CATEGORY_ID,SUB_CATEGORY_ID,SEGMENT_ID,");
		sb.append(" SUM(VISIT_REVENUE_AVG * VISIT_COUNT) AS VISIT_REVENUE_TOTAL,");
		sb.append(" SUM(VISIT_COUNT) AS TOTAL_VISIT_COUNT,");
		sb.append(" SUM(REVENUE_TOTAL) as REVENUE_TOTAL ,");
		sb.append(" SUM(REVENUE_REGULAR) AS REVENUE_REGULAR, SUM(REVENUE_SALE) AS REVENUE_SALE,");
		sb.append(" SUM(FINAL_COST) AS FINAL_COST,SUM(MARGIN) AS MARGIN ");
		sb.append(" FROM SUMMARY_WEEKLY_ROLLUP A WHERE A.LEVEL_TYPE_ID =  ")
				.append(levelTypeID.index());
		sb.append("	AND LEVEL_ID IN(").append(levelID).append(")");

		sb.append(String.format(
				" AND A.WEEK_END_DATE = TO_DATE('%1s','MM/DD/YYYY') ", endDate));

		if (productRollupLevel == PRODUCT_LEVEL_ENUM.PRODUCT_DEPT_LEVEL)
			sb.append(" AND A.DEPT_ID IS NOT NULL ");
		else if (productRollupLevel == PRODUCT_LEVEL_ENUM.PRODUCT_CATEGORY_LEVEL)
			sb.append(" AND A.DEPT_ID IS NOT NULL AND A.CATEGORY_ID IS NOT NULL ");
		else if (productRollupLevel == PRODUCT_LEVEL_ENUM.PRODUCT_SUBCATEGORY_LEVEL)
			sb.append(" AND A.DEPT_ID IS NOT NULL AND A.CATEGORY_ID IS NOT NULL AND A.SUB_CATEGORY_ID IS NOT NULL ");
		else if (productRollupLevel == PRODUCT_LEVEL_ENUM.PRODUCT_SEGMENT_LEVEL)
			sb.append(" AND A.DEPT_ID IS NOT NULL AND A.CATEGORY_ID IS NOT NULL AND A.SUB_CATEGORY_ID IS NOT NULL AND A.SEGMENT_ID IS NOT NULL");
		sb.append("GROUP BY WEEK_END_DATE,DEPT_ID,CATEGORY_ID,SUB_CATEGORY_ID,SEGMENT_ID");

		//logger.info("Summary Information:" + sb.toString());
		
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb,
				"getSummaryInformation");


		try {
			while (crs.next()) {
				StorePerformanceRollupSummaryDTO performanceData = new StorePerformanceRollupSummaryDTO();
				performanceData.setLevelTypeID(levelTypeID.index());
				performanceData.setWeekEndDate(endDate);
				performanceData.setDeptID(crs.getInt("DEPT_ID"));
				performanceData.setCategoryID(crs.getInt("CATEGORY_ID"));
				performanceData.setSubCategoryID(crs.getInt("SUB_CATEGORY_ID"));
				performanceData.setSegmentID(crs.getInt("SEGMENT_ID"));
				performanceData.setRevenueTotal(crs.getDouble("REVENUE_TOTAL"));
				performanceData.setRevenueRegular(crs.getDouble("REVENUE_REGULAR"));
				performanceData.setRevenueSale(crs.getDouble("REVENUE_SALE"));
				performanceData.setFinalCost(crs.getDouble("FINAL_COST"));
				performanceData.setMargin(crs.getDouble("MARGIN"));
				performanceData.setVisitCount(crs.getDouble("TOTAL_VISIT_COUNT"));
				
				if (crs.getDouble("TOTAL_VISIT_COUNT") != 0){
					double visitedAvg = crs.getDouble("VISIT_REVENUE_TOTAL") / crs.getDouble("TOTAL_VISIT_COUNT");
					performanceData.setVisitRevenueAvg(visitedAvg);
				}
				
				double marginPct = 0;
				if (crs.getDouble("REVENUE_REGULAR") != 0) {
					marginPct = (performanceData.getRevenueTotal() - performanceData.getFinalCost()) * 100 / performanceData.getRevenueTotal();
					if (marginPct > 999.99) 
						performanceData.setMarginPCT(999.99);
					else if (marginPct < -999.99)
						performanceData.setMarginPCT(-999.99);
					else
					performanceData.setMarginPCT(marginPct);
				}
				
				performanceData.setWeekEndDate(crs.getString("WEEK_END_DATE"));

				storeIDList.add(performanceData);
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return storeIDList;

	}

	public void deleteSummaryWeeklyRollup(Connection conn, String endDate,
			LEVEL_TYPE_ENUM performanceData,
			int levelId) throws GeneralException {

		StringBuffer sb = new StringBuffer();

		sb.append(" DELETE FROM SUMMARY_WEEKLY_ROLLUP WHERE WEEK_END_DATE = ")
				.append(String.format("to_date('%1s','MM/DD/YYYY') ", endDate));
		sb.append(" 	AND LEVEL_TYPE_ID = ").append(performanceData.index);
		sb.append(" 	AND LEVEL_ID = ").append(levelId);
		//logger.info(sb.toString());
		try {
			PristineDBUtil.execute(conn, sb,
					"Delete summary weekly rollup information");
		} catch (GeneralException e) {
			logger.error("Delete summary weekly rollup information", e);
		}

	}

	public void insertSummaryWeeklyRollup(Connection conn, String endDate,
			StorePerformanceRollupSummaryDTO performanceData) throws GeneralException {

		StringBuffer sb = new StringBuffer();

		sb.append(" INSERT INTO SUMMARY_WEEKLY_ROLLUP(WEEK_END_DATE, LEVEL_TYPE_ID,LEVEL_ID, ");
		sb.append(" DEPT_ID, CATEGORY_ID, REVENUE_TOTAL,REVENUE_REGULAR, REVENUE_SALE,");
		sb.append(" FINAL_COST, MARGIN, VISIT_COUNT, ");
		sb.append(" VISIT_REVENUE_AVG, MARGIN_PCT, ");
		sb.append(" SUB_CATEGORY_ID,SEGMENT_ID  ) VALUES( ");

		sb.append(String.format("to_date('%1s','MM/DD/YYYY'),", endDate));
		sb.append(String.format("'%1s',", performanceData.getLevelTypeID()));
		sb.append(String.format("'%1s',", performanceData.getLevelID()));

		if (performanceData.getDeptID() != 0)
			sb.append(String.format("'%1s',", performanceData.getDeptID()));
		else
			sb.append("null,");

		if (performanceData.getCategoryID() != 0)
			sb.append(String.format("'%1s',", performanceData.getCategoryID()));
		else
			sb.append("null,");
		
		if (performanceData.getRevenueTotal() != 0)
			sb.append(String.format("'%1s',", performanceData.getRevenueTotal()));
		else
			sb.append("null,");

		if (performanceData.getRevenueRegular() != 0)
			sb.append(String.format("'%1s',", performanceData.getRevenueRegular()));
		else
			sb.append("null,");

		if (performanceData.getRevenueSale() != 0)
			sb.append(String.format("'%1s',", performanceData.getRevenueSale()));
		else
			sb.append("null,");

		if (performanceData.getFinalCost() != 0)
			sb.append(String.format("'%1s',", performanceData.getFinalCost()));
		else
			sb.append("null,");

		if (performanceData.getMargin() != 0)
			sb.append(String.format("'%1s',", performanceData.getMargin()));
		else
			sb.append("null,");

		if (performanceData.getVisitCount() != 0)
			sb.append(String.format("'%1s',", performanceData.getVisitCount()));
		else
			sb.append("null,");

		if (performanceData.getVisitRevenueAvg() != 0)
			sb.append(String.format("'%1s',", performanceData.getVisitRevenueAvg()));
		else
			sb.append("null,");

		if (performanceData.getMarginPCT() != 0)
			sb.append(String.format("'%1s',",
					Round(performanceData.getMarginPCT(), 2)));
		else
			sb.append("null,");

		if (performanceData.getSubCategoryID() != 0)
			sb.append(String.format("'%1s',", performanceData.getSubCategoryID()));
		else
			sb.append("null,");

		if (performanceData.getSegmentID() != 0)
			sb.append(String.format("'%1s'", performanceData.getSegmentID()));
		else
			sb.append("null");

		sb.append(" )");

		//logger.debug(sb.toString());
		try {
			PristineDBUtil.execute(conn, sb, "Store Performance Roll up - Insert into Summary Weekly");
		}
		catch (GeneralException e) {
			logger.info(sb.toString());
			logger.error("Store Performance Roll up - Insert into Summary Weekly", e);
		}

	}

}
