package com.pristine.dao.offermgmt.mwr;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.mwr.MWRSummaryDTO;
import com.pristine.dto.offermgmt.mwr.QuarterlyDashboardDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.offermgmt.PRConstants;

public class QuarterlyDashboardDAO {

	static Logger logger = Logger.getLogger("QuartelyDashboardDAO");
	private Connection conn = null;

	public QuarterlyDashboardDAO(Connection conn) {
		this.conn = conn;
	}

	private static final String GET_CURRENT_QUARTER_RUN_ID = " SELECT MAX(RUN_ID) AS RUN_ID FROM " + "( SELECT "
			+ " RUN_ID, START_CALENDAR_ID, RC1.START_DATE AS START_DATE, "
			+ " END_CALENDAR_ID, RC2.END_DATE AS END_DATE, "
			+ " ACTUAL_START_CALENDAR_ID, RC3.START_DATE AS ACTUAL_START_DATE,"
			+ " ACTUAL_END_CALENDAR_ID, RC4.END_DATE  AS ACTUAL_END_DATE " + "FROM PR_QUARTER_REC_HEADER PRH "
			+ " JOIN RETAIL_CALENDAR RC1 ON RC1.CALENDAR_ID = PRH.START_CALENDAR_ID "
			+ " JOIN RETAIL_CALENDAR RC2 ON RC2.CALENDAR_ID = PRH.END_CALENDAR_ID "
			+ " JOIN RETAIL_CALENDAR RC3 ON RC3.CALENDAR_ID = PRH.ACTUAL_START_CALENDAR_ID "
			+ " JOIN RETAIL_CALENDAR RC4 ON RC4.CALENDAR_ID = PRH.ACTUAL_END_CALENDAR_ID "
			+ " WHERE RC3.START_DATE <=TO_DATE(?, 'MM/DD/YYYY')" + "AND RC4.END_DATE > TO_DATE(?, 'MM/DD/YYYY') "
			+ " AND PRH.PRODUCT_ID=? AND PRH.PRODUCT_LEVEL_ID=? AND PRH.LOCATION_LEVEL_ID=? "
			+ " AND PRH.LOCATION_ID=? AND PRH.RUN_TYPE <> '" + PRConstants.RUN_TYPE_TEMP + "' AND PRH.RUN_STATUS = '" + PRConstants.RUN_STATUS_SUCCESS +"')";

	private static final String GET_FUTURE_QUARTER_RUN_ID = " SELECT MAX(RUN_ID) AS RUN_ID FROM " + "( SELECT "
			+ " RUN_ID, START_CALENDAR_ID, RC1.START_DATE AS START_DATE, "
			+ " END_CALENDAR_ID, RC2.END_DATE AS END_DATE, "
			+ " ACTUAL_START_CALENDAR_ID, RC3.START_DATE AS ACTUAL_START_DATE, "
			+ " ACTUAL_END_CALENDAR_ID, RC4.END_DATE  AS ACTUAL_END_DATE FROM PR_QUARTER_REC_HEADER PRH "
			+ " JOIN RETAIL_CALENDAR RC1 ON RC1.CALENDAR_ID = PRH.START_CALENDAR_ID "
			+ " JOIN RETAIL_CALENDAR RC2 ON RC2.CALENDAR_ID = PRH.END_CALENDAR_ID "
			+ " JOIN RETAIL_CALENDAR RC3 ON RC3.CALENDAR_ID = PRH.ACTUAL_START_CALENDAR_ID "
			+ " JOIN RETAIL_CALENDAR RC4 ON RC4.CALENDAR_ID = PRH.ACTUAL_END_CALENDAR_ID "
			+ " WHERE RC3.START_DATE >TO_DATE(?, 'MM/DD/YYYY') " + " AND  RC4.END_DATE > TO_DATE(?, 'MM/DD/YYYY')"
			+ " AND PRH.PRODUCT_ID=? AND PRH.PRODUCT_LEVEL_ID=? AND PRH.LOCATION_LEVEL_ID=?  "
			+ " AND PRH.LOCATION_ID=? AND PRH.RUN_TYPE <> '" + PRConstants.RUN_TYPE_TEMP + "' AND PRH.RUN_STATUS = '" + PRConstants.RUN_STATUS_SUCCESS +"')";

	private static final String GET_QUARTERLY_SUMMARY = "SELECT RUN_ID, UNITS_ACTUAL,REV_ACTUAL,MAR_ACTUAL, MAR_PCT_ACTUAL,"
			+ " INBETWEEN_WEEKS_UNITS_PRED, "
			+ " INBETWEEN_WEEKS_REV_PRED, INBETWEEN_WEEKS_MAR_PRED, INBETWEEN_WEEKS_MAR_PCT_PRED, "
			+ " REC_WEEKS_UNITS_PRED, " + "REC_WEEKS_REV_PRED,REC_WEEKS_MAR_PRED,REC_WEEKS_MAR_PCT_PRED, "
			+ " UNITS_TOTAL_ILP,REV_TOTAL_ILP,MAR_TOTAL_ILP,MAR_PCT_TOTAL_ILP, "
			+ " PRICE_INDEX, CURR_UNITS_ILP, CURR_REVENUE_ILP, CURR_MARGIN_ILP, "
			+ " CURR_MAR_PCT_ILP, PROMO_UNITS_ILP, PROMO_REVENUE_ILP, PROMO_MARGIN_ILP, PROMO_MAR_PCT_ILP, "
			+ " PRICE_INDEX, PRICE_INDEX_PROMO, "
			+ " TOTAL_ITEMS, TOTAL_ITEMS_LIG, TOTAL_EDLP_ITEMS, TOTAL_EDLP_ITEMS_LIG, TOTAL_PROMO_ITEMS, "
			+ " TOTAL_PROMO_ITEMS_LIG, PRICE_BASE_INDEX, CURR_PRICE_INDEX_BASE, UNITS_TOTAL,REV_TOTAL,MAR_TOTAL,MAR_PCT_TOTAL, "
			+ " CURR_UNITS, CURR_REVENUE, CURR_MARGIN, CURR_MAR_PCT,"
			+ " TOTAL_REC_ITEMS, TOTAL_REC_ITEMS_LIG, TOTAL_COST_CHANGES, TOTAL_COST_CHANGES_LIG, "
			+ " TOTAL_COMP_CHANGES, TOTAL_COMP_CHANGES_LIG, PRC_CHANGE_IMPACT, MARK_UP, MARK_DOWN "
			+ " FROM PR_QUARTER_REC_SUMMARY WHERE RUN_ID=?";

	private static final String GET_DASHBOARD_ID = "SELECT DASHBOARD_ID FROM PR_QUARTER_DASHBOARD WHERE "
			+ "LOCATION_ID=? AND LOCATION_LEVEL_ID=? AND PRODUCT_ID=? AND PRODUCT_LEVEL_ID=? ";

	private static final String GET_MULTI_WEEK_DASHBOARD_RUN_ID = "SELECT PR_QUARTER_DASHBOARD_ID_SEQ.NEXTVAL AS DASHBOARD_ID FROM DUAL";

	private static final String INSERT_PR_QUARTER_DASHBOARD = "INSERT INTO PR_QUARTER_DASHBOARD (DASHBOARD_ID, "
			+ " LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, ACTIVE)" + " VALUES (?, ?, ?, ?, ?, 'Y') ";

	private static final String INSERT_PR_QUARTER_DASHBOARD_DETAIL = "INSERT INTO PR_QUARTER_DASHBOARD_DETAIL "
			+ "(DASHBOARD_ID, " + "RUN_ID, UNITS_TOT_ACTUAL, REV_TOT_ACTUAL, MAR_TOT_ACTUAL, MAR_PCT_TOT_ACTUAL, "
					+ " UNITS_TOT_PRED, REV_TOT_PRED, MAR_TOT_PRED, MAR_PCT_TOT_PRED, "
					+ " UNITS_TOT_FINAL_ILP, REV_TOT_FINAL_ILP, MAR_TOT_FINAL_ILP, MAR_PCT_TOT_FINAL_ILP, PRICE_TOT_INDEX, "
					+ " TOTAL_TOT_ITEMS, TOTAL_ITEMS_TOT_LIG, UNITS_PROMO_PRED, REV_PROMO_PRED, MAR_PROMO_PRED, "
					+ " MAR_PCT_PROMO_PRED, UNITS_PROMO_FINAL_ILP, REV_PROMO_FINAL_ILP, MAR_PROMO_FINAL_ILP, MAR_PCT_PROMO_FINAL_ILP, "
					+ " PRICE_PROMO_INDEX, TOTAL_PROMO_ITEMS, TOTAL_ITEMS_PROMO_LIG, "
					+ " IS_ON_GOING_QUARTER, PRICE_BASE_INDEX, UNITS_TOT_FINAL, REV_TOT_FINAL, MAR_TOT_FINAL, MAR_PCT_TOT_FINAL, "
					+ " TOTAL_REC_ITEMS, TOTAL_REC_ITEMS_LIG, TOTAL_COST_CHANGES, TOTAL_COST_CHANGES_LIG,"
					+ " TOTAL_COMP_CHANGES, TOTAL_COMP_CHANGES_LIG, CURR_TOT_UNITS, CURR_TOT_REVENUE,  CURR_TOT_MARGIN, "
					+ " CURR_TOT_UNITS_ILP, CURR_TOT_REVENUE_ILP, CURR_TOT_MARGIN_ILP, PRC_CHANGE_IMPACT, MARK_UP, MARK_DOWN)" 
					+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
					+ " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";

	private static final String DELETE_MULTI_WEEK_DASHBOARD_DATA = "DELETE FROM PR_QUARTER_DASHBOARD_DETAIL "
			+ " WHERE DASHBOARD_ID = ? ";

	/**
	 * @param List<QuarterlyDashboardDTO>
	 * @throws GeneralException
	 */

	public void saveDashboardDetails(List<QuarterlyDashboardDTO> quarterlyDashboard) throws GeneralException {

		for (QuarterlyDashboardDTO dashboardData : quarterlyDashboard) {
			long dashboardID = getDashboardId(dashboardData.getLocationId(), dashboardData.getLocationLevelId(),
					dashboardData.getProductId(), dashboardData.getProductLevelId());
			
			if (dashboardID == 0) {
				logger.info("Dashboard Id not found in PR_QUARTER_DASHBOARD for:" + dashboardData.getLocationLevelId());
				logger.info("Inserting data in PR_QUARTER_DASHBOARD ");
				insertDashboarData(conn, dashboardData);
				logger.info("Insert  in PR_QUARTER_DASHBOARD Complete ");

			} else {
				logger.info("Deleting data in PR_QUARTER_DASHBOARD  for " + dashboardID);
				deleteDashboarData(conn, dashboardID);
				logger.info("Delete Successful  ");
				logger.info("Inserting data in PR_QUARTER_DASHBOARD ");
				insertDashboarDetails(conn, dashboardData);
				logger.info("Insert  in PR_QUARTER_DASHBOARD Complete ");
			}

		}
	}

	/**
	 * @param inputDate,
	 *            locationId,LocatinlevelId,ProductId,productLevelId
	 * @return currentrunID
	 * @throws GeneralException
	 */

	public long getCurrentQuarterRunId(String inputDate, int productId, int productLevelId, int locationLevelId,
			int locationId) throws GeneralException {

		PreparedStatement stmt = null;
		ResultSet rs = null;
		long currentrunID = 0;
		try {
			stmt = conn.prepareStatement(GET_CURRENT_QUARTER_RUN_ID);
			int count = 0;
			stmt.setString(++count, inputDate);
			stmt.setString(++count, inputDate);
			stmt.setInt(++count, productId);
			stmt.setInt(++count, productLevelId);
			stmt.setInt(++count, locationLevelId);
			stmt.setInt(++count, locationId);

			rs = stmt.executeQuery();
			if (rs.next()) {

				currentrunID = rs.getLong("RUN_ID");

			}
		} catch (SQLException e) {
			logger.error("Error in getCurrentQuarterRunId() - " + e);
			throw new GeneralException("Error when retrieving run_id   - " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return currentrunID;
	}

	/**
	 * @param inputDate,
	 *            locationId,LocatinlevelId,ProductId,productLevelId
	 * @return futureRunId
	 * @throws GeneralException
	 */
	public long getFutureQuarterRunId(String inputDate, int productId, int productLevelId, int locationLevelId,
			int locationId) throws GeneralException {

		PreparedStatement stmt = null;
		ResultSet rs = null;
		long futureRunId = 0;
		try {
			stmt = conn.prepareStatement(GET_FUTURE_QUARTER_RUN_ID);
			int count = 0;
			stmt.setString(++count, inputDate);
			stmt.setString(++count, inputDate);
			stmt.setInt(++count, productId);
			stmt.setInt(++count, productLevelId);
			stmt.setInt(++count, locationLevelId);
			stmt.setInt(++count, locationId);

			rs = stmt.executeQuery();
			if (rs.next()) {

				futureRunId = rs.getLong("RUN_ID");

			}
		} catch (SQLException e) {
			logger.error("Error in getFutureQuarterRunId() - " + e);
			throw new GeneralException("Error when retrieving run_id   - " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return futureRunId;
	}

	/**
	 * @param runid,quarterType
	 * @return MWRSummaryDTO
	 * @throws GeneralException
	 */
	public MWRSummaryDTO getRecSummary(Long id, Character character) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		MWRSummaryDTO mwrSummary = null;
		try {
//			logger.debug("Summary query: " + GET_QUARTERLY_SUMMARY + ", " + id);
			stmt = conn.prepareStatement(GET_QUARTERLY_SUMMARY);
			stmt.setLong(1, id);

			rs = stmt.executeQuery();
			if (rs.next()) {

				mwrSummary = new MWRSummaryDTO();
				mwrSummary.setRunId(rs.getLong("RUN_ID"));
				mwrSummary.setUnitsActual(rs.getDouble("UNITS_ACTUAL"));
				mwrSummary.setSalesActual(rs.getDouble("REV_ACTUAL"));
				mwrSummary.setMarginActual(rs.getDouble("MAR_ACTUAL"));
				mwrSummary.setMarginPctActual(rs.getDouble("MAR_PCT_ACTUAL"));
				mwrSummary.setUnitsInBetWeeks(rs.getDouble("INBETWEEN_WEEKS_UNITS_PRED"));
				mwrSummary.setSalesInBetWeeks(rs.getDouble("INBETWEEN_WEEKS_REV_PRED"));
				mwrSummary.setMarginInBetWeeks(rs.getDouble("INBETWEEN_WEEKS_MAR_PRED"));
				mwrSummary.setMarginPctInBetWeeks(rs.getDouble("INBETWEEN_WEEKS_MAR_PCT_PRED"));
				mwrSummary.setUnitsPredicted(rs.getDouble("REC_WEEKS_UNITS_PRED"));
				mwrSummary.setSalesTotal(rs.getDouble("REC_WEEKS_REV_PRED"));
				mwrSummary.setMarginTotal(rs.getDouble("REC_WEEKS_MAR_PRED"));
				mwrSummary.setMarginPctTotal(rs.getDouble("REC_WEEKS_MAR_PCT_PRED"));
				mwrSummary.setUnitsTotal(rs.getDouble("UNITS_TOTAL_ILP"));
				mwrSummary.setSalesTotal(rs.getDouble("REV_TOTAL_ILP"));
				mwrSummary.setMarginTotal(rs.getDouble("MAR_TOTAL_ILP"));
				mwrSummary.setMarginPctTotal(rs.getDouble("MAR_PCT_TOTAL_ILP"));
				mwrSummary.setCurrentUnitsTotal(rs.getDouble("CURR_UNITS_ILP"));
				mwrSummary.setCurrentSalesTotal(rs.getDouble("CURR_REVENUE_ILP"));
				mwrSummary.setCurrentMarginTotal(rs.getDouble("CURR_MARGIN_ILP"));
				mwrSummary.setCurrentMarginPctTotal(rs.getDouble("CURR_MAR_PCT_ILP"));
				mwrSummary.setPromoUnits(rs.getDouble("PROMO_UNITS_ILP"));
				mwrSummary.setPromoSales(rs.getDouble("PROMO_REVENUE_ILP"));
				mwrSummary.setPromoMargin(rs.getDouble("PROMO_MARGIN_ILP"));
				mwrSummary.setPromoMarginPct(rs.getDouble("PROMO_MAR_PCT_ILP"));
				mwrSummary.setPriceIndex(rs.getDouble("PRICE_INDEX"));
				mwrSummary.setPriceIndexPromo(rs.getDouble("PRICE_INDEX_PROMO"));
				mwrSummary.setTotalItems(rs.getInt("TOTAL_ITEMS"));
				mwrSummary.setTotalDistinctItems(rs.getInt("TOTAL_ITEMS_LIG"));
				mwrSummary.setTotalEDLPItems(rs.getInt("TOTAL_EDLP_ITEMS"));
				mwrSummary.setTotalDistinctEDLPItems(rs.getInt("TOTAL_EDLP_ITEMS_LIG"));
				mwrSummary.setTotalPromoItems(rs.getInt("TOTAL_PROMO_ITEMS"));
				mwrSummary.setTotalDistinctPromoItems(rs.getInt("TOTAL_PROMO_ITEMS_LIG"));
				mwrSummary.setIsongoingQt(character);
				mwrSummary.setPriceIndexEDLPRec(rs.getDouble("PRICE_BASE_INDEX"));
				mwrSummary.setPriceIndexEDLPCurr(rs.getDouble("CURR_PRICE_INDEX_BASE"));
				mwrSummary.setUnitsTotalClpDlp(rs.getDouble("UNITS_TOTAL"));
				mwrSummary.setSalesTotalClpDlp(rs.getDouble("REV_TOTAL"));
				mwrSummary.setMarginTotalClpDlp(rs.getDouble("MAR_TOTAL"));
				mwrSummary.setMarginPctTotalClpDlp(rs.getDouble("MAR_PCT_TOTAL"));
				mwrSummary.setCurrentUnitsTotalClpDlp(rs.getDouble("CURR_UNITS"));
				mwrSummary.setCurrentSalesTotalClpDlp(rs.getDouble("CURR_REVENUE"));
				mwrSummary.setCurrentMarginTotalClpDlp(rs.getDouble("CURR_MARGIN"));
				mwrSummary.setCurrentMarginPctTotalClpDlp(rs.getDouble("CURR_MAR_PCT"));
				mwrSummary.setTotalRecommendedItems(rs.getInt("TOTAL_REC_ITEMS"));
				mwrSummary.setTotalRecommendedDistinctItems(rs.getInt("TOTAL_REC_ITEMS_LIG"));
				mwrSummary.setTotalCostChangedItems(rs.getInt("TOTAL_COST_CHANGES"));
				mwrSummary.setTotalCostChangedDistinctItems(rs.getInt("TOTAL_COST_CHANGES_LIG"));
				mwrSummary.setTotalCompChangedItems(rs.getInt("TOTAL_COMP_CHANGES"));
				mwrSummary.setTotalCompChangedDistinctItems(rs.getInt("TOTAL_COMP_CHANGES_LIG"));
				mwrSummary.setPriceChangeImpact(rs.getDouble("PRC_CHANGE_IMPACT"));
				mwrSummary.setMarkUP(rs.getDouble("MARK_UP"));
				mwrSummary.setMarkDown(rs.getDouble("MARK_DOWN"));
			}
		} catch (SQLException e) {
			logger.error("Error in getRecSummary() - " + e);
			throw new GeneralException("Error when retrieving run_id  details - " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}

		return mwrSummary;
	}

	/**
	 * @param locationId,LocatinlevelId,ProductId,productLevelId
	 * @return dashboardId
	 * @throws GeneralException
	 * 
	 */

	public long getDashboardId(int locid, int locLevelid, int prodid, int prodLevelid) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet resultset = null;
		long dashboardId = 0;
		try {
			stmt = conn.prepareStatement(GET_DASHBOARD_ID);
			stmt.setInt(1, locid);
			stmt.setInt(2, locLevelid);
			stmt.setInt(3, prodid);
			stmt.setInt(4, prodLevelid);

			resultset = stmt.executeQuery();
			if (resultset.next()) {

				dashboardId = resultset.getLong("DASHBOARD_ID");

			}
		} catch (SQLException e) {
			logger.error("Error in getDashboardId() - " + e);
			throw new GeneralException("Error when retrieving Dashboard Id- " + e);
		} finally {
			PristineDBUtil.close(resultset);
			PristineDBUtil.close(stmt);
		}

		return dashboardId;

	}

	/**
	 * @param conn
	 * @param QuarterlyDashboardDTO
	 * @throws GeneralException
	 */
	public void insertDashboarData(Connection conn, QuarterlyDashboardDTO dashboardData) throws GeneralException {

		long runId = getMultiWeekDashboardRunId(conn);

		insertDashboardheader(runId, dashboardData);

		saveDashboardDetails(runId, dashboardData);
	}

	/**
	 * @param conn
	 * @param QuarterlyDashboardDTO
	 * @throws GeneralException
	 */
	public void insertDashboarDetails(Connection conn, QuarterlyDashboardDTO dashboardData) throws GeneralException {

		long runId = getDashboardId(dashboardData.getLocationId(), dashboardData.getLocationLevelId(),
				dashboardData.getProductId(), dashboardData.getProductLevelId());

		saveDashboardDetails(runId, dashboardData);
	}

	/**
	 * @param conn
	 * @param QuarterlyDashboardDTO
	 * @throws GeneralException
	 */
	private void saveDashboardDetails(long dashboardId, QuarterlyDashboardDTO dashboardData) throws GeneralException {

		/*+ "(DASHBOARD_ID, " + "RUN_ID, UNITS_TOT_PRED, REV_TOT_PRED, MAR_TOT_PRED, MAR_PCT_TOT_PRED, "
				+ " UNITS_TOT_FINAL, REV_TOT_FINAL, MAR_TOT_FINAL, MAR_PCT_TOT_FINAL, PRICE_TOT_INDEX, "
				+ " TOTAL_TOT_ITEMS, TOTAL_ITEMS_TOT_LIG, UNITS_PROMO_PRED, REV_PROMO_PRED, MAR_PROMO_PRED, "
				+ " MAR_PCT_PROMO_PRED, UNITS_PROMO_FINAL, REV_PROMO_FINAL, MAR_PROMO_FINAL, MAR_PCT_PROMO_FINAL, "
				+ " PRICE_PROMO_INDEX, TOTAL_PROMO_ITEMS, TOTAL_ITEMS_PROMO_LIG "
				+ " IS_ON_GOING_QUARTER)" + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";*/
		logger.info("insertDashboarDetails - getRunID():"+dashboardData.getRunID());
		
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_PR_QUARTER_DASHBOARD_DETAIL);
			int counter = 0;
			statement.setLong(++counter, dashboardId);

			statement.setLong(++counter, dashboardData.getRunID());
			statement.setDouble(++counter, dashboardData.getUnits_actual());
			statement.setDouble(++counter, dashboardData.getRev_actual());
			statement.setDouble(++counter, dashboardData.getMar_actual());
			statement.setDouble(++counter, dashboardData.getMar_pct_actual());
			statement.setDouble(++counter, dashboardData.getUnits_pred());
			statement.setDouble(++counter, dashboardData.getRev_pred());
			statement.setDouble(++counter, dashboardData.getMar_pred());
			statement.setDouble(++counter, dashboardData.getMar_pct_pred());
			statement.setDouble(++counter, dashboardData.getUnits_total());
			statement.setDouble(++counter, dashboardData.getRev_total());
			statement.setDouble(++counter, dashboardData.getMar_total());
			statement.setDouble(++counter, dashboardData.getMar_pct_total());
			statement.setDouble(++counter, dashboardData.getPrice_index());
			statement.setInt(++counter, dashboardData.getTotal_items());
			statement.setInt(++counter, dashboardData.getTotal_items_lig());
			statement.setDouble(++counter, dashboardData.getPromoUnits());
			statement.setDouble(++counter, dashboardData.getPromoSales());
			statement.setDouble(++counter, dashboardData.getPromoMargin());
			statement.setDouble(++counter, dashboardData.getPromoMarginPct());
			statement.setDouble(++counter, dashboardData.getPromoUnits());
			statement.setDouble(++counter, dashboardData.getPromoSales());
			statement.setDouble(++counter, dashboardData.getPromoMargin());
			statement.setDouble(++counter, dashboardData.getPromoMarginPct());
			statement.setDouble(++counter, dashboardData.getPromo_price_index());
			statement.setInt(++counter, dashboardData.getTotalPromoItems());
			statement.setInt(++counter, dashboardData.getTotalDistinctPromoItems());
			statement.setString(++counter, String.valueOf(dashboardData.getIs_on_going_quarter()));
			statement.setDouble(++counter, dashboardData.getBase_price_index());
			statement.setDouble(++counter, dashboardData.getUnits_totalClpDlp());
			statement.setDouble(++counter, dashboardData.getRev_totalClpDlp());
			statement.setDouble(++counter, dashboardData.getMar_totalClpDlp());
			statement.setDouble(++counter, dashboardData.getMar_pct_totalClpDlp());
			statement.setInt(++counter, dashboardData.getTotalRecommendedItems());
			statement.setInt(++counter, dashboardData.getTotalRecommendedDistinctItems());
			statement.setInt(++counter, dashboardData.getTotalCostChangedItems());
			statement.setInt(++counter, dashboardData.getTotalCostChangedDistinctItems());
			statement.setInt(++counter, dashboardData.getTotalCompChangedItems());
			statement.setInt(++counter, dashboardData.getTotalCompChangedDistinctItems());
			statement.setDouble(++counter, dashboardData.getCurrentUnitsTotalClpDlp());
			statement.setDouble(++counter, dashboardData.getCurrentSalesTotalClpDlp());
			statement.setDouble(++counter, dashboardData.getCurrentMarginTotalClpDlp());
			statement.setDouble(++counter, dashboardData.getCurrentUnitsTotal());
			statement.setDouble(++counter, dashboardData.getCurrentSalesTotal());
			statement.setDouble(++counter, dashboardData.getCurrentMarginTotal());
			statement.setDouble(++counter, dashboardData.getPrcChangeImpact());
			statement.setDouble(++counter, dashboardData.getMarkUp());
			statement.setDouble(++counter, dashboardData.getMarkDown());
			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("saveDashboardDetails() - Error while inserting PR_QUARTER_DASHBOARD_DETAIL",
					sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * @param conn
	 * @param QuarterlyDashboardDTO
	 * @throws GeneralException
	 */

	private void insertDashboardheader(long runId, QuarterlyDashboardDTO dashboardData) throws GeneralException {

		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_PR_QUARTER_DASHBOARD);
			int counter = 0;
			statement.setLong(++counter, runId);
			statement.setInt(++counter, dashboardData.getLocationLevelId());
			statement.setInt(++counter, dashboardData.getLocationId());
			statement.setInt(++counter, dashboardData.getProductLevelId());
			statement.setInt(++counter, dashboardData.getProductId());

			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("insertDashboardheader() - Error while inserting PR_QUARTER_DASHBOARD", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * @param conn
	 * @return dashboardID
	 * @throws GeneralException
	 */
	private long getMultiWeekDashboardRunId(Connection conn) throws GeneralException {

		long dashboardID = 0;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_MULTI_WEEK_DASHBOARD_RUN_ID);
			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				dashboardID = resultSet.getLong("DASHBOARD_ID");
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("getMultiWeekDashboardRunId() - Error while getting dashboard id", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return dashboardID;
	}

	/**
	 * @param conn,dashboardID
	 * @throws GeneralException
	 */
	public void deleteDashboarData(Connection conn, long dashboardID) throws GeneralException {

		PreparedStatement statement = null;

		try {
			statement = conn.prepareStatement(DELETE_MULTI_WEEK_DASHBOARD_DATA);
			statement.setLong(1, dashboardID);
		//	statement.setString(2, String.valueOf(isOnGoingQuarter));
			statement.executeUpdate();

		} catch (SQLException sqlE) {
			throw new GeneralException("deleteDashboarData() - Error while deleting  dashboard details for id", sqlE);
		} finally {

			PristineDBUtil.close(statement);
		}
	}

}
