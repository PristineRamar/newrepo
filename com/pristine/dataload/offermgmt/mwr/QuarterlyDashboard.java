package com.pristine.dataload.offermgmt.mwr;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.mwr.QuarterlyDashboardDAO;
import com.pristine.dto.offermgmt.mwr.MWRSummaryDTO;
import com.pristine.dto.offermgmt.mwr.QuarterlyDashboardDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class QuarterlyDashboard {

	private static Logger logger = Logger.getLogger("Dashboard");

	private Connection conn = null;

	private QuarterlyDashboardDAO qdao = null;

	private static int locationLevelId;
	private static int locationId;
	private static int productLevelId;
	private static int productId;

	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";

	public QuarterlyDashboard(boolean connect) {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException exe) {
			logger.error("Error while connecting to DB:" + exe);
			System.exit(1);
		}
	}
	
	public QuarterlyDashboard() {}

	public static void main(String[] args) {
		
		PropertyConfigurator.configure("log4j-quarterly-dashboard.properties");
		PropertyManager.initialize("recommendation.properties");
		QuarterlyDashboard d = new QuarterlyDashboard(true);
		if (args.length > 0) {
			for (String arg : args) {

				if (arg.startsWith(LOCATION_LEVEL_ID)) {
					locationLevelId = (Integer.parseInt(arg.substring(LOCATION_LEVEL_ID.length())));
				} else if (arg.startsWith(LOCATION_ID)) {
					locationId = (Integer.parseInt(arg.substring(LOCATION_ID.length())));
				} else if (arg.startsWith(PRODUCT_LEVEL_ID)) {
					productLevelId = (Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length())));
				} else if (arg.startsWith(PRODUCT_ID)) {
					productId = (Integer.parseInt(arg.substring(PRODUCT_ID.length())));
				}
			}
		}

		d.populateDashboardData(locationLevelId, locationId, productLevelId, productId);
	}

	
	public void populateDashboardData(int locationLevelId, int locationId, int productLevelId,
			int productId) {
		try {
			
			populateDashboardData(conn, locationLevelId, locationId, productLevelId, productId);
			
			PristineDBUtil.commitTransaction(conn, "Quarterly Dashboard");
		
		} catch (GeneralException e) {
			logger.error("Exception in populateDashboardData()", e);
			PristineDBUtil.rollbackTransaction(conn, "Exception in populateDashboardData()");
		} finally {
			PristineDBUtil.close(conn);
		}
	}
	
	public void populateDashboardData(Connection conn, int locationLevelId, int locationId, int productLevelId,
			int productId) throws GeneralException {

		HashMap<Long, Character> rid = new HashMap<Long, Character>();
		qdao = new QuarterlyDashboardDAO(conn);

		// get currentDate
		long currentdate = System.currentTimeMillis();
		java.sql.Date date = new java.sql.Date(currentdate);

		// get week start date from currentDate
		String inputDate = DateUtil.getWeekStartDate(date, 0);

		// get latest run id for future recommendation by using max function
		long futureRun_Id = getLatestFutureRecommendation(inputDate, locationLevelId, locationId, productLevelId,
				productId);
		
		if (futureRun_Id != 0) {
			rid.put(futureRun_Id, 'N');

		} else {
			logger.info("No Recommendations for Future Quarter");

			// get latest run id for current recommendation by using max function
			long currentRun_Id = getLatestOnGoingRecommendation(inputDate, locationLevelId, locationId, productLevelId,
					productId);

			if (currentRun_Id != 0) {
				rid.put(currentRun_Id, 'Y');

			} else {
				logger.info("No Recommendations for Current Quarter");
			}
		
		}
		

		if (rid.size() != 0) {

			// get Recommendation Summary from table for run-id's
			List<MWRSummaryDTO> recSummary = getRecommendationDetails(rid);

			// convert MWRSummaryDTO to QuarterlyDashboardDTO List
			List<QuarterlyDashboardDTO> quarterlyDashboard = convertToDashboardObject(recSummary, locationLevelId,
					locationId, productLevelId, productId);

			// Save details in DB Tables,if dashboard_is already present,delete previous
			// details and insert new details
			qdao.saveDashboardDetails(quarterlyDashboard);

			logger.info("populateDashboardData()-Complete----------------------------");
		}
	}

	private List<QuarterlyDashboardDTO> convertToDashboardObject(List<MWRSummaryDTO> recSummary, int locationLevelId,
			int locationId, int productLevelId, int productId) {

		List<QuarterlyDashboardDTO> quarterlyDashboard = new ArrayList<QuarterlyDashboardDTO>();

		for (MWRSummaryDTO summary : recSummary) {
			QuarterlyDashboardDTO qDashboard = new QuarterlyDashboardDTO();
			qDashboard.setRunID(summary.getRunId());
			qDashboard.setLocationId(locationId);
			qDashboard.setLocationLevelId(locationLevelId);
			qDashboard.setProductId(productId);
			qDashboard.setProductLevelId(productLevelId);
			qDashboard.setUnits_actual(summary.getUnitsActual());
			qDashboard.setUnits_pred(summary.getUnitsPredicted());
			qDashboard.setUnits_total(summary.getUnitsTotal());
			qDashboard.setRev_actual(summary.getSalesActual());
			qDashboard.setMar_actual(summary.getMarginActual());
			qDashboard.setMar_pct_actual(summary.getMarginPctActual());
			qDashboard.setUnits_pred(summary.getUnitsPredicted());
			qDashboard.setRev_pred(summary.getSalesPredicted());
			qDashboard.setMar_pred(summary.getMarginPredicted());
			qDashboard.setMar_pct_pred(summary.getMarginPctPredicted());
			qDashboard.setUnits_total(summary.getUnitsTotal());
			qDashboard.setRev_total(summary.getSalesTotal());
			qDashboard.setMar_total(summary.getMarginTotal());
			qDashboard.setMar_pct_total(summary.getMarginPctTotal());
			qDashboard.setPrice_index(summary.getPriceIndex());
			qDashboard.setTotal_items(summary.getTotalItems());
			qDashboard.setTotal_items_lig(summary.getTotalDistinctItems());
			qDashboard.setCurrentUnitsTotal(summary.getCurrentUnitsTotal());
			qDashboard.setCurrentSalesTotal(summary.getCurrentSalesTotal());
			qDashboard.setCurrentMarginTotal(summary.getCurrentMarginTotal());
			qDashboard.setCurrentMarginPctTotal(summary.getCurrentMarginPctTotal());
			qDashboard.setPromoUnits(summary.getPromoUnits());
			qDashboard.setPromoSales(summary.getPromoSales());
			qDashboard.setPromoMargin(summary.getPromoMargin());
			qDashboard.setPromoMarginPct(summary.getPromoMarginPct());
			qDashboard.setPromo_price_index(summary.getPriceIndexPromo());
			qDashboard.setIs_on_going_quarter(summary.getIsongoingQt());
			qDashboard.setTotalPromoItems(summary.getTotalPromoItems());
			qDashboard.setTotalDistinctPromoItems(summary.getTotalDistinctPromoItems());
			qDashboard.setBase_price_index(summary.getPriceIndexEDLPRec());
			qDashboard.setCurrentUnitsTotalClpDlp(summary.getCurrentUnitsTotalClpDlp());
			qDashboard.setCurrentSalesTotalClpDlp(summary.getCurrentSalesTotalClpDlp());
			qDashboard.setCurrentMarginTotalClpDlp(summary.getCurrentMarginTotalClpDlp());
			qDashboard.setCurrentMarginPctTotalClpDlp(summary.getCurrentMarginPctTotalClpDlp());
			qDashboard.setUnits_totalClpDlp(summary.getUnitsTotalClpDlp());
			qDashboard.setRev_totalClpDlp(summary.getSalesTotalClpDlp());
			qDashboard.setMar_totalClpDlp(summary.getMarginTotalClpDlp());
			qDashboard.setMar_pct_totalClpDlp(summary.getMarginPctTotalClpDlp());
			qDashboard.setTotalRecommendedItems(summary.getTotalRecommendedItems());
			qDashboard.setTotalRecommendedDistinctItems(summary.getTotalRecommendedDistinctItems());
			qDashboard.setTotalCostChangedItems(summary.getTotalCostChangedItems());
			qDashboard.setTotalCostChangedDistinctItems(summary.getTotalCostChangedDistinctItems());
			qDashboard.setTotalCompChangedItems(summary.getTotalCompChangedItems());
			qDashboard.setTotalCompChangedDistinctItems(summary.getTotalCompChangedDistinctItems());
			qDashboard.setPrcChangeImpact(summary.getPriceChangeImpact());
			qDashboard.setMarkUp(summary.getMarkUP());
			qDashboard.setMarkDown(summary.getMarkDown());
			quarterlyDashboard.add(qDashboard);
		}

		return quarterlyDashboard;

	}

	private List<MWRSummaryDTO> getRecommendationDetails(HashMap<Long, Character> rid) throws GeneralException {

		List<MWRSummaryDTO> getSummary = new ArrayList<MWRSummaryDTO>();

		for (Entry<Long, Character> entry : rid.entrySet()) {
			MWRSummaryDTO summary = new MWRSummaryDTO();
			summary = qdao.getRecSummary(entry.getKey(), entry.getValue());
			getSummary.add(summary);
		}

		return getSummary;

	}

	private long getLatestFutureRecommendation(String inputDate, int locationLevelId, int locationId,
			int productLevelId, int productId) throws GeneralException {
		long currentRunID = qdao.getFutureQuarterRunId(inputDate, productId, productLevelId, locationLevelId,
				locationId);

		return currentRunID;

	}

	private long getLatestOnGoingRecommendation(String inputDate, int locationLevelId, int locationId,
			int productLevelId, int productId) throws GeneralException {
		long futureRunID = qdao.getCurrentQuarterRunId(inputDate, productId, productLevelId, locationLevelId,
				locationId);

		return futureRunID;

	}

}
