package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.pristine.util.PristineDBUtil;
import org.apache.log4j.Logger;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.StoreFileExportDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class StoreFileExportGEDAO {
	static Logger logger = Logger.getLogger("StoreFileExportDAO");
	private Connection conn = null;

	private static final String GET_USER_ID = "SELECT DISTINCT USER_ID FROM USER_ROLE_MAP WHERE ROLE_ID IN(?,?,?) AND USER_ID IN"
			+ "(SELECT USER_ID FROM USER_TASK WHERE (VALUE LIKE ? AND VALUE_TYPE = 'LOCATION_LIST') "
			+ "OR (VALUE LIKE ? AND " + "VALUE_TYPE = 'PRODUCT_LIST'))";	
	
	
	public StoreFileExportGEDAO(Connection conn){
		this.conn = conn;
	}

	public List<PRRecommendationRunHeader> getApprovedRecommendationHeader(
			Connection conn) throws GeneralException {
		
		List<PRRecommendationRunHeader> recHeaderList = new ArrayList<PRRecommendationRunHeader>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		 int totalDays = 1;
		 
		 // Get no of days data to consider for export
		 try{
			 totalDays = Integer.parseInt(PropertyManager.getProperty("NO_OF_DAYS_DATA_TO_BE_EXPORTED", ""));
		 }
		 catch(Exception ex){
		 	logger.error("Parse Error" + ex.getMessage());			 
		 }
		 
		 // Fix the start and end date duration to get the recommendation data
		 Date date = new Date();
		 Calendar c = Calendar.getInstance();
		 c.setTime(date);
		 c.add(Calendar.DATE, -totalDays);
		 Date startDate = c.getTime();
		 c.add(Calendar.DATE, (totalDays + 1));
		 Date endDate = c.getTime();
		 DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		 String stDate = dateFormat.format(startDate);
		 String edDate = dateFormat.format(endDate);
		 
		 logger.info("Processing Start Date:- " + stDate);
		 logger.info("Processing End Date:- " + edDate);		
		 
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT LOCATION_LEVEL_ID, LOCATION_ID,");
			sb.append(" PRODUCT_LEVEL_ID, PRODUCT_ID, MAX(RUN_ID) AS RUN_ID");
			sb.append(" FROM PR_RECOMMENDATION_RUN_HEADER"); 
			sb.append(" WHERE STATUS = ").append(PRConstants.STATUS_APPROVED);
			sb.append(" AND (APPROVED >= TO_DATE('" + stDate + "', 'MM/DD/YYYY')");
			sb.append(" AND APPROVED < TO_DATE('" + edDate + "', 'MM/DD/YYYY'))");
			sb.append(" GROUP BY LOCATION_LEVEL_ID, LOCATION_ID,");
			sb.append(" PRODUCT_LEVEL_ID, PRODUCT_ID");
			
			logger.debug("getApprovedRecommendationHeader SQL:- " + sb.toString());
			
			statement = conn.prepareStatement(sb.toString());
			 
			resultSet = statement.executeQuery();

			PRRecommendationRunHeader recHeaderObj = null;
			while (resultSet.next()) {
				recHeaderObj = new PRRecommendationRunHeader();
				recHeaderObj.setRunId(resultSet.getLong("RUN_ID"));
				recHeaderObj.setLocationLevelId(resultSet.getInt("LOCATION_LEVEL_ID"));
				recHeaderObj.setLocationId(resultSet.getInt("LOCATION_ID"));
				recHeaderObj.setProductLevelId(resultSet.getInt("PRODUCT_LEVEL_ID"));
				recHeaderObj.setProductId(resultSet.getInt("PRODUCT_ID"));
				recHeaderList.add(recHeaderObj);
			}
		} catch (SQLException e) {
			logger.error("com.pristine.dao.offermgmt.StoreFileExportGEDAO.getApprovedRecommendationHeader:- Error processing data " + e.toString(), e);
			throw new GeneralException("Error while getting recommendation header" + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return recHeaderList;
	}
	
	public ArrayList<StoreFileExportDTO> getRecommendationDataForStoreExport(
			long runId, ArrayList<StoreFileExportDTO> storeFileExportMap) 
													throws GeneralException{
	
	PreparedStatement stmt = null;
	ResultSet rs = null;

	StringBuilder sb = new StringBuilder();
	
	try{
		
		sb.append("SELECT");
		sb.append(" PR.RUN_ID, PR.COMP_STR_ID, PR.ITEM_CODE,");
		sb.append(" CASE WHEN PR.OVERRIDE_REG_PRICE > 0 THEN PR.OVERRIDE_REG_PRICE");
		sb.append(" ELSE PR.RECOMMENDED_REG_PRICE END AS RECOMMENDED_REG_PRICE,");
		sb.append(" CASE WHEN PR.OVERRIDE_REG_PRICE > 0 THEN PR.OVERRIDE_REG_MULTIPLE");
		sb.append(" ELSE PR.RECOMMENDED_REG_MULTIPLE END AS RECOMMENDED_REG_MULTIPLE,");
		sb.append(" PR.CUR_COMP_REG_MULTIPLE, PR.CUR_COMP_REG_PRICE,");
		sb.append(" PR.CUR_REG_PRICE, PR.CUR_REG_MULTIPLE, PR.IS_HOLD,");
		sb.append(" IL.RETAILER_ITEM_CODE,");
		sb.append(" TO_CHAR(REG_EFF_DATE, 'yyyy-mm-dd') AS REG_EFF_DATE,");
		sb.append(" SIM.LEVEL_ID AS BASE_STORE_ID, SIM.PRICE_ZONE_ID, SIM.VENDOR_ID,");
		sb.append(" RPZ.ZONE_NUM ,");
		sb.append(" CASE WHEN SIM.DIST_FLAG = 'D' THEN VL.VENDOR_NUMBER");
		sb.append(" ELSE '' END AS VENDOR_NUMBER");
		sb.append(" FROM PR_RECOMMENDATION PR");

		sb.append(" JOIN PR_RECOMMENDATION_RUN_HEADER RH"); 
		sb.append(" ON RH.RUN_ID = PR.RUN_ID");
		
		sb.append(" JOIN PR_PRODUCT_LOCATION_MAPPING PLM"); 
		sb.append(" ON PLM.LOCATION_LEVEL_ID = RH.LOCATION_LEVEL_ID"); 
		sb.append(" AND PLM.LOCATION_ID = RH.LOCATION_ID"); 
		sb.append(" AND PLM.PRODUCT_LEVEL_ID = RH.PRODUCT_LEVEL_ID"); 
		sb.append(" AND PLM.PRODUCT_ID = RH.PRODUCT_ID"); 
		
		sb.append(" JOIN PR_PRODUCT_LOCATION_STORE PLS"); 
		sb.append(" ON PLS.PRODUCT_LOCATION_MAPPING_ID = PLM.PRODUCT_LOCATION_MAPPING_ID");
		
		sb.append(" JOIN STORE_ITEM_MAP SIM"); 
		sb.append(" ON SIM.LEVEL_ID = PLS.STORE_ID"); 
		sb.append(" AND PR.ITEM_CODE = SIM.ITEM_CODE AND IS_AUTHORIZED = 'Y'");
		
		sb.append(" JOIN RETAIL_PRICE_ZONE RPZ"); 
		sb.append(" ON RPZ.PRICE_ZONE_ID = SIM.PRICE_ZONE_ID");
		
		sb.append(" JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = PR.ITEM_CODE");
		
		sb.append(" LEFT JOIN VENDOR_LOOKUP VL ON SIM.VENDOR_ID = VL.VENDOR_ID");
		
		sb.append(" WHERE PR.RUN_ID = ").append(runId).append(" AND PR.LIR_IND = 'N'");
		sb.append(" AND (IS_HOLD IS NULL OR IS_HOLD = '' OR IS_HOLD = 'N')");
		sb.append(" AND ((PR.OVERRIDE_REG_PRICE > 0 AND PR.CUR_REG_PRICE <> PR.OVERRIDE_REG_PRICE)");
		sb.append(" OR ((PR.OVERRIDE_REG_PRICE IS NULL OR PR.OVERRIDE_REG_PRICE = 0) AND PR.CUR_REG_PRICE <> PR.RECOMMENDED_REG_PRICE))");
		
		logger.debug("SQL:- " + sb.toString());
		stmt = conn.prepareStatement(sb.toString());
		rs = stmt.executeQuery();

		logger.debug("Orgaize the outdata into collection...");
		
		HashMap<String, String> zoneItemMap = new HashMap<String, String>(); 
		int cnt=0;
		while(rs.next()){
			String zoneStr = rs.getString("ZONE_NUM"); 
			String zoneItemStr =zoneStr + "_" +  rs.getString("RETAILER_ITEM_CODE");
			
			if (!zoneItemMap.containsKey(zoneItemStr)){

				cnt++;				
				
				StoreFileExportDTO dto = new StoreFileExportDTO();
				dto.setRunId(rs.getLong("RUN_ID"));
				dto.setRetItemCode(rs.getString("RETAILER_ITEM_CODE"));
				//dto.setCompStrNo(rs.getString("COMP_STR_NO"));
				dto.setBannerCode(zoneStr.split("-")[0]);
				dto.setZoneNo(zoneStr.split("-")[1]);

				if (rs.getInt("CUR_REG_MULTIPLE") == 0)
					dto.setCurrentQuantity(0);
				else
					dto.setCurrentQuantity(rs.getInt("CUR_REG_MULTIPLE"));
			
				if (rs.getDouble("CUR_REG_PRICE") == 0)
					dto.setCurrentPrice(0);
				else
					dto.setCurrentPrice(rs.getDouble("CUR_REG_PRICE"));
			
				dto.setQuantity(rs.getInt("RECOMMENDED_REG_MULTIPLE"));
				dto.setPrice(rs.getDouble("RECOMMENDED_REG_PRICE"));
				dto.setRegEffectiveDate(rs.getString("REG_EFF_DATE"));
			
				if (rs.getInt("CUR_COMP_REG_MULTIPLE") == 0)
					dto.setCompQuantity(0);
				else
					dto.setCompQuantity(rs.getInt("CUR_COMP_REG_MULTIPLE"));
			
				if (rs.getDouble("CUR_COMP_REG_PRICE") == 0)
					dto.setCompPrice(0);
				else
					dto.setCompPrice(rs.getDouble("CUR_COMP_REG_PRICE"));
			
				if(rs.getString("VENDOR_NUMBER") == null)
					dto.setVendorNo("");
				else
					dto.setVendorNo(rs.getString("VENDOR_NUMBER"));				
			
				storeFileExportMap.add(dto);
				zoneItemMap.put(zoneItemStr, "");
			}
		}
		
		logger.debug("Total Prices to export: " + cnt);
	}
	catch(SQLException ex){
		logger.error("com.pristine.dao.offermgmt.StoreFileExportGEDAO.getRecommendationDataForStoreExport() - Error when getting recommendat data - " + ex.getMessage());
		throw new GeneralException("Error when getting recommendat data - " + ex.getMessage());
	}finally{
		PristineDBUtil.close(rs);
		PristineDBUtil.close(stmt);
	}
	return storeFileExportMap;
}

	public int updateRecommendationStatus(Connection conn, 
		List<PRRecommendationRunHeader> recHeaderList) throws GeneralException{
		
		int status =0;
		
		StringBuilder sb = new StringBuilder();
		PreparedStatement stmt = null;
		
		StringBuilder sbRunIDs = new StringBuilder();
		
		for (int i=0; i < recHeaderList.size(); i++) {
			sbRunIDs.append(recHeaderList.get(i).getRunId());
			sbRunIDs.append(",");
		}

		sbRunIDs.setLength(sbRunIDs.length() - 1);
		
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm");
		Date date = new Date();
		String currentDate = dateFormat.format(date);
		
		try {
		sb.append("UPDATE PR_RECOMMENDATION_RUN_HEADER SET");
		sb.append(" STATUS = " + PRConstants.STATUS_EXPORTED );
		sb.append(", EXPORTED_BY = 'prestolive',");
		sb.append(" EXPORTED = TO_DATE('").append(currentDate).append("', 'MM/dd/yyyy HH24:MI') ");
		sb.append(" WHERE STATUS = " + PRConstants.STATUS_APPROVED );
		sb.append(" AND RUN_ID IN (" + sbRunIDs + ")");

		stmt = conn.prepareStatement(sb.toString());
		stmt.executeUpdate();
		status = 1;
		
		} catch (SQLException exception) {
			logger.error("com.pristine.dao.offermgmt.StoreFileExportGEDAO.updateRecommendationStatus() - Error while updating recommendation stauts" + exception.toString());
			status = 0;
			throw new GeneralException(exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}

		return status;
	}
	
	
	/* New Methods for quarterly recommendation - Begins */

	public List<PRRecommendationRunHeader> getApprovedQuarterlyRecHeader(
			Connection conn) throws GeneralException {
		
		List<PRRecommendationRunHeader> recHeaderList = new ArrayList<PRRecommendationRunHeader>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		 int totalDays = 1;
		 
		 // Get no of days data to consider for export
		 try{
			 totalDays = Integer.parseInt(PropertyManager.getProperty("NO_OF_DAYS_DATA_TO_BE_EXPORTED", ""));
		 }
		 catch(Exception ex){
		 	logger.error("Parse Error" + ex.getMessage());			 
		 }
		 
		 // Fix the start and end date duration to get the recommendation data
		 Date date = new Date();
		 Calendar c = Calendar.getInstance();
		 c.setTime(date);
		 c.add(Calendar.DATE, -totalDays);
		 Date startDate = c.getTime();
		 c.add(Calendar.DATE, (totalDays + 1));
		 Date endDate = c.getTime();
		 DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		 String stDate = dateFormat.format(startDate);
		 String edDate = dateFormat.format(endDate);
		 
		 logger.info("Processing Start Date:- " + stDate);
		 logger.info("Processing End Date:- " + edDate);		
		 
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT LOCATION_LEVEL_ID, LOCATION_ID,");
			sb.append(" PRODUCT_LEVEL_ID, PRODUCT_ID, RUN_ID");
			sb.append(" FROM PR_QUARTER_REC_HEADER WHERE RUN_ID IN ");
			sb.append(" (SELECT DISTINCT RUN_ID FROM PR_PRICE_EXPORT) ");
			/*sb.append(" WHERE STATUS = ").append(PRConstants.STATUS_APPROVED);
			sb.append(" AND (APPROVED >= TO_DATE('" + stDate + "', 'MM/DD/YYYY')");
			sb.append(" AND APPROVED < TO_DATE('" + edDate + "', 'MM/DD/YYYY'))");
			sb.append(" GROUP BY LOCATION_LEVEL_ID, LOCATION_ID,");
			sb.append(" PRODUCT_LEVEL_ID, PRODUCT_ID");*/
			
			logger.debug("getApprovedQuarterlyRecHeader SQL:- " + sb.toString());
			
			statement = conn.prepareStatement(sb.toString());
			 
			resultSet = statement.executeQuery();

			PRRecommendationRunHeader recHeaderObj = null;
			while (resultSet.next()) {
				recHeaderObj = new PRRecommendationRunHeader();
				recHeaderObj.setRunId(resultSet.getLong("RUN_ID"));
				recHeaderObj.setLocationLevelId(resultSet.getInt("LOCATION_LEVEL_ID"));
				recHeaderObj.setLocationId(resultSet.getInt("LOCATION_ID"));
				recHeaderObj.setProductLevelId(resultSet.getInt("PRODUCT_LEVEL_ID"));
				recHeaderObj.setProductId(resultSet.getInt("PRODUCT_ID"));
				recHeaderList.add(recHeaderObj);
			}
		} catch (SQLException e) {
			logger.error("com.pristine.dao.offermgmt.StoreFileExportGEDAO.getApprovedQuarterlyRecHeader:- Error processing data " + e.toString(), e);
			throw new GeneralException("Error while getting recommendation header" + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return recHeaderList;
	}
	
	public ArrayList<StoreFileExportDTO> getQuarterlyRecDataForStoreExport(
			long runId, ArrayList<StoreFileExportDTO> storeFileExportMap) 
													throws GeneralException{
	PreparedStatement stmt = null;
	ResultSet rs = null;

	StringBuilder sb = new StringBuilder();
	
	try{

		sb.append("SELECT");
		sb.append(" PR.RUN_ID, PR.COMP_STR_ID, PR.PRODUCT_ID AS ITEM_CODE,");
		sb.append(" CASE WHEN PR.OVERRIDE_REG_PRICE > 0 THEN PR.OVERRIDE_REG_PRICE");
		sb.append(" ELSE PR.REC_REG_PRICE END AS RECOMMENDED_REG_PRICE,");
		sb.append(" CASE WHEN PR.OVERRIDE_REG_PRICE > 0 THEN PR.OVERRIDE_REG_MULTIPLE");
		sb.append(" ELSE PR.REC_REG_MULTIPLE END AS RECOMMENDED_REG_MULTIPLE,");
		sb.append(" PR.COMP_REG_MULTIPLE AS CUR_COMP_REG_MULTIPLE, PR.COMP_REG_PRICE AS CUR_COMP_REG_PRICE,");
		sb.append(" PR.REG_PRICE AS CUR_REG_PRICE, PR.REG_MULTIPLE  AS CUR_REG_MULTIPLE, PR.IS_HOLD,");
		sb.append(" IL.RETAILER_ITEM_CODE,");
		sb.append(" TO_CHAR(REG_EFF_DATE, 'yyyy-mm-dd') AS REG_EFF_DATE,");
		sb.append(" SIM.LEVEL_ID AS BASE_STORE_ID, SIM.PRICE_ZONE_ID, SIM.VENDOR_ID,");
		sb.append(" RPZ.ZONE_NUM ,");
		sb.append(" CASE WHEN SIM.DIST_FLAG = 'D' THEN VL.VENDOR_NUMBER");
		sb.append(" ELSE '' END AS VENDOR_NUMBER");
		sb.append(" FROM PR_QUARTER_REC_ITEM PR");
		sb.append(" JOIN PR_QUARTER_REC_HEADER RH");
		sb.append(" ON RH.RUN_ID = PR.RUN_ID");
		
		sb.append(" JOIN PR_PRODUCT_LOCATION_MAPPING PLM");
		sb.append(" ON PLM.LOCATION_LEVEL_ID = RH.LOCATION_LEVEL_ID");
		sb.append(" AND PLM.LOCATION_ID = RH.LOCATION_ID");
		sb.append(" AND PLM.PRODUCT_LEVEL_ID = RH.PRODUCT_LEVEL_ID");
		sb.append(" AND PLM.PRODUCT_ID = RH.PRODUCT_ID");
		
		sb.append(" JOIN PR_PRODUCT_LOCATION_STORE PLS");
		sb.append(" ON PLS.PRODUCT_LOCATION_MAPPING_ID = PLM.PRODUCT_LOCATION_MAPPING_ID");
		
		sb.append(" JOIN STORE_ITEM_MAP SIM");
		sb.append(" ON SIM.LEVEL_ID = PLS.STORE_ID");
		sb.append(" AND PR.PRODUCT_ID = SIM.ITEM_CODE AND IS_AUTHORIZED = 'Y'");
		
		sb.append(" JOIN RETAIL_PRICE_ZONE RPZ");
		sb.append(" ON RPZ.PRICE_ZONE_ID = SIM.PRICE_ZONE_ID");
		
		sb.append(" JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = PR.PRODUCT_ID");
		
		sb.append(" LEFT JOIN VENDOR_LOOKUP VL ON SIM.VENDOR_ID = VL.VENDOR_ID");
		
		sb.append(" WHERE PR.RUN_ID = ").append(runId).append(" AND PR.PRODUCT_LEVEL_ID = 1");
		sb.append(" AND PR.CAL_TYPE = 'Q'");
		sb.append(" AND (IS_HOLD IS NULL OR IS_HOLD = '' OR IS_HOLD = 'N')");
		sb.append(" AND ((PR.OVERRIDE_REG_PRICE > 0 AND PR.REG_PRICE <> PR.OVERRIDE_REG_PRICE)");
		sb.append(" OR ((PR.OVERRIDE_REG_PRICE IS NULL OR PR.OVERRIDE_REG_PRICE = 0) AND PR.REG_PRICE <> PR.REC_REG_PRICE))");		
		
		logger.debug("getApprovedQuarterlyRecHeader SQL:- " + sb.toString());
		stmt = conn.prepareStatement(sb.toString());
		rs = stmt.executeQuery();

		logger.debug("Orgaize the outdata into collection...");
		
		HashMap<String, String> zoneItemMap = new HashMap<String, String>(); 
		int cnt=0;
		while(rs.next()){
			String zoneStr = rs.getString("ZONE_NUM"); 
			String zoneItemStr =zoneStr + "_" +  rs.getString("RETAILER_ITEM_CODE");
			
			if (!zoneItemMap.containsKey(zoneItemStr)){

				cnt++;				
				
				StoreFileExportDTO dto = new StoreFileExportDTO();
				dto.setRunId(rs.getLong("RUN_ID"));
				dto.setRetItemCode(rs.getString("RETAILER_ITEM_CODE"));
				//dto.setCompStrNo(rs.getString("COMP_STR_NO"));
				dto.setBannerCode(zoneStr.split("-")[0]);
				dto.setZoneNo(zoneStr.split("-")[1]);

				if (rs.getInt("CUR_REG_MULTIPLE") == 0)
					dto.setCurrentQuantity(0);
				else
					dto.setCurrentQuantity(rs.getInt("CUR_REG_MULTIPLE"));
			
				if (rs.getDouble("CUR_REG_PRICE") == 0)
					dto.setCurrentPrice(0);
				else
					dto.setCurrentPrice(rs.getDouble("CUR_REG_PRICE"));
			
				dto.setQuantity(rs.getInt("RECOMMENDED_REG_MULTIPLE"));
				dto.setPrice(rs.getDouble("RECOMMENDED_REG_PRICE"));
				dto.setRegEffectiveDate(rs.getString("REG_EFF_DATE"));
			
				if (rs.getInt("CUR_COMP_REG_MULTIPLE") == 0)
					dto.setCompQuantity(0);
				else
					dto.setCompQuantity(rs.getInt("CUR_COMP_REG_MULTIPLE"));
			
				if (rs.getDouble("CUR_COMP_REG_PRICE") == 0)
					dto.setCompPrice(0);
				else
					dto.setCompPrice(rs.getDouble("CUR_COMP_REG_PRICE"));
			
				if(rs.getString("VENDOR_NUMBER") == null)
					dto.setVendorNo("");
				else
					dto.setVendorNo(rs.getString("VENDOR_NUMBER"));				
			
				storeFileExportMap.add(dto);
				zoneItemMap.put(zoneItemStr, "");
			}
		}
		
		logger.debug("Total Prices to export: " + cnt);
	}
	catch(SQLException ex){
		logger.error("com.pristine.dao.offermgmt.StoreFileExportGEDAO.getQuarterlyRecDataForStoreExport() - Error when getting recommendat data - " + ex.getMessage());
		throw new GeneralException("Error when getting recommendat data - " + ex.getMessage());
	}finally{
		PristineDBUtil.close(rs);
		PristineDBUtil.close(stmt);
	}
	return storeFileExportMap;
}
	
	public int updateQuarterlyRecStatus(Connection conn, 
			List<PRRecommendationRunHeader> recHeaderList) throws GeneralException{
			
			int status =0;
			
			StringBuilder sb = new StringBuilder();
			PreparedStatement stmt = null;
			
			StringBuilder sbRunIDs = new StringBuilder();
			
			for (int i=0; i < recHeaderList.size(); i++) {
				sbRunIDs.append(recHeaderList.get(i).getRunId());
				sbRunIDs.append(",");
			}

			sbRunIDs.setLength(sbRunIDs.length() - 1);
			
			DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm");
			Date date = new Date();
			String currentDate = dateFormat.format(date);
			
			try {
			sb.append("UPDATE PR_QUARTER_REC_HEADER SET");
			sb.append(" STATUS = " + PRConstants.STATUS_EXPORTED );
			sb.append(", EXPORTED_BY = '" + PRConstants.BATCH_USER + "' ");
			sb.append(", STATUS_BY = '" + PRConstants.BATCH_USER + "', ");
			sb.append(" EXPORTED = TO_DATE('").append(currentDate).append("', 'MM/dd/yyyy HH24:MI') ");
			sb.append(" WHERE STATUS = " + PRConstants.STATUS_APPROVED );
			sb.append(" AND RUN_ID IN (" + sbRunIDs + ")");

			stmt = conn.prepareStatement(sb.toString());
			stmt.executeUpdate();
			status = 1;
			
			} catch (SQLException exception) {
				logger.error("com.pristine.dao.offermgmt.StoreFileExportGEDAO.updateQuarterlyRecStatus() - Error while updating recommendation stauts" + exception.toString());
				status = 0;
				throw new GeneralException(exception.toString());
			} finally {
				PristineDBUtil.close(stmt);
			}

			return status;
		}
			
	/* New Methods for quarterly recommendation - Ends */
	
	
	public ArrayList<String> getUserIDList(PRRecommendationRunHeader prRecommendationRunHeader) throws GeneralException {
		ArrayList<String> prRecommendationRunHeadersList = new ArrayList<String>();
		if (!(prRecommendationRunHeader == null)) {

			String locationList = "%\"l-l-i\":\"" + prRecommendationRunHeader.getLocationLevelId() + "\",\"l-i\":\""
					+ prRecommendationRunHeader.getLocationId() + "\"%";
			String productList = "%\"p-l-i\":\"" + prRecommendationRunHeader.getProductLevelId() + "\",\"p-i\":\""
					+ prRecommendationRunHeader.getProductId() + "\"%";

			prRecommendationRunHeadersList.addAll(getNotificationList(locationList, productList));
			
		}
		return prRecommendationRunHeadersList;
	}
	
	public ArrayList<String> getNotificationList(String locationList,String productList) throws GeneralException {
		ArrayList<String> prRecommendationRunHeadersList = new ArrayList<String>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String query = new String(GET_USER_ID);
			statement = conn.prepareStatement(query);
			int counter = 0;
			String[] NotificationRoleId = PropertyManager.getProperty("NOTIFICATION_SERVICE_ROLEID").split(",");
			for(String roleId: NotificationRoleId){
			statement.setInt(++counter, Integer.parseInt(roleId));
			}
			statement.setString(++counter, locationList);
			statement.setString(++counter, productList);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String userId = resultSet.getString("USER_ID");
				prRecommendationRunHeadersList.add(userId);
			}
			return prRecommendationRunHeadersList;

		} catch (SQLException e) {
			logger.error("Error while executing getNotificationList " + e.toString(), e);
			throw new GeneralException("Error while executing getNotificationList " + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param recHeaderList
	 * @return STATUS OF DELETE
	 * @throws GeneralException
	 */
	public int deleteExportQueue(Connection conn, 
			List<PRRecommendationRunHeader> recHeaderList) throws GeneralException {

		int status = 0;

		StringBuilder sb = new StringBuilder();
		PreparedStatement stmt = null;

		StringBuilder sbRunIDs = new StringBuilder();

		for (int i = 0; i < recHeaderList.size(); i++) {
			sbRunIDs.append(recHeaderList.get(i).getRunId());
			sbRunIDs.append(",");
		}

		sbRunIDs.setLength(sbRunIDs.length() - 1);

		try {
			sb.append("DELETE FROM PR_PRICE_EXPORT WHERE RUN_ID IN (").append(sbRunIDs).append(")");

			stmt = conn.prepareStatement(sb.toString());
			stmt.executeUpdate();
			status = 1;

		} catch (SQLException exception) {
			logger.error(
					"com.pristine.dao.offermgmt.StoreFileExportGEDAO.deleteExportQueue() - Error while updating recommendation stauts"
							+ exception.toString());
			status = 0;
			throw new GeneralException(exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}

		return status;
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param recHeaderList
	 * @return STATUS OF DELETE
	 * @throws GeneralException
	 */
	public int insertExportStatus(Connection conn, 
			List<PRRecommendationRunHeader> recHeaderList) throws GeneralException {

		int status = 0;

		StringBuilder sb = new StringBuilder();
		sb.append(" INSERT INTO PR_QUARTER_REC_STATUS (RECOMMENDATION_STATUS_ID, RUN_ID, ");
		sb.append(" STATUS, UPDATED_BY, UPDATED, STATUS_ROLE, MESSAGE) ");
		sb.append(" VALUES (RECOMMENDATION_STATUS_ID_SEQ.NEXTVAL, ?, ?, ?, SYSDATE, 0, ?)");
		
		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(sb.toString());
			for(PRRecommendationRunHeader recHeader: recHeaderList) {
				int colCount = 0;
				stmt.setLong(++colCount, recHeader.getRunId());
				stmt.setInt(++colCount, PRConstants.STATUS_EXPORTED);
				stmt.setString(++colCount, PRConstants.BATCH_USER);
				stmt.setString(++colCount, "Exported");
				stmt.addBatch();
			}
			stmt.executeBatch();
			status = 1;
		} catch (SQLException exception) {
			logger.error(
					"com.pristine.dao.offermgmt.StoreFileExportGEDAO.insertExportStatus() - Error while updating recommendation stauts"
							+ exception.toString());
			status = 0;
			throw new GeneralException(exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
		return status;
	}
	
	
	/* Get recommendation details - specific to Rite Aid - Begins*/
	
	public ArrayList<StoreFileExportDTO> getQuarterlyRecDataForStoreExportForRiteAid(
			long runId) 
													throws GeneralException{
	PreparedStatement stmt = null;
	ResultSet rs = null;
	ArrayList<StoreFileExportDTO> storeFileExportMap = new ArrayList<>();
	StringBuilder sb = new StringBuilder();
	
	try{

		/*sb.append("SELECT"); 
		sb.append(" STATUS_BY AS USER_ID, COMMENT_CODE AS REASON_CODE, RUN_ID, ");
		sb.append(" CASE WHEN IS_CHILD_LOC_REC = 1 THEN SECONDARY_ZONE  ELSE PRIMARY_ZONE END AS PRICE_BAND,");
		//sb.append(" CASE WHEN PRODUCT_LEVEL_ID = 11 THEN 'G' ELSE 'I' END AS ITEM_INDICATOR,");
		sb.append(" CASE WHEN ((PRODUCT_LEVEL_ID = 11 AND LIR_CODE LIKE 'ORG-%') OR PRODUCT_LEVEL_ID = 1) THEN 'I' ELSE 'G' END AS ITEM_INDICATOR, ");
		//sb.append(" CASE WHEN PRODUCT_LEVEL_ID = 11 THEN LIR_CODE ELSE RETAILER_ITEM_CODE END AS ITEM_NUMBER_OR_LIR_CODE,");
		sb.append(" CASE WHEN ((PRODUCT_LEVEL_ID = 11 AND LIR_CODE LIKE 'ORG-%') OR PRODUCT_LEVEL_ID = 1) THEN 1 ELSE 11 END AS PRODUCT_LEVEL_ID, ");
		sb.append(" CASE WHEN ((PRODUCT_LEVEL_ID = 11 AND LIR_CODE LIKE 'ORG-%') OR PRODUCT_LEVEL_ID = 1) THEN RETAILER_ITEM_CODE ELSE LIR_CODE END AS ITEM_NUMBER_OR_LIR_CODE, ");
		sb.append(" 1 AS UOM_PIECES, REG_EFF_DATE,");
		sb.append(" CASE WHEN IS_CHILD_LOC_REC = 1 THEN CH_RECOMMENDED_REG_MULTIPLE  ELSE RECOMMENDED_REG_MULTIPLE END AS PRICE_MULTIPLE,");
		sb.append(" CASE WHEN IS_CHILD_LOC_REC = 1 THEN CH_RECOMMENDED_REG_PRICE  ELSE RECOMMENDED_REG_PRICE END AS PRICE,");
		sb.append(" COMMENT_DESC AS APPROVAL_REASON,");
		sb.append(" '' AS COMMENTS,");
		sb.append(" TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD-HH24.MI.SSXFF') AS TIME_STAMP FROM (");
		sb.append(" SELECT PR.RUN_ID, PR.COMP_STR_ID, RH.APPROVAL_REASON_ID, APR.COMMENT_CODE, APR.COMMENT_DESC,");
		sb.append(" PR.PRODUCT_ID AS ITEM_CODE, ");
		//PR.PRODUCT_LEVEL_ID,");
		sb.append(" CASE WHEN PR.OVERRIDE_REG_PRICE > 0 THEN PR.OVERRIDE_REG_PRICE ELSE PR.REC_REG_PRICE END AS RECOMMENDED_REG_PRICE,");
		sb.append(" CASE WHEN PR.OVERRIDE_REG_PRICE > 0 THEN PR.OVERRIDE_REG_MULTIPLE ELSE PR.REC_REG_MULTIPLE END AS RECOMMENDED_REG_MULTIPLE,");
		sb.append(" PR.COMP_REG_MULTIPLE AS CUR_COMP_REG_MULTIPLE, PR.COMP_REG_PRICE AS CUR_COMP_REG_PRICE,");
		sb.append(" PR.REG_PRICE AS CUR_REG_PRICE, PR.REG_MULTIPLE AS CUR_REG_MULTIPLE,");
		sb.append(" PR.IS_HOLD, IL.RETAILER_ITEM_CODE, TO_CHAR(REG_EFF_DATE, 'YYYY-MM-DD') AS REG_EFF_DATE,");
		sb.append(" RPZ_PR.ZONE_NUM AS PRIMARY_ZONE, RPZ_CH.ZONE_NUM AS SECONDARY_ZONE,");
		sb.append(" RH.STATUS_BY, PR.IS_CHILD_LOC_REC,");
		sb.append(" CASE WHEN CHR.OVER_REG_PRICE > 0 THEN CHR.OVER_REG_PRICE ELSE CHR.REC_REG_PRICE END AS CH_RECOMMENDED_REG_PRICE,");
		sb.append(" CASE WHEN CHR.OVER_REG_PRICE > 0 THEN CHR.OVER_REG_MULTIPLE ELSE CHR.REC_REG_MULTIPLE END AS CH_RECOMMENDED_REG_MULTIPLE,");
		//sb.append(" REPLACE(LIG.RET_LIR_CODE, 'ORG-', '') AS LIR_CODE");
		sb.append(" LIG.RET_LIR_CODE AS LIR_CODE");
		sb.append(" FROM PR_QUARTER_REC_ITEM PR");
		sb.append(" LEFT JOIN PR_CHILD_LOCATION_REC CHR ON CHR.PRODUCT_ID = PR.PRODUCT_ID AND CHR.PRODUCT_LEVEL_ID = PR.PRODUCT_LEVEL_ID AND CHR.RUN_ID = PR.RUN_ID");
		sb.append(" LEFT JOIN PR_QUARTER_REC_HEADER RH ON RH.RUN_ID = PR.RUN_ID");
		sb.append(" LEFT JOIN PRICE_APPROVE_COMMENT_LOOKUP APR ON APR.COMMENT_ID = RH.APPROVAL_REASON_ID");		
		sb.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ_CH ON RPZ_CH.PRICE_ZONE_ID = CHR.CHILD_LOCATION_ID");
		sb.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ_PR ON RPZ_PR.PRICE_ZONE_ID = RH.LOCATION_ID");
		sb.append(" JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = PR.PRODUCT_ID");
		sb.append(" LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON PR.RET_LIR_ID = LIG.RET_LIR_ID");
		sb.append(" WHERE PR.RUN_ID =").append(runId);
		sb.append(" AND ((PR.PRODUCT_LEVEL_ID = 1 AND PR.RET_LIR_ID = 0) OR (PR.PRODUCT_LEVEL_ID = 11))");
		sb.append(" AND PR.CAL_TYPE = 'Q'");
		sb.append(" AND (IS_HOLD IS NULL OR IS_HOLD = '' OR IS_HOLD = 'N')");
		sb.append(" AND ((PR.OVERRIDE_REG_PRICE > 0	AND PR.REG_PRICE <> PR.OVERRIDE_REG_PRICE)");
		sb.append(" OR ((PR.OVERRIDE_REG_PRICE IS NULL OR PR.OVERRIDE_REG_PRICE = 0) AND PR.REG_PRICE <> PR.REC_REG_PRICE)))");*/
		
		sb.append(" SELECT ");
		sb.append(" STATUS_BY AS USER_ID, COMMENT_CODE AS REASON_CODE, RUN_ID, ITEM_CODE, RET_LIR_ID, ");
		sb.append(" CASE WHEN IS_CHILD_LOC_REC = 1 THEN SECONDARY_ZONE  ELSE PRIMARY_ZONE END AS PRICE_BAND, ");
		//commented and changed by kirthi, to handle LIG items 
		//sb.append(" CASE WHEN ((PRODUCT_LEVEL_ID = 11 AND LIR_CODE LIKE 'ORG-%') OR PRODUCT_LEVEL_ID = 1) THEN 'I' ELSE 'G' END AS ITEM_INDICATOR, ");
		//sb.append(" CASE WHEN ((PRODUCT_LEVEL_ID = 11 AND LIR_CODE LIKE 'ORG-%') OR PRODUCT_LEVEL_ID = 1) THEN RETAILER_ITEM_CODE ELSE LIR_CODE END AS ITEM_NUMBER_OR_LIR_CODE, ");
		//sb.append(" CASE WHEN ((PRODUCT_LEVEL_ID = 11 AND LIR_CODE LIKE 'ORG-%') OR PRODUCT_LEVEL_ID = 1) THEN 1 ELSE 11 END AS PRODUCT_LEVEL_ID, ");
		
		sb.append(" CASE WHEN ((RET_LIR_ID > 0 AND LIR_CODE LIKE 'ORG-%') OR (PRODUCT_LEVEL_ID = 1 AND RET_LIR_ID = 0)) THEN 'I' ELSE 'G' END AS ITEM_INDICATOR, ");
		sb.append(" CASE WHEN ((RET_LIR_ID > 0 AND LIR_CODE LIKE 'ORG-%') OR (PRODUCT_LEVEL_ID = 1 AND RET_LIR_ID = 0)) THEN RETAILER_ITEM_CODE ELSE LIR_CODE END AS ITEM_NUMBER_OR_LIR_CODE, ");
		sb.append(" CASE WHEN ((RET_LIR_ID > 0 AND LIR_CODE LIKE 'ORG-%') OR (PRODUCT_LEVEL_ID = 1 AND RET_LIR_ID = 0)) THEN 1 ELSE 11 END AS PRODUCT_LEVEL_ID, ");
		
		sb.append(" 1 AS UOM_PIECES, REG_EFF_DATE, ");
		sb.append(" CASE WHEN IS_CHILD_LOC_REC = 1 THEN CH_RECOMMENDED_REG_MULTIPLE  ELSE RECOMMENDED_REG_MULTIPLE END AS PRICE_MULTIPLE, ");
		sb.append(" CASE WHEN IS_CHILD_LOC_REC = 1 THEN CH_RECOMMENDED_REG_PRICE  ELSE RECOMMENDED_REG_PRICE END AS PRICE, ");
		sb.append(" COMMENT_DESC AS APPROVAL_REASON, ");
		sb.append(" '' AS COMMENTS, ");
		sb.append(" TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD-HH24.MI.SSXFF') AS TIME_STAMP FROM (  ");
		sb.append(" SELECT PR.RUN_ID, PR.COMP_STR_ID, RH.APPROVAL_REASON_ID, APR.COMMENT_CODE, APR.COMMENT_DESC, ");
		sb.append(" PR.PRODUCT_ID AS ITEM_CODE, PR.PRODUCT_LEVEL_ID,  ");
		sb.append(" CASE WHEN PR.OVERRIDE_REG_PRICE > 0 THEN PR.OVERRIDE_REG_PRICE ELSE PR.REC_REG_PRICE END AS RECOMMENDED_REG_PRICE,  ");
		sb.append(" CASE WHEN PR.OVERRIDE_REG_PRICE > 0 THEN PR.OVERRIDE_REG_MULTIPLE ELSE PR.REC_REG_MULTIPLE END AS RECOMMENDED_REG_MULTIPLE,  ");
		sb.append(" PR.COMP_REG_MULTIPLE AS CUR_COMP_REG_MULTIPLE, PR.COMP_REG_PRICE AS CUR_COMP_REG_PRICE, ");
		sb.append(" PR.REG_PRICE AS CUR_REG_PRICE, PR.REG_MULTIPLE AS CUR_REG_MULTIPLE, ");
		sb.append(" PR.IS_HOLD, IL.RETAILER_ITEM_CODE, PR.RET_LIR_ID, TO_CHAR(REG_EFF_DATE, 'YYYY-MM-DD') AS REG_EFF_DATE, ");
		sb.append(" RPZ_PR.ZONE_NUM AS PRIMARY_ZONE, RPZ_CH.ZONE_NUM AS SECONDARY_ZONE, ");
		sb.append(" RH.STATUS_BY, PR.IS_CHILD_LOC_REC, ");
		sb.append(" CASE WHEN CHR.OVER_REG_PRICE > 0 THEN CHR.OVER_REG_PRICE ELSE CHR.REC_REG_PRICE END AS CH_RECOMMENDED_REG_PRICE, ");
		sb.append(" CASE WHEN CHR.OVER_REG_PRICE > 0 THEN CHR.OVER_REG_MULTIPLE ELSE CHR.REC_REG_MULTIPLE END AS CH_RECOMMENDED_REG_MULTIPLE, ");
		sb.append(" LIG.RET_LIR_CODE AS LIR_CODE ");
		sb.append(" FROM PR_QUARTER_REC_ITEM PR ");
		sb.append(" LEFT JOIN PR_CHILD_LOCATION_REC CHR ON CHR.PRODUCT_ID = PR.PRODUCT_ID AND CHR.PRODUCT_LEVEL_ID = PR.PRODUCT_LEVEL_ID AND CHR.RUN_ID = PR.RUN_ID ");
		sb.append(" LEFT JOIN PR_QUARTER_REC_HEADER RH ON RH.RUN_ID = PR.RUN_ID ");
		sb.append(" LEFT JOIN PRICE_APPROVE_COMMENT_LOOKUP APR ON APR.COMMENT_ID = RH.APPROVAL_REASON_ID	 ");	
		sb.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ_CH ON RPZ_CH.PRICE_ZONE_ID = CHR.CHILD_LOCATION_ID ");
		sb.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ_PR ON RPZ_PR.PRICE_ZONE_ID = RH.LOCATION_ID ");
		sb.append(" JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = PR.PRODUCT_ID ");
		sb.append(" LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON PR.RET_LIR_ID = LIG.RET_LIR_ID ");
		sb.append(" WHERE PR.RUN_ID in (").append(runId).append(")");
		sb.append(" AND PR.PRODUCT_LEVEL_ID = 1 ");
		//commented by Kirthi on 29/07/2021. 
		//To avoid LIG rep item in export. For Issue fix PROM-2246
		//+ "AND PR.RET_LIR_ID = 0) ");
		//+ "OR (PR.PRODUCT_LEVEL_ID = 11)) ");
		sb.append(" AND PR.CAL_TYPE = 'Q' ");
		sb.append(" AND (IS_HOLD IS NULL OR IS_HOLD = '' OR IS_HOLD = 'N') ");
		sb.append(" AND ((PR.OVERRIDE_REG_PRICE > 0	AND PR.REG_PRICE <> PR.OVERRIDE_REG_PRICE) ");
		sb.append(" OR ((PR.OVERRIDE_REG_PRICE IS NULL OR PR.OVERRIDE_REG_PRICE = 0) AND PR.REG_PRICE <> PR.REC_REG_PRICE))) ");
		
		logger.debug("getQuarterlyRecDataForStoreExportForRiteAid SQL:- " + sb.toString());
		stmt = conn.prepareStatement(sb.toString());
		rs = stmt.executeQuery();

		logger.debug("Orgaize the outdata into collection...");
		
		HashMap<String, String> zoneItemMap = new HashMap<String, String>(); 
		int cnt=0;
		while(rs.next()){
			
			String zoneStr = rs.getString("PRICE_BAND");
			String itemInd = rs.getString("ITEM_INDICATOR");
			String zoneItemStr =zoneStr + "_" +  itemInd + "_" + rs.getString("ITEM_NUMBER_OR_LIR_CODE");
			
			if (!zoneItemMap.containsKey(zoneItemStr)){

				cnt++;				
				
				StoreFileExportDTO dto = new StoreFileExportDTO();
				
				dto.setRunId(rs.getLong("RUN_ID"));
				dto.setUserId(rs.getString("USER_ID"));
				dto.setZoneNo(rs.getString("PRICE_BAND"));
				dto.setItemIndicator(rs.getString("ITEM_INDICATOR"));
				dto.setRetItemCode(rs.getString("ITEM_NUMBER_OR_LIR_CODE"));
				dto.setUOMData(rs.getString("UOM_PIECES"));
				dto.setRegEffectiveDate(rs.getString("REG_EFF_DATE"));
				dto.setQuantity(rs.getInt("PRICE_MULTIPLE"));
				dto.setPrice(rs.getDouble("PRICE"));
				dto.setReasonCode(rs.getString("REASON_CODE"));
				dto.setComment(rs.getString("COMMENTS"));	
				dto.setTimeStampData(rs.getString("TIME_STAMP"));
				dto.setApprovalReason(rs.getString("APPROVAL_REASON"));
				
				storeFileExportMap.add(dto);
				zoneItemMap.put(zoneItemStr, "");
			}
		}
		
		logger.debug("Total Prices to export: " + cnt);
	}
	catch(SQLException ex){
		logger.error("com.pristine.dao.offermgmt.StoreFileExportGEDAO.getQuarterlyRecDataForStoreExportForRiteAid() - Error when getting recommendat data - " + ex.getMessage());
		throw new GeneralException("Error when getting recommendat data - " + ex.getMessage());
	}finally{
		PristineDBUtil.close(rs);
		PristineDBUtil.close(stmt);
	}
	return storeFileExportMap;
}

	/* Get recommendation details - specific to Fleet Farm - Begins*/
	
	public ArrayList<StoreFileExportDTO> getQuarterlyRecDataForStoreExportForFF(
			long runId) 
													throws GeneralException{
	PreparedStatement stmt = null;
	ResultSet rs = null;
	ArrayList<StoreFileExportDTO> storeFileExportMap = new ArrayList<>();
	StringBuilder sb = new StringBuilder();
	
	try{		
		sb.append(" SELECT ");
		//COMMENT_CODE AS REASON_CODE,
		sb.append(" STATUS_BY AS USER_ID,  RUN_ID, ITEM_CODE, RET_LIR_ID, LOCATION_ID,ZONE_NAME,");
		sb.append(" DEPT_CODE, CAT_CODE,");
		sb.append(" CASE WHEN IS_CHILD_LOC_REC = 1 THEN SECONDARY_ZONE  ELSE PRIMARY_ZONE END AS PRICE_BAND, ");
		sb.append(" CASE WHEN PRODUCT_LEVEL_ID = 1  THEN RETAILER_ITEM_CODE END AS RETAILER_ITEM_CODE,");
		//commented and changed by kirthi, to handle LIG items 
		//sb.append(" CASE WHEN ((PRODUCT_LEVEL_ID = 11 AND LIR_CODE LIKE 'ORG-%') OR PRODUCT_LEVEL_ID = 1) THEN 'I' ELSE 'G' END AS ITEM_INDICATOR, ");
		//sb.append(" CASE WHEN ((PRODUCT_LEVEL_ID = 11 AND LIR_CODE LIKE 'ORG-%') OR PRODUCT_LEVEL_ID = 1) THEN RETAILER_ITEM_CODE ELSE LIR_CODE END AS ITEM_NUMBER_OR_LIR_CODE, ");
		//sb.append(" CASE WHEN ((PRODUCT_LEVEL_ID = 11 AND LIR_CODE LIKE 'ORG-%') OR PRODUCT_LEVEL_ID = 1) THEN 1 ELSE 11 END AS PRODUCT_LEVEL_ID, ");
		
		sb.append(" CASE WHEN ((RET_LIR_ID > 0 AND LIR_CODE LIKE 'ORG-%') OR (PRODUCT_LEVEL_ID = 1 AND RET_LIR_ID = 0)) THEN 'I' ELSE 'G' END AS ITEM_INDICATOR, ");
		//sb.append(" CASE WHEN ((RET_LIR_ID > 0 AND LIR_CODE LIKE 'ORG-%') OR (PRODUCT_LEVEL_ID = 1 AND RET_LIR_ID = 0)) THEN RETAILER_ITEM_CODE ELSE LIR_CODE END AS ITEM_NUMBER_OR_LIR_CODE, ");
		sb.append(" CASE WHEN ((RET_LIR_ID > 0 AND LIR_CODE LIKE 'ORG-%') OR (PRODUCT_LEVEL_ID = 1 AND RET_LIR_ID = 0)) THEN 1 ELSE 11 END AS PRODUCT_LEVEL_ID, ");
		
		sb.append(" 1 AS UOM_PIECES, REG_EFF_DATE, ");
		sb.append(" CASE WHEN IS_CHILD_LOC_REC = 1 THEN CH_RECOMMENDED_REG_MULTIPLE  ELSE RECOMMENDED_REG_MULTIPLE END AS PRICE_MULTIPLE, ");
		sb.append(" CASE WHEN IS_CHILD_LOC_REC = 1 THEN CH_RECOMMENDED_REG_PRICE  ELSE RECOMMENDED_REG_PRICE END AS PRICE, ");
		sb.append(" COMMENT_DESC AS APPROVAL_REASON, ");
		sb.append(" '' AS COMMENTS, ");
		sb.append(" TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD-HH24.MI.SSXFF') AS TIME_STAMP FROM (  ");
		//APR.COMMENT_CODE,
		sb.append(" SELECT PR.RUN_ID, PR.COMP_STR_ID, RH.APPROVAL_REASON_ID,RH.LOCATION_ID AS LOCATION_ID,  APR.COMMENT_DESC, ");
		sb.append(" PR.PRODUCT_ID AS ITEM_CODE, PR.PRODUCT_LEVEL_ID,  ");
		sb.append(" CASE WHEN PR.OVERRIDE_REG_PRICE > 0 THEN PR.OVERRIDE_REG_PRICE ELSE PR.REC_REG_PRICE END AS RECOMMENDED_REG_PRICE,  ");
		sb.append(" CASE WHEN PR.OVERRIDE_REG_PRICE > 0 THEN PR.OVERRIDE_REG_MULTIPLE ELSE PR.REC_REG_MULTIPLE END AS RECOMMENDED_REG_MULTIPLE,  ");
		sb.append(" PR.COMP_REG_MULTIPLE AS CUR_COMP_REG_MULTIPLE, PR.COMP_REG_PRICE AS CUR_COMP_REG_PRICE, ");
		sb.append(" PR.REG_PRICE AS CUR_REG_PRICE, PR.REG_MULTIPLE AS CUR_REG_MULTIPLE, ");
		sb.append(" PR.IS_HOLD, IL.RETAILER_ITEM_CODE, PR.RET_LIR_ID, TO_CHAR(REG_EFF_DATE, 'YYYY-MM-DD') AS REG_EFF_DATE, ");
		sb.append(" RPZ_PR.ZONE_NUM AS PRIMARY_ZONE, RPZ_CH.ZONE_NUM AS SECONDARY_ZONE, ");
		sb.append(" RPZ_PR.NAME AS ZONE_NAME, ");
		sb.append(" RH.STATUS_BY, PR.IS_CHILD_LOC_REC, ");
		sb.append(" PG_DEPT.CODE AS DEPT_CODE, PG_CAT.CODE AS CAT_CODE,");
		sb.append(" CASE WHEN CHR.OVER_REG_PRICE > 0 THEN CHR.OVER_REG_PRICE ELSE CHR.REC_REG_PRICE END AS CH_RECOMMENDED_REG_PRICE, ");
		sb.append(" CASE WHEN CHR.OVER_REG_PRICE > 0 THEN CHR.OVER_REG_MULTIPLE ELSE CHR.REC_REG_MULTIPLE END AS CH_RECOMMENDED_REG_MULTIPLE, ");
		sb.append(" LIG.RET_LIR_CODE AS LIR_CODE ");
		sb.append(" FROM PR_QUARTER_REC_ITEM PR ");
		sb.append(" LEFT JOIN PR_CHILD_LOCATION_REC CHR ON CHR.PRODUCT_ID = PR.PRODUCT_ID AND CHR.PRODUCT_LEVEL_ID = PR.PRODUCT_LEVEL_ID AND CHR.RUN_ID = PR.RUN_ID ");
		sb.append(" LEFT JOIN PR_QUARTER_REC_HEADER RH ON RH.RUN_ID = PR.RUN_ID ");
		sb.append(" LEFT JOIN PRICE_APPROVE_COMMENT_LOOKUP APR ON APR.COMMENT_ID = RH.APPROVAL_REASON_ID	 ");	
		sb.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ_CH ON RPZ_CH.PRICE_ZONE_ID = CHR.CHILD_LOCATION_ID ");
		sb.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ_PR ON RPZ_PR.PRICE_ZONE_ID = RH.LOCATION_ID ");
		sb.append(" JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = PR.PRODUCT_ID ");
		sb.append(" LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON PR.RET_LIR_ID = LIG.RET_LIR_ID ");
		sb.append(" LEFT JOIN ITEM_DETAILS_VIEW IL1 ON IL1.ITEM_CODE = PR.PRODUCT_ID ");
		sb.append(" LEFT JOIN PRODUCT_GROUP PG_DEPT ON PG_DEPT.PRODUCT_ID = IL1.DEPARTMENT_ID ");
		sb.append(" LEFT JOIN PRODUCT_GROUP PG_CAT ON PG_CAT.PRODUCT_ID = IL1.CATEGORY_ID ");
		sb.append(" WHERE PR.RUN_ID in (").append(runId).append(")");
		sb.append(" AND PR.PRODUCT_LEVEL_ID = 1 ");
		//commented by Kirthi on 29/07/2021. 
		//To avoid LIG rep item in export. For Issue fix PROM-2246
		//+ "AND PR.RET_LIR_ID = 0) ");
		//+ "OR (PR.PRODUCT_LEVEL_ID = 11)) ");
		sb.append(" AND PR.CAL_TYPE = 'Q' ");
		sb.append(" AND (IS_HOLD IS NULL OR IS_HOLD = '' OR IS_HOLD = 'N') ");
		sb.append(" AND ((PR.OVERRIDE_REG_PRICE > 0	AND PR.REG_PRICE <> PR.OVERRIDE_REG_PRICE) ");
		sb.append(" OR ((PR.OVERRIDE_REG_PRICE IS NULL OR PR.OVERRIDE_REG_PRICE = 0) AND PR.REG_PRICE <> PR.REC_REG_PRICE))) ");
		
		logger.debug("getQuarterlyRecDataForStoreExportForFF SQL:- " + sb.toString());
		stmt = conn.prepareStatement(sb.toString());
		rs = stmt.executeQuery();

		logger.debug("Orgaize the outdata into collection...");
		
		HashMap<String, String> zoneItemMap = new HashMap<String, String>(); 
		int cnt=0;
		while(rs.next()){
			
			String zoneStr = rs.getString("PRICE_BAND");
			String itemInd = rs.getString("ITEM_INDICATOR");
			String zoneItemStr =zoneStr + "_" +  itemInd + "_" + rs.getString("RETAILER_ITEM_CODE");
			
			if (!zoneItemMap.containsKey(zoneItemStr)){

				cnt++;				
				
				StoreFileExportDTO dto = new StoreFileExportDTO();
				
				dto.setRunId(rs.getLong("RUN_ID"));
				dto.setUserId(rs.getString("USER_ID"));
				dto.setZoneNo(rs.getString("PRICE_BAND"));
				dto.setItemIndicator(rs.getString("ITEM_INDICATOR"));
				dto.setRetItemCode(rs.getString("RETAILER_ITEM_CODE"));
				dto.setUOMData(rs.getString("UOM_PIECES"));
				dto.setRegEffectiveDate(rs.getString("REG_EFF_DATE"));
				dto.setQuantity(rs.getInt("PRICE_MULTIPLE"));
				dto.setPrice(rs.getDouble("PRICE"));
			//	dto.setReasonCode(rs.getString("REASON_CODE"));
				dto.setComment(rs.getString("COMMENTS"));	
				dto.setTimeStampData(rs.getString("TIME_STAMP"));
				dto.setApprovalReason(rs.getString("APPROVAL_REASON"));
				dto.setLocationId(rs.getInt("LOCATION_ID"));
				dto.setProductId(rs.getInt("ITEM_CODE"));
				dto.setZoneName(rs.getString("ZONE_NAME"));
				dto.setDeptId(rs.getString("DEPT_CODE"));
				dto.setCatCode(rs.getString("CAT_CODE"));
				storeFileExportMap.add(dto);
				zoneItemMap.put(zoneItemStr, "");
			}
		}
		
		logger.debug("Total Prices to export: " + cnt);
	}
	catch(SQLException ex){
		ex.printStackTrace();
		logger.error("com.pristine.dao.offermgmt.StoreFileExportGEDAO.getQuarterlyRecDataForStoreExportForRiteAid() - Error when getting recommendat data - " + ex.getMessage());
		throw new GeneralException("Error when getting recommendat data - " + ex.getMessage());
	}finally{
		PristineDBUtil.close(rs);
		PristineDBUtil.close(stmt);
	}
	return storeFileExportMap;
}
	
	/* Get recommendation details - specific to FleetFarm - Ends */
	
	public Map<Integer, List<StoreDTO>> getStoreZoneMapping() {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		StringBuilder sb = new StringBuilder();
		Map<Integer, List<StoreDTO>> storeZoneMapping = new HashMap<Integer, List<StoreDTO>>();
		sb.append("SELECT PRICE_ZONE_ID, COMP_STR_ID, COMP_STR_NO FROM COMPETITOR_STORE WHERE comp_chain_id = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y')");
		try {
			stmt = conn.prepareStatement(sb.toString());
			logger.info("Zone to Store mapping begins");
			rs = stmt.executeQuery();
			List<StoreDTO> storeList = new ArrayList<StoreDTO>();
			while (rs.next()) {
				StoreDTO storeDTO = new StoreDTO();
				storeDTO.setPriceZoneId(rs.getInt("PRICE_ZONE_ID"));
				storeDTO.setStrId(rs.getInt("COMP_STR_ID"));
				storeDTO.setStrNum(rs.getString("COMP_STR_NO"));
				storeList.add(storeDTO);
			}
			storeZoneMapping = storeList.stream().collect(Collectors.groupingBy(StoreDTO::getPriceZoneId));
		} catch (SQLException e) {
			logger.error("Error in fetching the comp store data");
		}finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);			
		}		
		return storeZoneMapping;
	}
	
	public Map<Integer, List<StoreDTO>> getGlobalStoreZoneMapping() {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT PRICE_ZONE_ID_3, COMP_STR_ID, COMP_STR_NO FROM COMPETITOR_STORE WHERE comp_chain_id = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y')");
		Map<Integer, List<StoreDTO>> storeZoneMapping = new HashMap<Integer, List<StoreDTO>>();
		try {
			stmt = conn.prepareStatement(sb.toString());
			logger.info("Global Zone to Store mapping begins");
			rs = stmt.executeQuery();		
			List<StoreDTO> storeList = new ArrayList<StoreDTO>();
			while (rs.next()) {
				StoreDTO storeDTO = new StoreDTO();
				storeDTO.setPriceZoneId3(rs.getInt("PRICE_ZONE_ID_3"));
				storeDTO.setStrId(rs.getInt("COMP_STR_ID"));
				storeDTO.setStrNum(rs.getString("COMP_STR_NO"));
				storeList.add(storeDTO);
			}
			storeZoneMapping = storeList.stream().collect(Collectors.groupingBy(StoreDTO::getPriceZoneId3));
		} catch (SQLException e) {
			logger.error("Error in fetching data from competitor store{}"+e.getMessage());
		}finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return storeZoneMapping;
	}
	
	public Map<Integer,List<RetailPriceDTO>> cacheExistingClearancePriceDataFromDB(List<Integer> itemCodes)  {
		logger.info("Caching clearance price data begins...");
		Map<Integer, List<RetailPriceDTO>> hmap = new HashMap<>();
		List<RetailPriceDTO> clearancePriceList = new ArrayList<RetailPriceDTO>();
		PreparedStatement stmt = null;
		ResultSet resultSet = null;
		StringBuilder itemCodeSb = new StringBuilder();
		List<Integer> itemCodeList = new ArrayList<Integer>();
		for(Integer oneItem:itemCodes) {
			itemCodeList .add(oneItem);
			itemCodeSb.append(oneItem);
			itemCodeSb.append(",");
			if(itemCodeList.size()==100) {
				StringBuffer sb = new StringBuffer();
				itemCodeSb.setLength(itemCodeSb.length() - 1);
				sb.append(
						"SELECT ITEM_CODE, PRICE_ID, LOCATION_ID, TO_CHAR(START_DATE, 'MM/dd/yyyy') AS START_DATE,TO_CHAR(END_DATE, 'MM/dd/yyyy') AS END_DATE FROM MD_ITEM_PRICE where PRICE_TYPE_ID='2'");
				sb.append(" AND ITEM_CODE IN (" + itemCodeSb + ")");
				try {
					stmt = conn.prepareStatement(sb.toString());
					resultSet = stmt.executeQuery();
					while (resultSet.next()) {
						int locationId = resultSet.getInt("LOCATION_ID");
						String startDate = resultSet.getString("START_DATE");
						String endDate = resultSet.getString("END_DATE");
						int itemCode = resultSet.getInt("ITEM_CODE");
						RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
						retailPriceDTO.setLocationId(locationId);
						retailPriceDTO.setSaleStartDate(startDate);
						retailPriceDTO.setSaleEndDate(endDate);
						retailPriceDTO.setItemcode(String.valueOf(itemCode));
						clearancePriceList.add(retailPriceDTO);
					}
					itemCodeList = new ArrayList<Integer>();
					itemCodeSb = new StringBuilder();
				} catch (SQLException e) {
					logger.error("Error while fetching price data");
				}finally {
					PristineDBUtil.close(resultSet);
					PristineDBUtil.close(stmt);
				}
				
			}
		}
		
		if(itemCodeList.size()>0) {
			itemCodeSb.setLength(itemCodeSb.length() - 1);
			StringBuffer sb = new StringBuffer();
			sb.append(
					"SELECT ITEM_CODE, PRICE_ID, LOCATION_ID, TO_CHAR(START_DATE, 'MM/dd/yyyy') AS START_DATE,TO_CHAR(END_DATE, 'MM/dd/yyyy') AS END_DATE FROM MD_ITEM_PRICE where PRICE_TYPE_ID='2'");
			sb.append(" AND ITEM_CODE IN (" + itemCodeSb + ")");
			try {
				stmt = conn.prepareStatement(sb.toString());
				resultSet = stmt.executeQuery();
				while (resultSet.next()) {
					int locationId = resultSet.getInt("LOCATION_ID");
					String startDate = resultSet.getString("START_DATE");
					String endDate = resultSet.getString("END_DATE");
					int itemCode = resultSet.getInt("ITEM_CODE");
					RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
					retailPriceDTO.setLocationId(locationId);
					retailPriceDTO.setSaleStartDate(startDate);
					retailPriceDTO.setSaleEndDate(endDate);
					retailPriceDTO.setItemcode(String.valueOf(itemCode));
					clearancePriceList.add(retailPriceDTO);
				}
			} catch (SQLException e) {
				logger.error("Error while fetching the data");
			}finally {
				PristineDBUtil.close(resultSet);
				PristineDBUtil.close(stmt);
			}
		}
		
		hmap = clearancePriceList.stream().collect(Collectors.groupingBy(RetailPriceDTO::getLocationId));
		logger.info("#No of stores in clearance... " + hmap.size());

		return hmap;
	}
}
