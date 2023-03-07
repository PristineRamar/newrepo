package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.StoreFileExportDTO;
import com.pristine.dto.offermgmt.UserDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;


public class StoreFileExportDAO {
	static Logger logger = Logger.getLogger("StoreFileExportDAO");
	private Connection conn = null;
	
	public StoreFileExportDAO(Connection conn){
		this.conn = conn;
	}
	
    // insert into notification_header
	private static final String INSERT_INTO_NOTIFICATION_HEADER = "INSERT INTO NOTIFICATION_HEADER "
			   + "(NOTIFICATION_ID, MODULE_ID, NOTIFICATION_TYPE, NOTIFICATION_KEY1) VALUES (?,?,?,?)";

	// insert notification_detail
	private static final String INSERT_INTO_NOTIFICATION_DETAILS = "INSERT INTO NOTIFICATION_DETAIL"
			   + "(NOTIFICATION_DETAIL_ID, NOTIFICATION_ID, USER_ID) VALUES (NOTIFICATION_DETAIL_SEQUENCE.NEXTVAL,?,?)";

	private static final String GET_NOTIFICATION_ID_SEQUENCE = "SELECT NOTIFICATION_ID_SEQUENCE.NEXTVAL AS NOTIFICATION_ID FROM DUAL";
	
	// Get values from GET_PR_RECOMMENDATION_RUN_HEADER
	private static final String GET_PR_RECOMMENDATION_RUN_HEADER = "SELECT RUN_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID,"
				+ "PRODUCT_ID FROM PR_RECOMMENDATION_RUN_HEADER WHERE RUN_ID = ?";
	
	private StringBuilder getStoreFileExportData(String stDate, String endDate) throws GeneralException {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT LPAD(S.COMP_STR_NO, 6, 0) AS COMP_STR_NO ,(I.RETAILER_ITEM_CODE || '_00000') AS RETAILER_ITEM_CODE, ");
		sb.append("CASE WHEN R.OVERRIDE_REG_MULTIPLE > 0 THEN R.OVERRIDE_REG_MULTIPLE  ELSE R.RECOMMENDED_REG_MULTIPLE END AS QTY, ");
		sb.append("CASE WHEN R.OVERRIDE_REG_PRICE > 0 THEN R.OVERRIDE_REG_PRICE ELSE R.RECOMMENDED_REG_PRICE END AS PRICE, ");
		sb.append("TO_CHAR(R.REG_EFF_DATE, 'mm/dd/yyyy') AS REG_EFF_DATE ");
		sb.append("FROM PR_RECOMMENDATION R JOIN PR_RECOMMENDATION_RUN_HEADER RH ON RH.RUN_ID = R.RUN_ID ");
		sb.append("JOIN PR_PRODUCT_LOCATION_MAPPING PLM ON PLM.LOCATION_LEVEL_ID = RH.LOCATION_LEVEL_ID AND PLM.LOCATION_ID = RH.LOCATION_ID AND PLM.PRODUCT_LEVEL_ID = RH.PRODUCT_LEVEL_ID AND PLM.PRODUCT_ID = RH.PRODUCT_ID ");
		sb.append("JOIN PR_PRODUCT_LOCATION_STORE PLS ON PLS.PRODUCT_LOCATION_MAPPING_ID = PLM.PRODUCT_LOCATION_MAPPING_ID ");
		sb.append("JOIN STORE_ITEM_MAP SIM ON SIM.LEVEL_TYPE_ID  = 2 AND SIM.LEVEL_ID = PLS.STORE_ID AND SIM.ITEM_CODE = R.LIR_ID_OR_ITEM_CODE AND SIM.IS_AUTHORIZED ='Y' ");
		sb.append("JOIN COMPETITOR_STORE S ON S.COMP_STR_ID = PLS.STORE_ID ");
		sb.append("LEFT OUTER JOIN PR_RECOMMENDATION_STORE C ON R.PR_RECOMMENDATION_ID = C.PR_RECOMMENDATION_ID AND C.STORE_ID = PLS.STORE_ID ");
		sb.append("JOIN ITEM_LOOKUP I ON I.ITEM_CODE = R.LIR_ID_OR_ITEM_CODE ");
		sb.append("WHERE R.IS_HOLD != 'Y' AND RH.STATUS = " + PRConstants.STATUS_APPROVED + " AND R.LIR_IND = 'N' AND (R.IS_PRICE_RECOMMENDED = 1 OR R.OVERRIDE_REG_PRICE > 0) AND ");
		sb.append("RH.CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE >= '");
		sb.append(stDate);
		sb.append("' AND START_DATE <= '");
		sb.append(endDate);
		sb.append("' AND ROW_TYPE = 'W')");
		return sb;
	}
	
	private StringBuilder getRunIDs(int locationId, int productId, int productLevelId, String stDate, String endDate) throws GeneralException {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT DISTINCT RH.RUN_ID ");
		sb.append("FROM PR_RECOMMENDATION R JOIN PR_RECOMMENDATION_RUN_HEADER RH ON RH.RUN_ID = R.RUN_ID ");
		sb.append("JOIN PR_PRODUCT_LOCATION_MAPPING PLM ON PLM.LOCATION_LEVEL_ID = RH.LOCATION_LEVEL_ID AND PLM.LOCATION_ID = RH.LOCATION_ID AND PLM.PRODUCT_LEVEL_ID = RH.PRODUCT_LEVEL_ID AND PLM.PRODUCT_ID = RH.PRODUCT_ID ");
		sb.append("JOIN PR_PRODUCT_LOCATION_STORE PLS ON PLS.PRODUCT_LOCATION_MAPPING_ID = PLM.PRODUCT_LOCATION_MAPPING_ID ");
		sb.append("JOIN STORE_ITEM_MAP SIM ON SIM.LEVEL_TYPE_ID  = 2 AND SIM.LEVEL_ID = PLS.STORE_ID AND SIM.ITEM_CODE = R.LIR_ID_OR_ITEM_CODE AND SIM.IS_AUTHORIZED ='Y' ");
		sb.append("JOIN COMPETITOR_STORE S ON S.COMP_STR_ID = PLS.STORE_ID ");
		sb.append("LEFT OUTER JOIN PR_RECOMMENDATION_STORE C ON R.PR_RECOMMENDATION_ID = C.PR_RECOMMENDATION_ID AND C.STORE_ID = PLS.STORE_ID ");
		sb.append("JOIN ITEM_LOOKUP I ON I.ITEM_CODE = R.LIR_ID_OR_ITEM_CODE ");
		sb.append("WHERE R.IS_HOLD != 'Y' AND RH.STATUS = " + PRConstants.STATUS_APPROVED + " AND R.LIR_IND = 'N' AND (R.IS_PRICE_RECOMMENDED = 1 OR R.OVERRIDE_REG_PRICE > 0) AND ");
		if(productId != -1){
			sb.append("RH.PRODUCT_ID = '" + productId + "' AND ");
		}
		if(locationId != -1){
			sb.append("RH.LOCATION_ID = '" + locationId + "' AND ");
			//sb.append("RH.LOCATION_ID IN ('6','10','15') AND ");
		}
		sb.append("RH.CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE >= '");
		sb.append(stDate);
		sb.append("' AND START_DATE <= '");
		sb.append(endDate);
		sb.append("' AND ROW_TYPE = 'W')");
		return sb;
	}
	
	private static final String GET_USER_ID = "SELECT DISTINCT USER_ID FROM USER_ROLE_MAP WHERE ROLE_ID IN(?,?,?) AND USER_ID IN"
			+ "(SELECT USER_ID FROM USER_TASK WHERE (VALUE LIKE ? AND VALUE_TYPE = 'LOCATION_LIST') "
			+ "OR (VALUE LIKE ? AND " + "VALUE_TYPE = 'PRODUCT_LIST'))";
	
	private StringBuilder getRunIDbyProductId(int productId) throws GeneralException {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT MAX(RH.RUN_ID) AS RUN_ID ");
		sb.append("FROM PR_RECOMMENDATION R JOIN PR_RECOMMENDATION_RUN_HEADER RH ON RH.RUN_ID = R.RUN_ID ");
		sb.append("JOIN PR_PRODUCT_LOCATION_MAPPING PLM ON PLM.LOCATION_LEVEL_ID = RH.LOCATION_LEVEL_ID AND PLM.LOCATION_ID = RH.LOCATION_ID AND PLM.PRODUCT_LEVEL_ID = RH.PRODUCT_LEVEL_ID AND PLM.PRODUCT_ID = RH.PRODUCT_ID ");
		sb.append("JOIN PR_PRODUCT_LOCATION_STORE PLS ON PLS.PRODUCT_LOCATION_MAPPING_ID = PLM.PRODUCT_LOCATION_MAPPING_ID ");
		sb.append("JOIN STORE_ITEM_MAP SIM ON SIM.LEVEL_TYPE_ID  = 2 AND SIM.LEVEL_ID = PLS.STORE_ID AND SIM.ITEM_CODE = R.LIR_ID_OR_ITEM_CODE AND SIM.IS_AUTHORIZED ='Y' ");
		sb.append("JOIN COMPETITOR_STORE S ON S.COMP_STR_ID = PLS.STORE_ID ");
		sb.append("LEFT OUTER JOIN PR_RECOMMENDATION_STORE C ON R.PR_RECOMMENDATION_ID = C.PR_RECOMMENDATION_ID AND C.STORE_ID = PLS.STORE_ID ");
		sb.append("JOIN ITEM_LOOKUP I ON I.ITEM_CODE = R.LIR_ID_OR_ITEM_CODE ");
		sb.append("WHERE R.IS_HOLD != 'Y' AND RH.STATUS = " + PRConstants.STATUS_APPROVED + " AND R.LIR_IND = 'N' AND (R.IS_PRICE_RECOMMENDED = 1 OR R.OVERRIDE_REG_PRICE > 0) AND RH.PRODUCT_ID = ");
		sb.append(productId);
		sb.append(" ORDER BY RUN_ID DESC");
		return sb;
	}
	
	private StringBuilder getProdIDbyRunId() throws GeneralException {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT DISTINCT PRODUCT_ID FROM PR_RECOMMENDATION_RUN_HEADER WHERE RUN_ID IN ( ");
		sb.append("SELECT DISTINCT  RH.RUN_ID ");
		sb.append("FROM PR_RECOMMENDATION R JOIN PR_RECOMMENDATION_RUN_HEADER RH ON RH.RUN_ID = R.RUN_ID ");
		sb.append("JOIN PR_PRODUCT_LOCATION_MAPPING PLM ON PLM.LOCATION_LEVEL_ID = RH.LOCATION_LEVEL_ID AND PLM.LOCATION_ID = RH.LOCATION_ID AND PLM.PRODUCT_LEVEL_ID = RH.PRODUCT_LEVEL_ID AND PLM.PRODUCT_ID = RH.PRODUCT_ID ");
		sb.append("JOIN PR_PRODUCT_LOCATION_STORE PLS ON PLS.PRODUCT_LOCATION_MAPPING_ID = PLM.PRODUCT_LOCATION_MAPPING_ID ");
		sb.append("JOIN STORE_ITEM_MAP SIM ON SIM.LEVEL_TYPE_ID  = 2 AND SIM.LEVEL_ID = PLS.STORE_ID AND SIM.ITEM_CODE = R.LIR_ID_OR_ITEM_CODE AND SIM.IS_AUTHORIZED ='Y' ");
		sb.append("JOIN COMPETITOR_STORE S ON S.COMP_STR_ID = PLS.STORE_ID ");
		sb.append("LEFT OUTER JOIN PR_RECOMMENDATION_STORE C ON R.PR_RECOMMENDATION_ID = C.PR_RECOMMENDATION_ID AND C.STORE_ID = PLS.STORE_ID ");
		sb.append("JOIN ITEM_LOOKUP I ON I.ITEM_CODE = R.LIR_ID_OR_ITEM_CODE ");
		sb.append("WHERE R.IS_HOLD != 'Y' AND RH.STATUS = " + PRConstants.STATUS_APPROVED + " AND R.LIR_IND = 'N' AND (R.IS_PRICE_RECOMMENDED = 1 OR R.OVERRIDE_REG_PRICE > 0)) ");
		return sb;
	}
	
	public ArrayList<Integer> getRunID(int locationId, int productId, int productLevelId, String stDate, String endDate) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String query;
		StringBuilder sb = new StringBuilder();
		ArrayList<Integer> runID = new ArrayList<Integer>();
		try {
			sb = getRunIDs(locationId,productId,productLevelId,stDate,endDate);
			query = sb.toString();
			stmt = conn.prepareStatement(query);
			rs = stmt.executeQuery();
			while(rs.next()){
				runID.add(rs.getInt("RUN_ID"));
			}
			
		}catch (SQLException exception) {
			logger.error("Error in getRunID() - " + exception.toString());
			throw new GeneralException(exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
		return runID;
	}
	
	
	public StringBuilder updateRecStatusAsExported(int locationId, int productId, int productLevelId, String stDate, String endDate) throws GeneralException {
		StringBuilder sb = new StringBuilder();
		PreparedStatement stmt = null;
		String query;
		ArrayList<Integer> runID = new ArrayList<Integer>();
		runID = getRunID(locationId,productId,productLevelId,stDate,endDate);
		
		StringBuilder sbRunIDs = new StringBuilder();
		for (Integer number : runID) {
			//sbRunIDs.append("'");
			sbRunIDs.append(number != null ? number.toString() : "");
			//sbRunIDs.append("',");
			sbRunIDs.append(",");
		}
		if(runID.size() != 0)
		{
			sbRunIDs.setLength(sbRunIDs.length() - 1);
		 
		
		
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm");
		Date date = new Date();
		String currentDate = dateFormat.format(date);
		
		try {
		sb.append("UPDATE PR_RECOMMENDATION_RUN_HEADER SET STATUS = " + PRConstants.STATUS_EXPORTED );
		sb.append(" ,EXPORTED_BY = 'prestolive', EXPORTED = TO_DATE('" + currentDate + "', 'MM/dd/yyyy HH24:MI') ");
		sb.append(" WHERE STATUS = " + PRConstants.STATUS_APPROVED );
		sb.append(" AND RUN_ID IN (" + sbRunIDs + ")");
		if(locationId != -1)
		{
			sb.append(" AND LOCATION_ID = " + locationId);
		}
		if(productLevelId != -1 && productId != -1)
		{
		  if(locationId != -1)
			{
			  sb.append(" AND ");
			}
		  else
		  {
			  sb.append(" AND ");
		  }
			  sb.append(" PRODUCT_ID = " + productId);;
			  sb.append(" AND PRODUCT_LEVEL_ID = " + productLevelId);
		}
		query = sb.toString();
		stmt = conn.prepareStatement(query);
		stmt.executeUpdate();
		} catch (SQLException exception) {
			logger.error("Error in updateRecStatusAsExported() - " + exception.toString());
			throw new GeneralException(exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
			PristineDBUtil.commitTransaction(conn, "Committed Successfully");
		}
	}
		
		return sb;
	}
	 
	private StringBuilder getStoreFileExportDataWithLoc(int locationId) throws GeneralException {
		StringBuilder sb = new StringBuilder();
		sb.append(" AND RH.LOCATION_LEVEL_ID = '" + PRConstants.ZONE_LEVEL_TYPE_ID + "' AND RH.LOCATION_ID = " + locationId);
		return sb;
	}
	
	private StringBuilder getStoreFileExportDataWithProd(int productId, int productLevelId) throws GeneralException {
		StringBuilder sb = new StringBuilder();
		sb.append(" AND RH.PRODUCT_ID = " + productId);;
		sb.append(" AND RH.PRODUCT_LEVEL_ID = " + productLevelId);
		return sb;
	}
	
	public ArrayList<UserDTO> getUserIDbyProductID(ArrayList<Integer> productId, ArrayList<Integer> locationId) throws GeneralException{
		PreparedStatement stmt = null;
		PreparedStatement stmtRunId = null;
		ArrayList<UserDTO> userResult = new ArrayList<UserDTO>();
		PRRecommendationRunHeader prrecommendationRunHeader = null;
		try {
			for (int i = 0; i < productId.size(); i++)
			{
			
				//Get RunID by ProductID
				String queryRunId;
				int runID = 0;
				StringBuilder sbRunId = new StringBuilder();
				ResultSet rsRunID = null;
				sbRunId = getRunIDbyProductId(productId.get(i));
				queryRunId = sbRunId.toString();
				stmtRunId = conn.prepareStatement(queryRunId);
				rsRunID = stmtRunId.executeQuery();
				while(rsRunID.next()){
					runID = (rsRunID.getInt("RUN_ID"));
				}
				
				if(runID != 0)
				{
				prrecommendationRunHeader = getNotificationDetails(runID, productId, locationId);
				ArrayList<String> userList = getUserIDList(prrecommendationRunHeader);
				long notificationId = getSequence();
				insertNotificationHeader(notificationId, PRConstants.STATUS_EXPORTED, runID);
				insertNotificationDetails(notificationId, userList);
				}
				 
		    }
		}catch (SQLException exception) {
			logger.error("Error in getUserIDbyRunId() - " + exception.toString());
			throw new GeneralException(exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
		return userResult;
	}
	
	public PRRecommendationRunHeader getNotificationDetails(long runId, ArrayList<Integer> productId, ArrayList<Integer> locationId) throws GeneralException {
		PRRecommendationRunHeader prrecommendationRunHeader = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		 
		try {
			if(productId.size() != 0 && locationId.size() != 0)
			{
				prrecommendationRunHeader = new PRRecommendationRunHeader();
				prrecommendationRunHeader.setRunId(runId);
				prrecommendationRunHeader.setLocationLevelId(6);
				prrecommendationRunHeader.setLocationId(locationId.get(0));
				prrecommendationRunHeader.setProductLevelId(4);
				prrecommendationRunHeader.setProductId(productId.get(0));
			}
			else
			{
			String query = new String(GET_PR_RECOMMENDATION_RUN_HEADER);
			statement = conn.prepareStatement(query);
			 
			int counter = 0;
			statement.setLong(++counter, runId);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				prrecommendationRunHeader = new PRRecommendationRunHeader();
				prrecommendationRunHeader.setRunId(resultSet.getLong("RUN_ID"));
				prrecommendationRunHeader.setLocationLevelId(resultSet.getInt("LOCATION_LEVEL_ID"));
				prrecommendationRunHeader.setLocationId(resultSet.getInt("LOCATION_ID"));
				prrecommendationRunHeader.setProductLevelId(resultSet.getInt("PRODUCT_LEVEL_ID"));
				prrecommendationRunHeader.setProductId(resultSet.getInt("PRODUCT_ID"));

			}
		  }
		} catch (SQLException e) {
			logger.error("Error while executing Get Notification Details " + e.toString(), e);
			throw new GeneralException("Error while executing Get Notification Details " + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return prrecommendationRunHeader;

	}
	
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
	
	public long getSequence() throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		long sequence = 0;

		try {
			String query = new String(GET_NOTIFICATION_ID_SEQUENCE);
			statement = conn.prepareStatement(query);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				sequence = (resultSet.getLong("NOTIFICATION_ID"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_NOTIFICATION_ID_SEQUENCE " + e.toString(), e);
			throw new GeneralException("Error while executing GET_NOTIFICATION_ID_SEQUENCE " + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return sequence;

	}
	
	public void insertNotificationHeader(long notificationId, int notificationType, long runId)
			throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_INTO_NOTIFICATION_HEADER);
     		int counter = 0;
			statement.setLong(++counter, notificationId);
			statement.setInt(++counter, PRConstants.REGULAR_PRICE_RECOMMENDATION);
			statement.setLong(++counter, notificationType);
			statement.setLong(++counter, runId);
			statement.executeUpdate();

		} catch (SQLException e) {
			logger.error("Error in insertNotificationHeader()" + e.toString(), e);
			throw new GeneralException("Error in insertNotificationHeader() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	public void insertNotificationDetails(long notificationId,
			ArrayList<String> prRecommendationUserID) throws GeneralException {
		PreparedStatement statement = null;

		try {
			statement = conn.prepareStatement(INSERT_INTO_NOTIFICATION_DETAILS);
			for (String userId : prRecommendationUserID) {
				int counter = 0;
				statement.setLong(++counter, notificationId);
				statement.setString(++counter,userId);
				statement.addBatch();
			}

			statement.executeBatch();
		} catch (SQLException e) {
			logger.error("Error in insertNotificationDetails()" + e.toString(), e);
			throw new GeneralException("Error in insertNotificationDetails() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
		
	public ArrayList<Integer> getProductIDbyRunId() throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String query;
		StringBuilder sb = new StringBuilder();
		ArrayList<Integer> prodID = new ArrayList<Integer>();
		try {
			sb = getProdIDbyRunId();
			query = sb.toString();
			stmt = conn.prepareStatement(query);
			rs = stmt.executeQuery();
			while(rs.next()){
				 prodID.add(rs.getInt("PRODUCT_ID"));
			}
			
		}catch (SQLException exception) {
			logger.error("Error in getUserIDbyRunId() - " + exception.toString());
			throw new GeneralException(exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
		return prodID;
	}
	
	public ArrayList<StoreFileExportDTO> getStoreExportData(int locationId, int productLevelId, int productId, String stDate, String endDate) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String query;
		StringBuilder sb = new StringBuilder();
		ArrayList<StoreFileExportDTO> storeFileExportMap = new ArrayList<StoreFileExportDTO>();
		try{
			
			sb = getStoreFileExportData(stDate, endDate);
			if(locationId != -1)
			{
				sb.append(getStoreFileExportDataWithLoc(locationId));
			}
			if(productLevelId != -1 && productId != -1)
			{
				sb.append(getStoreFileExportDataWithProd(productId,productLevelId));
			}
			query = sb.toString();
			stmt = conn.prepareStatement(query);
			rs = stmt.executeQuery();
			while(rs.next()){
				StoreFileExportDTO dto = new StoreFileExportDTO();
				dto.setCompStrNo(rs.getString("COMP_STR_NO"));
				dto.setRetItemCode(rs.getString("RETAILER_ITEM_CODE"));
				dto.setQuantity(rs.getInt("QTY"));
				dto.setPrice(rs.getDouble("PRICE"));
				if(rs.getString("REG_EFF_DATE") == null)
				{
					dto.setRegEffectiveDate("");
				}
				else
				{
					dto.setRegEffectiveDate(rs.getString("REG_EFF_DATE"));
				}
				storeFileExportMap.add(dto);
			}
		}
		catch(SQLException ex){
			logger.error("Error when retrieving store file data - " + ex.getMessage());
			throw new GeneralException("Error when retrieving store file data - " + ex.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return storeFileExportMap;
	}
}
