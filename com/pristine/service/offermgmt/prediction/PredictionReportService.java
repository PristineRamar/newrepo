package com.pristine.service.offermgmt.prediction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.oos.OOSAnalysisDAO;
import com.pristine.dao.offermgmt.prediction.PredictionReportDAO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionReportItemUIDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PredictionReportService {
	private static Logger logger = Logger.getLogger("PredictionReport");
	private Connection conn = null;
	private Context initContext;
	private Context envContext;
	private DataSource ds;
	
	public List<PredictionReportItemUIDTO> generatePredictionReportForUI(int locationLevelId, int locationId, int productLevelId,
			int productId, int weekCalendarId) {
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		List<PredictionReportItemUIDTO> predictionReportDTO = new ArrayList<PredictionReportItemUIDTO>();
		List<OOSItemDTO> oosCandidateItems = null;
		PredictionReportDAO predictionReportDAO = null;
		if (conn == null) {
			initializeForWS();
			conn = getConnection();
		}
		
		try {
			// TODO:: Change to work for other clients also
			if (locationLevelId == Constants.CHAIN_LEVEL_ID) {
				int topsStoreListId = Integer.parseInt(PropertyManager.getProperty("TOPS_WITHOUT_GU_STORE_LIST_ID"));
				int guStoreListId = Integer.parseInt(PropertyManager.getProperty("TOPS_GU_STORE_LIST_ID"));
				String locationIds = topsStoreListId + ", " + guStoreListId;
				oosCandidateItems = oosAnalysisDAO.getOOSCandidateItemsForChainForecast(conn, Constants.STORE_LIST_LEVEL_ID, locationIds,
						weekCalendarId, topsStoreListId, guStoreListId);
			} else if (locationLevelId == Constants.STORE_LIST_LEVEL_ID) {
				oosCandidateItems = oosAnalysisDAO.getOOSCandidateItemsForChainForecast(conn, weekCalendarId, locationLevelId, locationId,
						productLevelId, productId);
			} else {
				predictionReportDAO = new PredictionReportDAO();
				oosCandidateItems = predictionReportDAO.getPredictionReportItemForZoneForecast(conn, locationLevelId, locationId, productLevelId,
						productId, weekCalendarId);
			}

			predictionReportDTO = convertToPredictionReportDTO(oosCandidateItems);
		} catch (GeneralException ex) {
			ex.printStackTrace();
			logger.error("Error in generatePredictionReportForUI()" + ex.toString(), ex);
		} finally {
			PristineDBUtil.close(conn);
		}
		return predictionReportDTO;
	}
	
	//For debugging alone
	public List<PredictionReportItemUIDTO> generatePredictionReportForUI(Connection conn, int locationLevelId, int locationId, int productLevelId,
			int productId, int weekCalendarId) {
		this.conn = conn;
		return generatePredictionReportForUI(locationLevelId, locationId, productLevelId, productId, weekCalendarId);
	}

	protected Connection getConnection() {
		return conn;
	}

	private List<PredictionReportItemUIDTO> convertToPredictionReportDTO(List<OOSItemDTO> oosItems) {
		List<PredictionReportItemUIDTO> predictionReportDTO = new ArrayList<PredictionReportItemUIDTO>();
		// Convert to UI format
		for (OOSItemDTO oosItemDTO : oosItems) {
			PredictionReportItemUIDTO predictionReportUI = new PredictionReportItemUIDTO();
			predictionReportUI.setProductId(oosItemDTO.getProductId());
			predictionReportUI.setProductLevelId(oosItemDTO.getProductLevelId());
			predictionReportUI.setLocationLevelId(oosItemDTO.getLocationLevelId());
			predictionReportUI.setLocationId(oosItemDTO.getLocationId());
			predictionReportUI.setRegPrice(oosItemDTO.getRegPrice());
			predictionReportUI.setSalePrice(oosItemDTO.getSalePrice());
			predictionReportUI.setCategoryName(oosItemDTO.getCategoryName());
			predictionReportUI.setRetailerItemCode(oosItemDTO.getRetailerItemCode());
			predictionReportUI.setItemName(oosItemDTO.getItemName());
			predictionReportUI.setPromoTypeId(oosItemDTO.getPromoTypeId());
			predictionReportUI.setAdPageNo(oosItemDTO.getAdPageNo());
			predictionReportUI.setBlockNo(oosItemDTO.getBlockNo());
			predictionReportUI.setDisplayTypeId(oosItemDTO.getDisplayTypeId());
			predictionReportUI.setRetLirId(oosItemDTO.getRetLirId());
			predictionReportUI.setLirName(oosItemDTO.getLirName());
			predictionReportUI.setWeeklyPredictedMovement(oosItemDTO.getWeeklyPredictedMovement());
			predictionReportUI.setClientWeeklyPredictedMovement(oosItemDTO.getClientWeeklyPredictedMovement());
			predictionReportUI.setWeeklyActualMovement(oosItemDTO.getWeeklyActualMovement());
			predictionReportUI.setNoOfLigorNonLigInABlock(oosItemDTO.getNoOfLigOrNonLig());
			predictionReportDTO.add(predictionReportUI);
		}
		return predictionReportDTO;
	}
	
	protected void initializeForWS() {
		setConnection(getDSConnection());		
	}
	
	/**
	 * Returns Connection from datasource
	 * @return
	 */
	private Connection getDSConnection() {
		Connection connection = null;
		logger.info("WS Connection - " + PropertyManager.getProperty("WS_CONNECTION"));;
		try{
			if(ds == null){
				initContext = new InitialContext();
				envContext  = (Context)initContext.lookup("java:/comp/env");
				ds = (DataSource)envContext.lookup(PropertyManager.getProperty("WS_CONNECTION"));
			}
			connection = ds.getConnection();
		}catch(NamingException exception){
			logger.error("Error when creating connection from datasource " + exception.toString());
		}catch(SQLException exception){
			logger.error("Error when creating connection from datasource " + exception.toString());
		}
		return connection;
	}
	
	/**
	 * Sets database connection. Used when program runs in batch mode
	 */
	protected void setConnection() {
		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}
}
