package com.pristine.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.PriceExportDAO;
import com.pristine.dao.offermgmt.StoreFileExportGEDAO;
import com.pristine.dataload.offermgmt.StorePriceExport;
import com.pristine.dto.offermgmt.NotificationDetailInputDTO;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.AuditTrailStatusLookup;
import com.pristine.lookup.AuditTrailTypeLookup;
import com.pristine.service.offermgmt.NotificationService;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.Constants;
//util class for store file export
public class StoreFileExportUtil {
	private static Logger logger = Logger.getLogger("StoreFileExportUtil");

	public static void addNotification(Connection conn, List<Long> runIdList) throws SQLException, GeneralException {
		HashMap<Long, Integer> runIdAndStatusCode = new PriceExportDAO().getRunIdWithStatusCode(conn, runIdList);
		int statusCode = 0;

		NotificationService notificationService = new NotificationService();
		for (Map.Entry<Long, Integer> runIdAndStatusEntry : runIdAndStatusCode.entrySet()) {
			List<NotificationDetailInputDTO> notificationDetailDTOs = new ArrayList<NotificationDetailInputDTO>();
			long runId = runIdAndStatusEntry.getKey();
			logger.debug("Export Status code: 7 - Partially exported, 8 - Exported, 10 - Emergency Approved");
			statusCode = runIdAndStatusEntry.getValue();
			logger.debug("addNotification() - Exported Status code : " + statusCode);
			NotificationDetailInputDTO notificationDetailInputDTO = new NotificationDetailInputDTO();
			notificationDetailInputDTO.setModuleId(PRConstants.REC_MODULE_ID);
			notificationDetailInputDTO.setNotificationTypeId(statusCode);
			notificationDetailInputDTO.setNotificationKey1(runId);
			notificationDetailDTOs.add(notificationDetailInputDTO);
			// logger.info("notificationDetailDTOs size: " + notificationDetailDTOs.size());
			notificationService.addNotificationsBatch(conn, notificationDetailDTOs, true);
		}
	}
	
	public static void updateStatus(List<PRRecommendationRunHeader> recHeaderList, StoreFileExportGEDAO storeFileExportDAO,Connection conn) throws GeneralException {

		try {

			// Update the recommendation header status
			logger.debug("Update expored status to recommendation header");
			storeFileExportDAO.updateQuarterlyRecStatus(conn, recHeaderList);
			storeFileExportDAO.deleteExportQueue(conn, recHeaderList);
			storeFileExportDAO.insertExportStatus(conn, recHeaderList);
			List<Long> runIdList = recHeaderList.stream().map(PRRecommendationRunHeader::getRunId)
					.collect(Collectors.toList());
			addNotification(conn, runIdList);

			PristineDBUtil.commitTransaction(conn, "Commit Changes");
		} catch (Exception ex) {
			logger.error("Outer Exception - JavaException", ex);
			PristineDBUtil.rollbackTransaction(conn, "Rollback Changes");
			throw new GeneralException("Outer Exception - JavaException - " + ex.toString());
		}
	}
	
	public static void callAuditTrail(List<PRRecommendationRunHeader> recHeaderList) {
		Long distinctItemIds = recHeaderList.stream().map(PRRecommendationRunHeader::getProductId).distinct().count();
		Long distinctLocationIds = recHeaderList.stream().map(PRRecommendationRunHeader::getProductId).distinct().count();
		StorePriceExport storePriceExport = new StorePriceExport();
		recHeaderList.forEach(oneHeader->{
		try {
			storePriceExport.callAuditTrail(oneHeader.getLocationLevelId(), oneHeader.getLocationId(), oneHeader.getProductLevelId(),
						oneHeader.getProductId(), AuditTrailTypeLookup.RECOMMENDATION.getAuditTrailTypeId(), Constants.EXPORTED_STATUS_CODE,
						oneHeader.getRunId(), PRConstants.BATCH_USER,
						AuditTrailStatusLookup.AUDIT_TYPE.getAuditTrailTypeId(),
						AuditTrailStatusLookup.AUDIT_SUB_TYPE_EXPORT.getAuditTrailTypeId(),
						AuditTrailStatusLookup.SUB_STATUS_TYPE_EXPORT.getAuditTrailTypeId(),distinctItemIds.intValue(),distinctLocationIds.intValue(),0);
		} catch (GeneralException e) {
			logger.error("Error in updating Audit trail....");
		}
		});	
	}
}
