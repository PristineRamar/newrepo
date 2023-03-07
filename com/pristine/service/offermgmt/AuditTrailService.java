package com.pristine.service.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.AuditTrailDAO;
import com.pristine.dto.offermgmt.AuditTrailDetailDTO;
import com.pristine.exception.GeneralException;

public class AuditTrailService {

	static Logger logger = Logger.getLogger("AutoRecommondationApprova");
	private static Connection _conn = null;
	public void auditRecommendation(Connection conn, int locationLevelId, int locationId, int productLevelId,
			int productId, int auditTrailTypeId, int statusCode, long runId, String userId, int auditType,int auditSubsType,
			int auditSubTypeStatus, int countOfItems, int countOfStores, int exportTime) throws GeneralException {		
		
		AuditTrailDetailDTO auditTrailDetailDTO = new AuditTrailDetailDTO();
		AuditTrailDAO auditTrailDAO = new AuditTrailDAO();
		List<AuditTrailDetailDTO> auditTrailDetails = new ArrayList<AuditTrailDetailDTO>();

		auditTrailDetailDTO.setProductLevelId(productLevelId);
		auditTrailDetailDTO.setProductId(productId);
		auditTrailDetailDTO.setLocationLevelId(locationLevelId);
		auditTrailDetailDTO.setLocationId(locationId);
		auditTrailDetailDTO.setAuditKey1(runId);
		auditTrailDetailDTO.setAuditTrailType(auditTrailTypeId);
		auditTrailDetailDTO.setStatusType(statusCode);
		auditTrailDetailDTO.setUserId(userId);
		auditTrailDetailDTO.setAuditType(auditType);
		auditTrailDetailDTO.setAuditSubType(auditSubsType);
		auditTrailDetailDTO.setAuditSubStatus(auditSubTypeStatus);
		auditTrailDetailDTO.setExportItemCount(countOfItems);
		auditTrailDetailDTO.setExportStoreCount(countOfStores);
		auditTrailDetailDTO.setExportTime(exportTime);
		auditTrailDetails.add(auditTrailDetailDTO);
		auditTrailDAO.insertAuditTrailDetail(conn, auditTrailDetails);		
		
	}
	public void weeklyAuditTrail(Connection conn, int locationLevelId, int locationId, int productLevelId,
			int productId,  long runId, String userId1, int auditType,int auditSubsType,int auditSubTypeStatus,int calId) throws GeneralException {
		AuditTrailDetailDTO auditTrailDetailDTO = new AuditTrailDetailDTO();
		AuditTrailDAO auditTrailDAO = new AuditTrailDAO();
		List<AuditTrailDetailDTO> auditTrailDetails = new ArrayList<AuditTrailDetailDTO>();

		auditTrailDetailDTO.setProductLevelId(productLevelId);
		auditTrailDetailDTO.setProductId(productId);
		auditTrailDetailDTO.setLocationLevelId(locationLevelId);
		auditTrailDetailDTO.setLocationId(locationId);
		auditTrailDetailDTO.setAuditKey1(runId);
		auditTrailDetailDTO.setUserId(userId1);
		auditTrailDetailDTO.setAuditType(auditType);
		auditTrailDetailDTO.setAuditSubType(auditSubsType);
		auditTrailDetailDTO.setAuditSubStatus(auditSubTypeStatus);
		auditTrailDetailDTO.setAuditKey3(calId);
		auditTrailDetails.add(auditTrailDetailDTO);
		String details=" ProductLevelId: "+auditTrailDetailDTO.getProductLevelId()+" ProductId "+auditTrailDetailDTO.getProductId()+" locationLevelId: "+auditTrailDetailDTO.getLocationLevelId()+
				" locationId:  "+auditTrailDetailDTO.getLocationId()+ " auditKey1: "+auditTrailDetailDTO.getAuditKey1()+" userId: "+auditTrailDetailDTO.getUserId()+" AuditType: "+auditTrailDetailDTO.getAuditType()+
				"auditSubType: "+auditTrailDetailDTO.getAuditSubType()+" auditSubStatus:  "+auditTrailDetailDTO.getAuditSubStatus()+
				"CalId: "+auditTrailDetailDTO.getAuditKey3();
		logger.info("weeklyAuditTrail() Set parameters for weeklyAuditTrail->  "+details);
		auditTrailDAO.insertAuditTrailWeeklyDeatils(conn, auditTrailDetails);		
		
	}
}
