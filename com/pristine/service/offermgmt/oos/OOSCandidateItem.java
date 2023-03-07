package com.pristine.service.offermgmt.oos;

import java.sql.Connection;
import java.util.List;

import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.exception.GeneralException;

public interface OOSCandidateItem {
	public List<PRItemDTO> getAuthorizedItemOfWeeklyAd(Connection conn, int productLevelId, int productId,
			int locationLevelId, int locationId, int weekCalendarId, int chainId) throws GeneralException;
	
	public List<PRItemDTO> clearDuplicates(List<PRItemDTO> itemList);
	
	public List<OOSItemDTO> getOOSCandidateItems(Connection conn, int locationLevelId, int locationId,
			int calendarId) throws GeneralException;
}
