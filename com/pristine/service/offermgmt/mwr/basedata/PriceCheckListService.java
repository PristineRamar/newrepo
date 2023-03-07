package com.pristine.service.offermgmt.mwr.basedata;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;

public class PriceCheckListService {

	/**
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param isPriceTestZone 
	 * @param reccStartWeekDate 
	 * @param reccWeekStartDate 
	 * @return Item/LIG level price check lists
	 * @throws GeneralException 
	 */
	public HashMap<ItemKey, List<PriceCheckListDTO>> getPriceCheckLists(Connection conn, int locationLevelId,
			int locationId, int productLevelId, int productId, boolean isPriceTestZone,String reccStartWeekDate) throws GeneralException {

		// Get all price check list of the zone
		HashMap<ItemKey, List<PriceCheckListDTO>> priceCheckListInfo = null;
		if (isPriceTestZone) {
			priceCheckListInfo = new PricingEngineDAO().getPriceCheckListInfoForPriceTest(conn, locationLevelId,
					locationId, productLevelId, productId);
		} else
			priceCheckListInfo = new PricingEngineDAO().getPriceCheckListInfo(conn, locationLevelId, locationId,
					productLevelId, productId,reccStartWeekDate);

		return priceCheckListInfo;

	}

}
