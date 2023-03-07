package com.pristine.service.offermgmt.mwr.basedata;

import java.sql.Connection;
import java.util.List;

import com.pristine.dataload.offermgmt.mwr.CommonDataHelper;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ItemService;

public class AuthorizedItemService {

	
	/**
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param priceZoneStores
	 * @return List of authorized items for given product and location
	 * @throws GeneralException
	 * @throws OfferManagementException
	 */
	public List<PRItemDTO> getAuthorizedItems(Connection conn, RecommendationInputDTO recommendationInputDTO,
			List<Integer> priceZoneStores) throws GeneralException, OfferManagementException {

		PRStrategyDTO inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);

		// Fetch all authorized items for given stores
		List<PRItemDTO> authorizedItems = new ItemService(null).getAuthorizedItemsOfZoneAndStore(conn, inputDTO,
				priceZoneStores);

		return authorizedItems;
	}
	
	
}
