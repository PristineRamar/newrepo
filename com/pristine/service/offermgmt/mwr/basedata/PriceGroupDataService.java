package com.pristine.service.offermgmt.mwr.basedata;

import java.sql.Connection;
import java.util.HashMap;

import com.pristine.dao.offermgmt.PriceGroupRelationDAO;
import com.pristine.dataload.offermgmt.mwr.CommonDataHelper;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;

public class PriceGroupDataService {

	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param itemDataMap
	 * @throws GeneralException
	 */
	public HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> getPriceGroupDetails(Connection conn,
			RecommendationInputDTO recommendationInputDTO, HashMap<ItemKey, PRItemDTO> itemDataMap)
					throws GeneralException {

		PRStrategyDTO inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);

		inputDTO.setStartDate(recommendationInputDTO.getStartWeek());
		inputDTO.setEndDate(recommendationInputDTO.getEndWeek());
		
		PriceGroupRelationDAO pgDAO = new PriceGroupRelationDAO(conn);

		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupRelation = pgDAO.getPriceGroupDetails(inputDTO,
				itemDataMap);

		return priceGroupRelation;
	}

}
