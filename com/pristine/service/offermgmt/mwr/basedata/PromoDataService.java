package com.pristine.service.offermgmt.mwr.basedata;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;

public class PromoDataService {

	/**
	 * 
	 * @param conn
	 * @param storeList
	 * @param recommendationInputDTO
	 * @return Sale details
	 * @throws GeneralException
	 */
	public HashMap<Integer, List<PRItemSaleInfoDTO>> getSaleDetails(Connection conn, List<Integer> storeList,
			RecommendationInputDTO recommendationInputDTO) throws GeneralException {
		// 31st Jan 2017, passed price zone stores as previously stores in a zone are taken from the competitor store
		// table. for GE, there is no zone defined for store in competitor store table. So in order to work for all
		// clients the stores are passed as parameter itself

		int noOfsaleAdDisplayWeeks = MultiWeekRecConfigSettings.getMwrNoOfSaleAdDisplayWeeks();

		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();

		if (recommendationInputDTO.isPriceTestZone())
			saleDetails = new PricingEngineDAO().getSaleDetails(conn, recommendationInputDTO.getProductLevelId(),
					recommendationInputDTO.getProductId(), recommendationInputDTO.getChainId(),
					recommendationInputDTO.getTempLocationID(), recommendationInputDTO.getQuarterStartDate(),
					noOfsaleAdDisplayWeeks, storeList, recommendationInputDTO.isGlobalZone());
		else
			saleDetails = new PricingEngineDAO().getSaleDetails(conn, recommendationInputDTO.getProductLevelId(),
					recommendationInputDTO.getProductId(), recommendationInputDTO.getChainId(),
					recommendationInputDTO.getLocationId(), recommendationInputDTO.getQuarterStartDate(),
					noOfsaleAdDisplayWeeks, storeList, recommendationInputDTO.isGlobalZone());

		return saleDetails;
	}

}
