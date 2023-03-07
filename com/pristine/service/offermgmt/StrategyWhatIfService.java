package com.pristine.service.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.pristine.dao.offermgmt.StrategyDAO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.exception.OfferManagementException;

public class StrategyWhatIfService {

	public List<PRStrategyDTO> getWhatIfStrategies(Connection conn, List<Long> whatIfStrategyIds) throws OfferManagementException {
		List<PRStrategyDTO> whatIfStrategies = new ArrayList<PRStrategyDTO>();
		StrategyDAO strategyDAO = new StrategyDAO();
		// Get the strategy detail of the what-if strategies
		for (Long strategyId : whatIfStrategyIds) {
			PRStrategyDTO strategy = strategyDAO.getStrategyDefinition(conn, strategyId);
			whatIfStrategies.add(strategy);
		}
		return whatIfStrategies;
	}

	/**
	 * During strategy what-if, user may change more than one strategy and
	 * checks the financial results. If a strategy is already existing for those
	 * combination, replace those with the user modified strategies
	 * 
	 * @throws OfferManagementException
	 */
	public void replaceStrategies(HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap, List<PRStrategyDTO> whatIfStrategies)
			throws OfferManagementException {
		
		StrategyKey strategyKey = null;

		// Loop strategy what-if
		for (PRStrategyDTO whatIfStrategy : whatIfStrategies) {
			strategyKey = new StrategyKey(whatIfStrategy.getLocationLevelId(), whatIfStrategy.getLocationId(),
					whatIfStrategy.getProductLevelId(), whatIfStrategy.getProductId());
			List<PRStrategyDTO> existingStrategies = strategyMap.get(strategyKey);

			// Check if same strategy already exists with the same combination
			if (existingStrategies != null) {
				for (Iterator<PRStrategyDTO> iterator = existingStrategies.iterator(); iterator.hasNext();) {
					PRStrategyDTO strategy = iterator.next();
					if (strategy.getPriceCheckListId() == whatIfStrategy.getPriceCheckListId()
							&& strategy.getVendorId() == whatIfStrategy.getVendorId()
							&& strategy.getStateId() == whatIfStrategy.getStateId()
							&& strategy.getCriteriaId() == whatIfStrategy.getCriteriaId()) {
						iterator.remove();
					}
				}
			}
		}

		//Add all what-if strategies
		for (PRStrategyDTO whatIfStrategy : whatIfStrategies) {
			strategyKey = new StrategyKey(whatIfStrategy.getLocationLevelId(), whatIfStrategy.getLocationId(), whatIfStrategy.getProductLevelId(),
					whatIfStrategy.getProductId());
			if (strategyMap.get(strategyKey) != null) {
				strategyMap.get(strategyKey).add(whatIfStrategy);
			} else {
				List<PRStrategyDTO> newStrategies = new ArrayList<PRStrategyDTO>();
				newStrategies.add(whatIfStrategy);
				strategyMap.put(strategyKey, newStrategies);
			}
		}
	}
}
