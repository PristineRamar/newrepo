package com.pristine.util;

import java.sql.Connection;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.PriceGroupRelationDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dto.offermgmt.PRConstraintRounding;
import com.pristine.dto.offermgmt.PRConstraintThreshold;
import com.pristine.dto.offermgmt.PRPriceGroupRelnDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.offermgmt.PRConstants;

public class Test {
	public static void main(String[] args) {
		// Tests Threshold
		/*try{
			PropertyManager.initialize("analysis.properties");
			Connection conn = DBManager.getConnection();
			PricingEngineDAO dao = new PricingEngineDAO();
			PRConstraintRounding roundingConstraint = new PRConstraintRounding();
			roundingConstraint.setRoundingTableId(2);
			roundingConstraint.setRoundingTableContent(dao.getRoundingTableDetail(conn, 2));
			PRConstraintThreshold threshold = new PRConstraintThreshold();
			threshold.setMaxValue(10);
			threshold.setValueType('P');
			PRRange inputRange = new PRRange();
			inputRange.setStartVal(Constants.DEFAULT_NA);
			inputRange.setEndVal(27.74);
			threshold.getPriceRange(29.99, inputRange, roundingConstraint);
		}catch(GeneralException ge){
			ge.printStackTrace();
		}*/
		
			// Tests filterRange Method
			/*PricingEngineWS pricingEngineWS = new PricingEngineWS();
			PRRange inputRange = new PRRange();
			inputRange.setStartVal(Constants.DEFAULT_NA);
			inputRange.setEndVal(27.74);
			PRRange guidelineRange = new PRRange();
			guidelineRange.setStartVal(29.99);
			guidelineRange.setEndVal(32.99);
			PRRange outputRange = pricingEngineWS.filterRange(inputRange, guidelineRange);
			System.out.println(outputRange.toString());
			System.out.println(outputRange.isConflict());*/
		
		// Test Price Group Retrieval
		try{
			PropertyManager.initialize("analysis.properties");
			Connection conn = DBManager.getConnection();
			PriceGroupRelationDAO pgDAO = new PriceGroupRelationDAO(conn);
			PRStrategyDTO inputDTO = new PRStrategyDTO();
			inputDTO.setLocationLevelId(6);
			inputDTO.setLocationId(66);
			inputDTO.setProductLevelId(4);
			inputDTO.setProductId(149);
			inputDTO.setStartDate("08/18/2014");
			//pgDAO.getPriceGroupDetails(inputDTO);
		}catch(GeneralException ge){
			ge.printStackTrace();
		}
		
		// Test Size relation
		/*PRPriceGroupRelnDTO reln = new PRPriceGroupRelnDTO();
		reln.setRetailType(PRConstants.RETAIL_TYPE_UNIT);
		reln.setOperatorText(PRConstants.PRICE_GROUP_EXPR_LESSER_SYM);
		reln.setValueType(PRConstants.VALUE_TYPE_PCT);
		reln.setMinValue(5);
		reln.setMaxValue(Constants.DEFAULT_NA);
		reln.getBrandPriceRangeWithPct(6.19, 128, 89);*/
	}	
}

