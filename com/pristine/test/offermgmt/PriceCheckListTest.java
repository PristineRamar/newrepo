package com.pristine.test.offermgmt;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.CheckListService;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class PriceCheckListTest {
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("Testing");
	String chainId = "";
	
	public static void main(String[] args) throws GeneralException, Exception, OfferManagementException {
		PriceCheckListTest priceCheckListTest = new PriceCheckListTest();
		PropertyConfigurator.configure("log4j-testing.properties");
		PropertyManager.initialize("analysis.properties");
		
		priceCheckListTest.intialSetup();
		priceCheckListTest.defaults();
		
		priceCheckListTest.testPriceCheckList();
	}
	
	private void defaults() throws GeneralException {
		chainId = new RetailPriceDAO().getChainId(conn);
		
	}
	
	private void testPriceCheckList() throws NumberFormatException, GeneralException, OfferManagementException {
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		HashMap<ItemKey, List<PriceCheckListDTO>> priceCheckListInfo;
		CheckListService checkListService = new CheckListService();
		int locationId = 2909;
		int productId = 2040;
		String weekstartdate="08/08/2021";
		
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(216920);
		itemDataMap.put(new ItemKey(prItemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR), prItemDTO);

		int divisionId = new RetailPriceZoneDAO().getDivisionIdForZone(conn, locationId);

		priceCheckListInfo = pricingEngineDAO.getPriceCheckListInfo(conn, PRConstants.ZONE_LEVEL_TYPE_ID, locationId, Constants.CATEGORYLEVELID,
				productId,null);

		checkListService.populatePriceCheckListDetailsZone(conn, Integer.valueOf(chainId), divisionId, PRConstants.ZONE_LEVEL_TYPE_ID, locationId,
				Constants.CATEGORYLEVELID, productId, itemDataMap, priceCheckListInfo, divisionId, null, null, null, null, null, pricingEngineDAO, null, null, null, divisionId);
	}
	
	
	public void intialSetup() {
		initialize();
	}
	
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

	/**
	 * Initializes object
	 */
	protected void initialize() {
		setConnection();
	}
}
