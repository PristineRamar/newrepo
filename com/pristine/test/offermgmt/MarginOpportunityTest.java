package com.pristine.test.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.MarginOpportunity;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;

public class MarginOpportunityTest {
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("Testing");

	public static void main(String[] args) throws GeneralException, Exception, OfferManagementException {
		PropertyConfigurator.configure("log4j-testing.properties");
		PropertyManager.initialize("recommendation.properties");

		MarginOpportunityTest marginOpportunityTest = new MarginOpportunityTest();
		marginOpportunityTest.intialSetup();
		marginOpportunityTest.marginOpporTest();
	}

	private void marginOpporTest() throws GeneralException {
		MarginOpportunity marginOppor = new MarginOpportunity();
		List<Integer> priceZoneStores = new ArrayList<Integer>();
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		ItemService itemService = new ItemService(executionTimeLogs);
		List<PRItemDTO> itemListWithOpp = new ArrayList<PRItemDTO>();
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();

		curCalDTO.setStartDate("11/22/2015");

		PRItemDTO prItemDTO = new PRItemDTO();
		prItemDTO.setLir(false);
		prItemDTO.setRetLirId(-1);
		prItemDTO.setIsOppurtunity(String.valueOf(Constants.YES));
		prItemDTO.setOppurtunityQty(1);
		prItemDTO.setOppurtunityPrice(2.99);
		prItemDTO.setItemCode(167062);
		itemListWithOpp.add(prItemDTO);

		prItemDTO = new PRItemDTO();
		prItemDTO.setLir(true);
		prItemDTO.setRetLirId(1000);
		prItemDTO.setIsOppurtunity(String.valueOf(Constants.YES));
		prItemDTO.setOppurtunityQty(1);
		prItemDTO.setOppurtunityPrice(3.19);
		prItemDTO.setItemCode(1000);
		itemListWithOpp.add(prItemDTO);

		prItemDTO = new PRItemDTO();
		prItemDTO.setLir(false);
		prItemDTO.setRetLirId(1000);
		prItemDTO.setIsOppurtunity(String.valueOf(Constants.YES));
		prItemDTO.setOppurtunityQty(1);
		prItemDTO.setOppurtunityPrice(3.19);
		prItemDTO.setItemCode(114139);
		itemListWithOpp.add(prItemDTO);

		priceZoneStores = itemService.getPriceZoneStores(conn, 4, 1297, 6, 6);
		marginOppor.resetOpporIfNotInHistory(conn, priceZoneStores, itemListWithOpp, curCalDTO);

		for (PRItemDTO itemDTO : itemListWithOpp) {
			logger.debug("getIsOppurtunity:" + itemDTO.getIsOppurtunity());
		}
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
