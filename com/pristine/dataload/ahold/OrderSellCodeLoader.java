package com.pristine.dataload.ahold;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.OrderSellCodeDAO;
import com.pristine.dto.OrderSellCodeDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class OrderSellCodeLoader extends PristineFileParser {
	static Logger logger = Logger.getLogger("OrderSellCodeLoader");
	Connection conn = null;
	private int stopCount = -1;
	OrderSellCodeDAO oscDAO = new OrderSellCodeDAO();
	private List<OrderSellCodeDTO> orderAndSellCodes;

	public OrderSellCodeLoader() {
		super("analysis.properties");
		try {
			conn = DBManager.getConnection();

		} catch (GeneralException ex) {

		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-OrderCodeLoader.properties");
		String FilePath = PropertyManager.getProperty("DATALOAD.ROOTPATH");
		PropertyManager.initialize("analysis.properties");
		String subFolder = null;

		logger.info("main() - Started");

		for (int ii = 0; ii < args.length; ii++) {
			String arg = args[ii];
			if (arg.startsWith("SUBFOLDER")) {
				subFolder = arg.substring("SUBFOLDER=".length());
			}
		}
		OrderSellCodeLoader orderSellCodeLoader = new OrderSellCodeLoader();
		orderSellCodeLoader.processOrderSellCodeFile(subFolder);
	}

	private void processOrderSellCodeFile(String subFolder) {
		try {
			ArrayList<String> fileList = getFiles(subFolder);
			String fieldNames[] = new String[8];
			setHeaderPresent(true);
			fieldNames[0] = "DeptNum";
			fieldNames[1] = "SubDeptNum";
			fieldNames[2] = "OrderCode";
			fieldNames[3] = "OrderCodeDescr";
			fieldNames[4] = "SellCode";
			fieldNames[5] = "SellCodeDescr";
			fieldNames[6] = "UPCtoUse";
			fieldNames[7] = "Yield";

			logger.info("OrderSellCodeLoader starts");
			for (int j = 0; j < fileList.size(); j++) {
				orderAndSellCodes = new ArrayList<OrderSellCodeDTO>();
				logger.info("processing - " + fileList.get(j));
				parseDelimitedFile(OrderSellCodeDTO.class, fileList.get(j), ',', fieldNames, stopCount);
				loadOrderAndSellCodes(orderAndSellCodes);
				PristineDBUtil.commitTransaction(conn, "Commit");
			}
			logger.info("OrderSellCodeLoader ends");
		} catch (GeneralException ge) {
			logger.error("Error while processing OrderSellCodeLoader File - " + ge.toString() + ge);
			ge.printStackTrace();
			PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
		} finally {
			PristineDBUtil.close(getOracleConnection());
		}
	}

	@Override
	public void processRecords(List listobj) throws GeneralException {
		for (int j = 0; j < listobj.size(); j++) {
			OrderSellCodeDTO orderSellCodeDTO = (OrderSellCodeDTO) listobj.get(j);
			// Removing % sign in Yield
			if (orderSellCodeDTO.getYield().length() > 0
					&& orderSellCodeDTO.getYield().charAt(orderSellCodeDTO.getYield().length() - 1) == '%') {
				orderSellCodeDTO.setUpdatedYield(Float.parseFloat(orderSellCodeDTO.getYield().substring(0,
						orderSellCodeDTO.getYield().length() - 1)));
			} else if (orderSellCodeDTO.getYield() == null || orderSellCodeDTO.getYield().length() == 0
					|| "".equals(orderSellCodeDTO.getYield().trim())) {
				orderSellCodeDTO.setUpdatedYield(0.0f);
			}

			String upc = orderSellCodeDTO.getUPCtoUse();
			String sellCode = Integer.toString(orderSellCodeDTO.getSellCode());

			orderSellCodeDTO.setUPCtoUse(formUPC(upc, sellCode));

			orderAndSellCodes.add(orderSellCodeDTO);
		}
	}

	private void loadOrderAndSellCodes(List<OrderSellCodeDTO> orderAndSellCodes) throws GeneralException {
		int deleteCnt = 0, insertCnt = 0, updateCnt = 0;
		
		logger.info("Deleting all Records from PR_ORDER_SELL_CODE_MAPPING is Started");
		deleteCnt = oscDAO.deleteOrderSellCodeMapping(conn);
		logger.info("Deleting all Records from PR_ORDER_SELL_CODE_MAPPING is Completed");
		logger.info("No. of Records Deleted from PR_ORDER_SELL_CODE_MAPPING : " + deleteCnt);

		logger.info("Reseting SEQUENCE PR_REC_ORDER_CODE_ID_SEQ is Started");
		oscDAO.resetSequenceOrderSellCodeMapping(conn);
		logger.info("Reseting SEQUENCE PR_REC_ORDER_CODE_ID_SEQ is Completed");
		
		logger.info("Inserting Records in PR_ORDER_SELL_CODE_MAPPING is Started");
		insertCnt = oscDAO.insertOrderSellCodeMapping(conn, orderAndSellCodes);
		logger.info("Inserted Records in PR_ORDER_SELL_CODE_MAPPING is Completed");
		logger.info("No. of Records Inserted in PR_ORDER_SELL_CODE_MAPPING : " + insertCnt);

		logger.info("Setting SellCode to null in Item_lookup is Started");
		oscDAO.setSellCodeToNull(conn);
		logger.info("Setting SellCode to null in Item_lookup is Completed");

		logger.info("Updating SellCode in Item_lookup is Started");
		updateCnt = oscDAO.updateSellCodeInItemLookup(conn, orderAndSellCodes);
		logger.info("Updating SellCode in Item_Lookup is Completed");
		logger.info("No. of SellCodes udpated in ITEM_LOOKUP table : " + updateCnt);

		logger.info("Updating Yield in PR_ORDER_SELL_CODE_MAPPING is Started");
		updateCnt = oscDAO.updateYield(conn,orderAndSellCodes);
		logger.info("Updating Yield in PR_ORDER_SELL_CODE_MAPPING is Completed");
		logger.info("No. of Records updated Yield as 0 in PR_ORDER_SELL_CODE_MAPPING table : " + updateCnt);
	}

	public static String formUPC(String upc, String sellCode) {
		// Form UPC for '#N/A' records using SellCode
		String retUPC = null;
		if (upc.equals("#N/A")) {
			if (sellCode.substring(1, 4).contains("000")) {
				retUPC = "0" + sellCode.substring(0, 2) + sellCode.substring(4) + "0000";

			} else if (sellCode.substring(1, 3).contains("00")) {
				retUPC = "0" + sellCode.substring(0, 1) + sellCode.substring(3) + "0000";
			} else if (sellCode.substring(2, 3).contains("0")) {
				retUPC = "00" + sellCode.substring(0, 3) + sellCode.substring(3) + "0";

			} else {
				retUPC = "0" + sellCode + "00";
			}
		} else {
			retUPC = upc;
		}
		return retUPC;
	}

}
