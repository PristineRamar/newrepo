package com.pristine.service;

import java.sql.Connection;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dao.CompStoreDAO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;

public class RetailStoreService {
	private static Logger logger = Logger.getLogger("RetailStoreService");
	
	public List<SummaryDataDTO> getStoreNumebrs(Connection conn,
			String districtNumber, String storeNumber) throws GeneralException {
		
		logger.debug("Create instance for CompStoreDAO...");
		CompStoreDAO objStoreDao = new CompStoreDAO();
		
		logger.debug("Call getStoreNumebrs...");		
		return objStoreDao.getStoreNumebrs(conn, districtNumber, storeNumber);
	}
	
}
