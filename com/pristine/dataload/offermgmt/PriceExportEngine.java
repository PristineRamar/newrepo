package com.pristine.dataload.offermgmt;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.rosuda.JRI.Rengine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dto.offermgmt.AuditTrailDetailDTO;

public class PriceExportEngine {
	private static Logger logger = Logger.getLogger("PriceExportEngine");

	private AuditTrailDetailDTO callAuditMessageForPriceExport(Rengine re, AuditTrailDetailDTO inputMessageCall)
			throws JsonProcessingException {
		String input = "";
		String output = "";
		String timeStamp = new SimpleDateFormat("MMddyyyy_HHmmss").format(new Date());

		AuditTrailDetailDTO outputMessageCall = new AuditTrailDetailDTO();
		try {
			ObjectMapper mapper = new ObjectMapper();

			String logPath = null;
			
			input = mapper.writeValueAsString(inputMessageCall);
			
			AuditTrailDetailDTO auditTrailDetailDto = inputMessageCall;
			mapper.writeValue(
					new File(logPath + "/" + auditTrailDetailDto.getProductId() + "_"
							+ auditTrailDetailDto.getLocationId() + "_input_" + timeStamp + ".json"),inputMessageCall);

		} catch (Exception e) {
			logger.error("Error while executing Multi Week Prediction Engine(): " + e.getMessage(), e);
		}

		return outputMessageCall;
	}
}
