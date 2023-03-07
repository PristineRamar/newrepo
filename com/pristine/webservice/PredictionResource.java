package com.pristine.webservice;


import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredHeaderAPIDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.multiWeekPrediction.MultiWeekPredictionService;

@Path("/predictionResource")
public class PredictionResource extends BaseResource {
	private static Logger logger = Logger.getLogger("PredictionResource");

	public PredictionResource() throws GeneralException {
		setLog4jProperties();
	}

	@Path("/multiWeekPrediction")
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public String multiWeekPrediction(String multiWeekPredHeaderDTO) {
		ObjectMapper mapper = new ObjectMapper();
		String jsonOutput = "";
		MultiWeekPredictionService multiWeekPredictionService = new MultiWeekPredictionService();

		try {
			MultiWeekPredHeaderAPIDTO objMultiWeekPredHeaderDTO = mapper.readValue(multiWeekPredHeaderDTO, MultiWeekPredHeaderAPIDTO.class);
			MultiWeekPredHeaderAPIDTO output = multiWeekPredictionService.onDemandPrediction(objMultiWeekPredHeaderDTO);
			jsonOutput = mapper.writeValueAsString(output);
		} catch (JsonProcessingException jexception){
			logger.error("Exception in multiWeekPrediction(): Incorrect Json format passed in input " +
			jexception.toString(), jexception);
		} catch (Exception exception) {
			logger.error("Exception in multiWeekPrediction()" + exception.toString(), exception);
		}

		return jsonOutput;
	}

}
