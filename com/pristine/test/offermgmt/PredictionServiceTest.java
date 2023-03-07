package com.pristine.test.offermgmt;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonParser;
import com.pristine.dao.DBManager;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionOutputDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.dto.offermgmt.prediction.RunStatusDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.prediction.PredictionEngineBatchImpl;
import com.pristine.service.offermgmt.prediction.PredictionEngineInput;
import com.pristine.service.offermgmt.prediction.PredictionEngineItem;
import com.pristine.service.offermgmt.prediction.PredictionEngineOutput;
import com.pristine.service.offermgmt.prediction.PredictionException;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PredictionServiceTest {
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("Testing");
	 

	public static void main(String[] args) throws IOException {
		PropertyConfigurator.configure("log4j-testing.properties");
		PropertyManager.initialize("recommendation.properties");
					
		PredictionServiceTest predictionServiceTest = new PredictionServiceTest();
		predictionServiceTest.intialSetup();
		
		predictionServiceTest.testUpdatePrediction();
		//predictionServiceTest.testPredictMovement();
		//predictionServiceTest.testOnDemandPrediction();
		//predictionServiceTest.testPredictSubstitueImpact();
		// try{
		// PredictionEngineInput pei = initializePredictionEngineInput();
		// PredictionEngineBatchImpl p = new
		// PredictionEngineBatchImpl("D:/WorkspaceR/code/api/api_java.R");
		// PredictionEngineOutput peo = p.predict(pei);
		// peo = p.predict(pei);
		// System.out.print(peo);
		// }catch(PredictionException pe)
		// {
		// System.out.println("EX:"+pe.getMessage());
		// //pe.printStackTrace();
		// }
	}

	public void testUpdatePrediction(){
		//String predictionInput = "[{'r-r-i':6264,'l-n':'','p-l-i':4,'p-i':157,'l-l-i':6,'l-i':66,'s-c-i':1249,'e-c-i':1249,'s-w-d':'01/25/2015',";
		//predictionInput = predictionInput + "'e-w-d':'01/31/2015','p-b':'prestolive','r-t':'O','u-s-f':'N','i-f-p':false,'p-items':[],'i-g-a-m':false}]";
		
		String predictionInput = "[{\"r-r-i\":6264,\"l-n\":\"\",\"p-l-i\":4,\"p-i\":157,\"l-l-i\":6,\"l-i\":66,\"s-c-i\":1249,\"e-c-i\":1249,\"s-w-d\":\"01/25/2015\",\"e-w-d\":\"01/31/2015\",\"p-b\":\"prestolive\",\"r-t\":\"O\",\"u-s-f\":\"N\",\"i-f-p\":false,\"p-items\":[],\"i-g-a-m\":false}]";
		predictionInput = "[{'r-r-i':10352,'l-n':'','p-l-i':4,'p-i':264,'l-l-i':6,'l-i':47,'s-c-i':1250,'e-c-i':1250,'s-w-d':'02/01/2015','e-w-d':'02/07/2015','p-b':'prestolive','r-t':'O','u-s-f':'N','i-f-p':false,'p-items':[],'i-g-a-m':false},{'r-r-i':11177,'l-n':'','p-l-i':4,'p-i':264,'l-l-i':6,'l-i':69,'s-c-i':1250,'e-c-i':1250,'s-w-d':'02/01/2015','e-w-d':'02/07/2015','p-b':'prestolive','r-t':'O','u-s-f':'N','i-f-p':false,'p-items':[],'i-g-a-m':false}][{'r-r-i':10352,'l-n':'','p-l-i':4,'p-i':264,'l-l-i':6,'l-i':47,'s-c-i':1250,'e-c-i':1250,'s-w-d':'02/01/2015','e-w-d':'02/07/2015','p-b':'prestolive','r-t':'O','u-s-f':'N','i-f-p':false,'p-items':[],'i-g-a-m':false},{'r-r-i':11177,'l-n':'','p-l-i':4,'p-i':264,'l-l-i':6,'l-i':69,'s-c-i':1250,'e-c-i':1250,'s-w-d':'02/01/2015','e-w-d':'02/07/2015','p-b':'prestolive','r-t':'O','u-s-f':'N','i-f-p':false,'p-items':[],'i-g-a-m':false}]";
		
		 ObjectMapper mapper = new ObjectMapper();
		 mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		 JsonNode df;
		try {
			df = mapper.readValue(predictionInput, JsonNode.class);
			PredictionService predictionService = new PredictionService();
//			predictionService.updatePredictionTest(conn, df.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	
	}
	public void testPredictSubstitueImpact() {
		PredictionService predictionService = new PredictionService();
		PredictionInputDTO predictionInputDTO = new PredictionInputDTO();

		fillPredictionInputForOnDemand(predictionInputDTO);
		predictionInputDTO.useSubstFlag = String.valueOf(Constants.YES);
		addItems(predictionInputDTO);
		
		 
		 
		 //String jsonTestString = "{'r-r-i':2356,'p-l-i':4,'p-i':149,'l-l-i':6,'l-i':66,'s-c-i':850,'e-c-i':850,'s-w-d':'11/09/2014','e-w-d':'11/15/2014','p-b':'presolive','r-t':'O','i-f-p':false,'p-items':[{'i-c-o-l-i':47813,'u':'001370021735','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':63592,'u':'001370021725','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':53386,'u':'001370020625','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':801,'u':'001370083546','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':84955,'u':'001370021755','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':795,'u':'001370021720','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':19808,'u':'001370083545','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':171057,'u':'001370083540','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':794,'u':'001370009545','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':792,'u':'001370004520','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':16009,'u':'001370003547','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':170081,'u':'001370083638','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':150942,'u':'001370027585','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':171107,'u':'001370083539','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':194966,'u':'001370025735','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':196177,'u':'001370085528','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':194967,'u':'001370025755','l-i':false,'p-p':[{'r-q':1,'r-p':7.29,'p-m':0.0,'e-p':'','i-a-p':true}],'u-s-f':'Y','m-o-i-f':'M'}]}";
		String jsonTestString = "{'r-r-i':1234,'p-l-i':4,'p-i':156,'l-l-i':6,'l-i':66,'s-c-i':850,'e-c-i':850,'s-w-d':'11/09/2014','e-w-d':'11/15/2014','p-b':null,'r-t':null,'i-f-p':true,'p-items':[{'i-c-o-l-i':59637,'u':'068826712665','l-i':false,'p-p':[{'r-q':1,'r-p':4.29,'p-m':0.0,'e-p':null}],'u-s-f':'Y','m-o-i-f':'M'},{'i-c-o-l-i':178411,'u':'008860301069','l-i':false,'p-p':[{'r-q':1,'r-p':4.29,'p-m':0.0,'e-p':null}],'u-s-f':'Y','m-o-i-f':'M'}]}";
								
		jsonTestString = "{'r-r-i':14707,'l-n':null,'p-l-i':4,'p-i':264,'l-l-i':6,'l-i':66,'s-c-i':1251,'e-c-i':1251,'s-w-d':'02/08/2015','e-w-d':'02/14/2015','p-b':'presolive','r-t':'O','u-s-f':'Y','i-f-p':true,'p-items':[{'i-c-o-l-i':71769,'u':'068826703587','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':'','p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'M'},{'i-c-o-l-i':9361,'u':'068826707267','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':'','p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'M'},{'i-c-o-l-i':36412,'u':'068826707269','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':'','p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'M'},{'i-c-o-l-i':56920,'u':'068826712897','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':'','p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'M'},{'i-c-o-l-i':192176,'u':'068826702363','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':'','p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'M'},{'i-c-o-l-i':42002,'u':'068826703975','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':'','p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'M'},{'i-c-o-l-i':9362,'u':'068826707273','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':'','p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'M'},{'i-c-o-l-i':75534,'u':'068826703647','l-i':false,'p-p':[{'r-q':1,'r-p':4.49,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':9233,'u':'068826703646','l-i':false,'p-p':[{'r-q':1,'r-p':4.49,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':29836,'u':'068826703645','l-i':false,'p-p':[{'r-q':1,'r-p':4.49,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':23093,'u':'007482270659','l-i':false,'p-p':[{'r-q':1,'r-p':4.99,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':51048,'u':'007482261063','l-i':false,'p-p':[{'r-q':1,'r-p':4.99,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':141984,'u':'005150024112','l-i':false,'p-p':[{'r-q':1,'r-p':2.79,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':141981,'u':'005150024307','l-i':false,'p-p':[{'r-q':1,'r-p':2.79,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':64815,'u':'005150024130','l-i':false,'p-p':[{'r-q':1,'r-p':2.79,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':4451,'u':'005150024138','l-i':false,'p-p':[{'r-q':1,'r-p':2.79,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':4450,'u':'005150024136','l-i':false,'p-p':[{'r-q':1,'r-p':2.79,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':192163,'u':'005150024090','l-i':false,'p-p':[{'r-q':1,'r-p':6.19,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':41588,'u':'005150024321','l-i':false,'p-p':[{'r-q':1,'r-p':6.19,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':60136,'u':'005150024091','l-i':false,'p-p':[{'r-q':1,'r-p':6.19,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':307204,'u':'003760029062','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':157385,'u':'004800121125','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':56473,'u':'004800100687','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':15327,'u':'004800100678','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':191401,'u':'003760011087','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':191408,'u':'003760010557','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':191405,'u':'003760010503','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':191404,'u':'003760010506','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':44354,'u':'004800100677','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':4181,'u':'004800100686','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':4180,'u':'004800100681','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':192469,'u':'003760011072','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':192468,'u':'003760010499','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':192467,'u':'003760011075','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':192466,'u':'003760010500','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':192465,'u':'003760010508','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':192463,'u':'003760010549','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':141868,'u':'004800121210','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':88963,'u':'004800100641','l-i':false,'p-p':[{'r-q':1,'r-p':2.39,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':38786,'u':'005150005038','l-i':false,'p-p':[{'r-q':1,'r-p':3.29,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':19141,'u':'005150004346','l-i':false,'p-p':[{'r-q':1,'r-p':3.29,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':15351,'u':'005150001700','l-i':false,'p-p':[{'r-q':1,'r-p':3.29,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':43491,'u':'005150001701','l-i':false,'p-p':[{'r-q':1,'r-p':3.29,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':192172,'u':'005150025594','l-i':false,'p-p':[{'r-q':1,'r-p':2.99,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':160218,'u':'005150025565','l-i':false,'p-p':[{'r-q':1,'r-p':2.99,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':160124,'u':'005150025527','l-i':false,'p-p':[{'r-q':1,'r-p':2.99,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':160082,'u':'005150025518','l-i':false,'p-p':[{'r-q':1,'r-p':2.99,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':160085,'u':'005150025537','l-i':false,'p-p':[{'r-q':1,'r-p':2.99,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':160077,'u':'005150025516','l-i':false,'p-p':[{'r-q':1,'r-p':2.99,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':159982,'u':'005150025530','l-i':false,'p-p':[{'r-q':1,'r-p':2.99,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':162761,'u':'005150025578','l-i':false,'p-p':[{'r-q':1,'r-p':2.99,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'},{'i-c-o-l-i':68581,'u':'005150024139','l-i':false,'p-p':[{'r-q':1,'r-p':2.79,'p-m':0.0,'e-p':null,'p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'I'}],'i-g-a-m':false}";
		
		 
		try {
			 ObjectMapper mapper = new ObjectMapper();
			 mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
			 JsonNode df = mapper.readValue(jsonTestString, JsonNode.class);
			 
			//String test = mapper.writeValueAsString(predictionInputDTO);
			//predictionService.predictSubstituteImpactTest(conn, mapper.writeValueAsString(predictionInputDTO));
//			predictionService.predictSubstituteImpactTest(conn, df.toString());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void testOnDemandPrediction() throws IOException {
		PredictionService predictionService = new PredictionService();
		PredictionInputDTO predictionInputDTO1 = new PredictionInputDTO();
		PredictionInputDTO predictionInputDTO2 = new PredictionInputDTO();
		ObjectMapper mapper = new ObjectMapper();
		 
		fillPredictionInputForOnDemand1(predictionInputDTO1);
		addItemsForOnDemandPrediction(predictionInputDTO1);
		
		fillPredictionInputForOnDemand2(predictionInputDTO2);
		addItemsForOnDemandPrediction(predictionInputDTO2);
		
		List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
		
		predictionInputDTOs.add(predictionInputDTO1);
		predictionInputDTOs.add(predictionInputDTO2);
		
		try {
			//String test = mapper.writeValueAsString(predictionInputDTO);
			String input = mapper.writeValueAsString(predictionInputDTOs);

			input =  "[{'r-r-i':14691,'l-n':'693','p-l-i':4,'p-i':569,'l-l-i':6,'l-i':66,'s-c-i':1251,'e-c-i':1251,'s-w-d':'02/08/2015','e-w-d':'02/14/2015','p-b':'prestolive','r-t':'O','u-s-f':'N','i-f-p':false,'p-items':[{'i-c-o-l-i':125633,'u':'068826712345','l-i':false,'p-p':[{'r-q':1,'r-p':26.79,'p-m':0.0,'e-p':'','p-s':0,'s-i-min':'','s-i-max':''}],'m-o-i-f':'N'}],'i-g-a-m':false}]";
			 mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
			 JsonNode df = mapper.readValue(input, JsonNode.class);
			 
			
			
//			RunStatusDTO runStatusDTO = predictionService.predictMovementOnDemandTest(conn, df.toString());
			
			logger.debug("Test Message");
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void fillPredictionInputForOnDemand(PredictionInputDTO predictionInputDTO){
		predictionInputDTO.recommendationRunId = 29;
		predictionInputDTO.locationLevelId = 6;
		predictionInputDTO.locationId = 66;
		predictionInputDTO.productLevelId = 4;
		predictionInputDTO.productId = 264;
		predictionInputDTO.startCalendarId = 1251;
		predictionInputDTO.endCalendarId = 1251;
		predictionInputDTO.startWeekDate = "08/24/2014";
		predictionInputDTO.endWeekDate = "08/24/2014";		 
		predictionInputDTO.predictedBy = "ONLINE";
	}
	
	private void fillPredictionInputForOnDemand1(PredictionInputDTO predictionInputDTO){
		predictionInputDTO.recommendationRunId = 29;
		predictionInputDTO.locationLevelId = 6;
		predictionInputDTO.locationId = 64;
		predictionInputDTO.productLevelId = 4;
		predictionInputDTO.productId = 264;
		predictionInputDTO.startCalendarId = 1251;
		predictionInputDTO.endCalendarId = 1251;
		predictionInputDTO.startWeekDate = "08/24/2014";
		predictionInputDTO.endWeekDate = "08/24/2014";		 
		predictionInputDTO.predictedBy = "ONLINE";
	}
	
	private void fillPredictionInputForOnDemand2(PredictionInputDTO predictionInputDTO){
		predictionInputDTO.recommendationRunId = 29;
		predictionInputDTO.locationLevelId = 6;
		predictionInputDTO.locationId = 66;
		predictionInputDTO.productLevelId = 4;
		predictionInputDTO.productId = 264;
		predictionInputDTO.startCalendarId = 1251;
		predictionInputDTO.endCalendarId = 1251;
		predictionInputDTO.startWeekDate = "08/24/2014";
		predictionInputDTO.endWeekDate = "08/24/2014";		 
		predictionInputDTO.predictedBy = "ONLINE";
	}
	
	private void addItemsForOnDemandPrediction(PredictionInputDTO predictionInputDTO){
		PredictionItemDTO predictionItemDTO;
		PricePointDTO pricePointDTO;
		
		predictionInputDTO.predictionItems = new ArrayList<PredictionItemDTO>();
		
		//Item with one price point
		predictionItemDTO = new PredictionItemDTO();
		predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();
		predictionItemDTO.itemCodeOrLirId = 12486;
		predictionItemDTO.lirInd = false;		
		predictionItemDTO.upc = "005260011216";

		pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(1);
		pricePointDTO.setRegPrice(2.39);
		pricePointDTO.setPredictionStatus(PredictionStatus.ERROR_MISC);
		predictionItemDTO.pricePoints.add(pricePointDTO);
		
		pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(1);
		pricePointDTO.setRegPrice(2.49);
		pricePointDTO.setPredictionStatus(PredictionStatus.ERROR_MISC);
		predictionItemDTO.pricePoints.add(pricePointDTO);

		predictionInputDTO.predictionItems.add(predictionItemDTO);
		
		predictionItemDTO = new PredictionItemDTO();
		predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();
		predictionItemDTO.itemCodeOrLirId = 83773;
		predictionItemDTO.lirInd = false;		
		predictionItemDTO.upc = "068826703648";

		pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(1);
		pricePointDTO.setRegPrice(12.39);
		pricePointDTO.setPredictionStatus(PredictionStatus.ERROR_MISC);
		predictionItemDTO.pricePoints.add(pricePointDTO);
		
		pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(1);
		pricePointDTO.setRegPrice(12.49);
		pricePointDTO.setPredictionStatus(PredictionStatus.ERROR_MISC);
		predictionItemDTO.pricePoints.add(pricePointDTO);

		predictionInputDTO.predictionItems.add(predictionItemDTO);
	}
	
	private void addItems(PredictionInputDTO predictionInputDTO){
		PredictionItemDTO predictionItemDTO;
		PricePointDTO pricePointDTO;
		
		predictionInputDTO.predictionItems = new ArrayList<PredictionItemDTO>();
		
		//Item with one price point
		predictionItemDTO = new PredictionItemDTO();
		predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();
		predictionItemDTO.itemCodeOrLirId = 192451;
		predictionItemDTO.lirInd = false;		
		predictionItemDTO.upc = "085114900504";

		pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(1);
		pricePointDTO.setRegPrice(4.99);
		pricePointDTO.setPredictionStatus(PredictionStatus.ERROR_MISC);
		predictionItemDTO.pricePoints.add(pricePointDTO);
		
		pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(1);
		pricePointDTO.setRegPrice(5.19);
		pricePointDTO.setPredictionStatus(PredictionStatus.ERROR_MISC);
		predictionItemDTO.pricePoints.add(pricePointDTO);

		predictionInputDTO.predictionItems.add(predictionItemDTO);
		
		predictionItemDTO = new PredictionItemDTO();
		predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();
		predictionItemDTO.itemCodeOrLirId = 1263;
		predictionItemDTO.lirInd = false;		
		predictionItemDTO.upc = "002073511013";

		pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(1);
		pricePointDTO.setRegPrice(2.69);
		pricePointDTO.setPredictionStatus(PredictionStatus.ERROR_MISC);
		predictionItemDTO.pricePoints.add(pricePointDTO);
		
		pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(1);
		pricePointDTO.setRegPrice(2.79);
		pricePointDTO.setPredictionStatus(PredictionStatus.ERROR_MISC);
		predictionItemDTO.pricePoints.add(pricePointDTO);

		predictionInputDTO.predictionItems.add(predictionItemDTO);
	}
	
	private static PredictionEngineInput initializePredictionEngineInput() {
		PredictionEngineInput pi = new PredictionEngineInput();
		pi.setProductLevelId(4); // category level
		pi.setProductId(263); // category id
//		pi.setLocationLevelId(6); // zone level
//		pi.setLocationId(66); // zone id
//		pi.setPredictionStartDateStr("2014-03-16"); // start day of week i.e.
//													// sunday

		List<PredictionEngineItem> itemList = new ArrayList<PredictionEngineItem>();
//		itemList.add(new PredictionEngineItem(1, 173643, 980080005, 1.59, 1, Constants.NO, true, false));
//		itemList.add(new PredictionEngineItem(1, 180184, 980089220, 8.49, 1, Constants.NO, true, false));
//		itemList.add(new PredictionEngineItem(1, 51539, 980089500, 3.39, 1, Constants.NO, true, false));
		pi.setPredictionEngineItems(itemList);

		return pi;
	}

	public void testPredictMovement() {
		initialize();

		// Test 1
		PredictionService predictionService = new PredictionService();
		PredictionInputDTO predictionInputDTO = new PredictionInputDTO();
		PredictionOutputDTO predictionOutputDTO = new PredictionOutputDTO();
		PredictionItemDTO predictionItemDTO;
		PricePointDTO pricePointDTO;

		Calendar cal = Calendar.getInstance();
		cal.set(2014, Calendar.AUGUST, 31); // Year, month and day of month
		java.util.Date date = cal.getTime();

		predictionInputDTO.predictionItems = new ArrayList<PredictionItemDTO>();
		predictionInputDTO.productLevelId = 4;
		predictionInputDTO.productId = 264;
		predictionInputDTO.startCalendarId = 842;
		predictionInputDTO.endCalendarId = 842;
		predictionInputDTO.startWeekDate = "08/24/2014";
		predictionInputDTO.endWeekDate = "08/24/2014";
		predictionInputDTO.locationLevelId = 6;
		predictionInputDTO.locationId = 66;
		predictionInputDTO.predictedBy = "BATCH";

		predictionItemDTO = new PredictionItemDTO();
		predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();
		predictionItemDTO.itemCodeOrLirId = 21076;
		predictionItemDTO.lirInd = false;
		// predictionItemDTO.listCost = 1.5;
		predictionItemDTO.upc = "005150024177";

		pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(1);
		pricePointDTO.setRegPrice(4.29);
		predictionItemDTO.pricePoints.add(pricePointDTO);

		pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(1);
		pricePointDTO.setRegPrice(4.19);
		predictionItemDTO.pricePoints.add(pricePointDTO);

		predictionInputDTO.predictionItems.add(predictionItemDTO);

		/*
		 * predictionItemDTO = new PredictionItemDTO();
		 * predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();
		 * predictionItemDTO.itemCodeOrLirId = 56517; predictionItemDTO.lirInd
		 * =false; //predictionItemDTO.listCost = 1.8; predictionItemDTO.upc =
		 * "3377610030";
		 * 
		 * pricePointDTO = new PricePointDTO(); pricePointDTO.setRegQuantity(1);
		 * pricePointDTO.setRegPrice(3.59);
		 * 
		 * predictionItemDTO.pricePoints.add(pricePointDTO); pricePointDTO = new
		 * PricePointDTO(); pricePointDTO.setRegQuantity(1);
		 * pricePointDTO.setRegPrice(2.0);
		 * predictionItemDTO.pricePoints.add(pricePointDTO);
		 * 
		 * predictionInputDTO.predictionItems.add(predictionItemDTO);
		 */

		try {
			List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
//			predictionOutputDTO = predictionService.predictMovement(conn, predictionInputDTO, executionTimeLogs, "PREDICTION_PRICE_REC");
			// RunStatusDTO runStatusDto;
			// runStatusDto =
			// predictionService.predictMovementOnDemand(predictionInputDTO,
			// true);
			PristineDBUtil.commitTransaction(conn, "Transaction is Committed");
		} catch (GeneralException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			PristineDBUtil.close(conn);
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
