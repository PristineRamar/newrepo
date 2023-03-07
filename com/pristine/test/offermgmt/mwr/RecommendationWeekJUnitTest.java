package com.pristine.test.offermgmt.mwr;

import static org.junit.Assert.*;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import com.pristine.dataload.offermgmt.mwr.RecommendationWeek;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class RecommendationWeekJUnitTest {
	private static Logger logger = Logger.getLogger("RecommendationWeekTest");

	@Before
	public void init() {
		PropertyManager.initialize("recommendation.properties");
	}

	/**
	 * 1. For future quarter – use Quarter Start Week and Quarter End Week. Current week will be the base week
	 * 
	 * @throws GeneralException
	 */
	@Test
	public void case1_FutureQuarterRecommendation() throws GeneralException {

		logger.info("case1_FutureQuarterRecommendation() - Future quarter");

		RecommendationWeek recommendationWeek = new RecommendationWeek(null, null);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper
				.getRecommendationInputDTO(null, null, "04/29/2018", "07/28/2018", PRConstants.MW_QUARTER_RECOMMENDATION, 2);

		RecommendationInputHelper.calDatails.put("04/29/2018", RecommendationInputHelper.getCalendarId(2001));
		RecommendationInputHelper.calDatails.put("07/28/2018", RecommendationInputHelper.getCalendarId(2002));

		recommendationWeek.setupRecommendationWeeks(recommendationInputDTO, RecommendationInputHelper.calDatails);
		
		recommendationWeek.logRecWeeksDetail(recommendationInputDTO);

		RecommendationInputDTO expectedDTO = new RecommendationInputDTO();

		expectedDTO.setActualEndCalendarId(2002);
		expectedDTO.setActualStartCalendarId(2001);
		expectedDTO.setStartCalendarId(2001);
		expectedDTO.setEndCalendarId(2002);
		expectedDTO.setBaseWeek("04/15/2018");
		assertEquals("Acutal start calendar id not set", expectedDTO.getActualStartCalendarId(),
				recommendationInputDTO.getActualStartCalendarId());

		assertEquals("Acutal end calendar id not set", expectedDTO.getActualEndCalendarId(),
				recommendationInputDTO.getActualEndCalendarId());

		assertEquals("Start calendar id not set", expectedDTO.getStartCalendarId(),
				recommendationInputDTO.getStartCalendarId());

		assertEquals("End calendar id not set", expectedDTO.getEndCalendarId(),
				recommendationInputDTO.getEndCalendarId());
		
		/*assertEquals("Base incorrect", expectedDTO.getBaseWeek(),
				recommendationInputDTO.getBaseWeek());*/

		logger.info("*************************************************************");
	}

	/**
	 * 2. For past quarter – use Quarter Start Week and Quarter End Week. Use “No of Weeks in Advance” to identity the base week
	 * 
	 * @throws GeneralException
	 */
	@Test
	public void case2_PastQuarterRecommendation() throws GeneralException {

		logger.info("case2_PastQuarterRecommendation() - Past quarter");

		RecommendationWeek recommendationWeek = new RecommendationWeek(null, null);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper
				.getRecommendationInputDTO(null, null, "10/08/2017", "01/06/2018", PRConstants.MW_QUARTER_RECOMMENDATION, 2);

		RecommendationInputHelper.calDatails.put("10/08/2017", RecommendationInputHelper.getCalendarId(2001));
		RecommendationInputHelper.calDatails.put("01/06/2018", RecommendationInputHelper.getCalendarId(2002));

		recommendationWeek.setupRecommendationWeeks(recommendationInputDTO, RecommendationInputHelper.calDatails);
		
		recommendationWeek.logRecWeeksDetail(recommendationInputDTO);

		RecommendationInputDTO expectedDTO = new RecommendationInputDTO();

		expectedDTO.setActualEndCalendarId(2002);
		expectedDTO.setActualStartCalendarId(2001);
		expectedDTO.setStartCalendarId(2001);
		expectedDTO.setEndCalendarId(2002);
		expectedDTO.setBaseWeek("09/24/2017");
		
		assertEquals("Acutal start calendar id not set", expectedDTO.getActualStartCalendarId(),
				recommendationInputDTO.getActualStartCalendarId());

		assertEquals("Acutal end calendar id not set", expectedDTO.getActualEndCalendarId(),
				recommendationInputDTO.getActualEndCalendarId());

		assertEquals("Start calendar id not set", expectedDTO.getStartCalendarId(),
				recommendationInputDTO.getStartCalendarId());

		assertEquals("End calendar id not set", expectedDTO.getEndCalendarId(),
				recommendationInputDTO.getEndCalendarId());
		assertEquals("Base incorrect", expectedDTO.getBaseWeek(),
				recommendationInputDTO.getBaseWeek());

		logger.info("*************************************************************");
	}

	
	/**
	 * 3. For X weeks recommendation (future) use the “Recommendation Start Week” and “Recommendation End Week”. Current week will
	 * be the base week
	 * 
	 * @throws GeneralException
	 */
	
	@Test
	public void case3_XweeksFuture() throws GeneralException {

		logger.info("case3_XweeksFuture() - X Weeks Future");

		RecommendationWeek recommendationWeek = new RecommendationWeek(null, null);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper
				.getRecommendationInputDTO("05/06/2018", "06/02/2018", null, null, PRConstants.MW_X_WEEKS_RECOMMENDATION, 2);

		RecommendationInputHelper.calDatails.put("05/06/2018", RecommendationInputHelper.getCalendarId(2001));
		RecommendationInputHelper.calDatails.put("06/02/2018", RecommendationInputHelper.getCalendarId(2002));

		recommendationWeek.setupRecommendationWeeks(recommendationInputDTO, RecommendationInputHelper.calDatails);
		
		recommendationWeek.logRecWeeksDetail(recommendationInputDTO);

		RecommendationInputDTO expectedDTO = new RecommendationInputDTO();

		expectedDTO.setActualEndCalendarId(2002);
		expectedDTO.setActualStartCalendarId(2001);
		expectedDTO.setStartCalendarId(2001);
		expectedDTO.setEndCalendarId(2002);
		expectedDTO.setBaseWeek("04/15/2018");
		
		assertEquals("Acutal start calendar id not set", expectedDTO.getActualStartCalendarId(),
				recommendationInputDTO.getActualStartCalendarId());

		assertEquals("Acutal end calendar id not set", expectedDTO.getActualEndCalendarId(),
				recommendationInputDTO.getActualEndCalendarId());

		assertEquals("Start calendar id not set", expectedDTO.getStartCalendarId(),
				recommendationInputDTO.getStartCalendarId());

		assertEquals("End calendar id not set", expectedDTO.getEndCalendarId(),
				recommendationInputDTO.getEndCalendarId());
		/*assertEquals("Base incorrect", expectedDTO.getBaseWeek(),
				recommendationInputDTO.getBaseWeek());*/

		logger.info("*************************************************************");
	}

	/**
	 * 4. For X weeks recommendation (past) use the “Recommendation Start Week” and “Recommendation End Week”, “No of Weeks in
	 * Advance” to identity the base week
	 * 
	 * @throws GeneralException
	 */
	
	@Test
	public void case4_XweeksPast() throws GeneralException {

		logger.info("case4_XweeksPast() - X Weeks Past");

		RecommendationWeek recommendationWeek = new RecommendationWeek(null, null);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper
				.getRecommendationInputDTO("01/07/2018", "02/03/2018", null, null, PRConstants.MW_X_WEEKS_RECOMMENDATION, 2);

		RecommendationInputHelper.calDatails.put("01/07/2018", RecommendationInputHelper.getCalendarId(2001));
		RecommendationInputHelper.calDatails.put("02/03/2018", RecommendationInputHelper.getCalendarId(2002));

		recommendationWeek.setupRecommendationWeeks(recommendationInputDTO, RecommendationInputHelper.calDatails);
		
		recommendationWeek.logRecWeeksDetail(recommendationInputDTO);

		RecommendationInputDTO expectedDTO = new RecommendationInputDTO();

		expectedDTO.setActualEndCalendarId(2002);
		expectedDTO.setActualStartCalendarId(2001);
		expectedDTO.setStartCalendarId(2001);
		expectedDTO.setEndCalendarId(2002);
		expectedDTO.setBaseWeek("12/24/2017");
		
		assertEquals("Acutal start calendar id not set", expectedDTO.getActualStartCalendarId(),
				recommendationInputDTO.getActualStartCalendarId());

		assertEquals("Acutal end calendar id not set", expectedDTO.getActualEndCalendarId(),
				recommendationInputDTO.getActualEndCalendarId());

		assertEquals("Start calendar id not set", expectedDTO.getStartCalendarId(),
				recommendationInputDTO.getStartCalendarId());

		assertEquals("End calendar id not set", expectedDTO.getEndCalendarId(),
				recommendationInputDTO.getEndCalendarId());
		assertEquals("Base incorrect", expectedDTO.getBaseWeek(),
				recommendationInputDTO.getBaseWeek());

		logger.info("*************************************************************");

	}

	/**
	 * 5. For future on-going quarter – use Quarter Start Week and Quarter End Week and “Recommendation Start Week” and
	 * “Recommendation End Week”, “No of Weeks in Advance” to identity the base week. 
	 * 
	 * @throws GeneralException
	 */
	@Test
	public void case5_XweeksOnGoingQuarterNoStartWeek() throws GeneralException {

		logger.info("case5_XweeksOnGoingQuarter() - X Weeks on going quarter. No start week");

		RecommendationWeek recommendationWeek = new RecommendationWeek(null, null);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper
				.getRecommendationInputDTO(null, null, "03/18/2018", "05/19/2018", PRConstants.MW_X_WEEKS_RECOMMENDATION, 2);

		RecommendationInputHelper.calDatails.put("03/18/2018", RecommendationInputHelper.getCalendarId(2001));
		RecommendationInputHelper.calDatails.put("05/19/2018", RecommendationInputHelper.getCalendarId(2002));
		RecommendationInputHelper.calDatails.put("04/29/2018", RecommendationInputHelper.getCalendarId(2003));
		
		
		recommendationWeek.setupRecommendationWeeks(recommendationInputDTO, RecommendationInputHelper.calDatails);
		
		recommendationWeek.logRecWeeksDetail(recommendationInputDTO);

		RecommendationInputDTO expectedDTO = new RecommendationInputDTO();

		expectedDTO.setActualEndCalendarId(2002);
		expectedDTO.setActualStartCalendarId(2001);
		expectedDTO.setStartCalendarId(2003);
		expectedDTO.setEndCalendarId(2002);
		//expectedDTO.setBaseWeek("04/15/2018");
		
		assertEquals("Acutal start calendar id not set", expectedDTO.getActualStartCalendarId(),
				recommendationInputDTO.getActualStartCalendarId());

		assertEquals("Acutal end calendar id not set", expectedDTO.getActualEndCalendarId(),
				recommendationInputDTO.getActualEndCalendarId());

		assertEquals("Start calendar id not set", expectedDTO.getStartCalendarId(),
				recommendationInputDTO.getStartCalendarId());

		assertEquals("End calendar id not set", expectedDTO.getEndCalendarId(),
				recommendationInputDTO.getEndCalendarId());
		/*assertEquals("Base incorrect", expectedDTO.getBaseWeek(),
				recommendationInputDTO.getBaseWeek());*/

		logger.info("*************************************************************");

	}

	
	/**
	 * 6. Extension of case 5, If “Recommendation Start Week” and “Recommendation End Week” is not mentioned, then identify it
	 * using “No of Weeks in Advance” from current week
	 * 
	 * @throws GeneralException
	 */
	
	@Test
	public void case6_XweeksOnGoingQuarterWithStartWeek() throws GeneralException {

		logger.info("case6_XweeksOnGoingQuarterWithStartWeek() - X Weeks on going quarter. No start week");

		RecommendationWeek recommendationWeek = new RecommendationWeek(null, null);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper
				.getRecommendationInputDTO("05/06/2018", "05/19/2018", "03/18/2018", "05/19/2018", PRConstants.MW_X_WEEKS_RECOMMENDATION, 2);

		RecommendationInputHelper.calDatails.put("03/18/2018", RecommendationInputHelper.getCalendarId(2001));
		RecommendationInputHelper.calDatails.put("05/19/2018", RecommendationInputHelper.getCalendarId(2002));
		RecommendationInputHelper.calDatails.put("05/06/2018", RecommendationInputHelper.getCalendarId(2003));
		
		
		recommendationWeek.setupRecommendationWeeks(recommendationInputDTO, RecommendationInputHelper.calDatails);
		
		recommendationWeek.logRecWeeksDetail(recommendationInputDTO);

		RecommendationInputDTO expectedDTO = new RecommendationInputDTO();

		expectedDTO.setActualEndCalendarId(2002);
		expectedDTO.setActualStartCalendarId(2001);
		expectedDTO.setStartCalendarId(2003);
		expectedDTO.setEndCalendarId(2002);
		expectedDTO.setBaseWeek("04/22/2018");
		
		assertEquals("Acutal start calendar id not set", expectedDTO.getActualStartCalendarId(),
				recommendationInputDTO.getActualStartCalendarId());

		assertEquals("Acutal end calendar id not set", expectedDTO.getActualEndCalendarId(),
				recommendationInputDTO.getActualEndCalendarId());

		assertEquals("Start calendar id not set", expectedDTO.getStartCalendarId(),
				recommendationInputDTO.getStartCalendarId());

		assertEquals("End calendar id not set", expectedDTO.getEndCalendarId(),
				recommendationInputDTO.getEndCalendarId());
		assertEquals("Base incorrect", expectedDTO.getBaseWeek(),
				recommendationInputDTO.getBaseWeek());

		logger.info("*************************************************************");
	}
	
	/**
	 * 6. For remaining weeks in the past quarter – use Quarter Start Week and Quarter End Week and “Recommendation Start Week”
	 * and “Recommendation End Week”, “No of Weeks in Advance” to identity the base week
	 * 
	 * @throws GeneralException
	 */
	@Test
	public void case7_PastQuarterRemainingWeeks() throws GeneralException {

		logger.info("case7_PastQuarterRemainingWeeks() - Past Quarter. Remaining weeks");

		RecommendationWeek recommendationWeek = new RecommendationWeek(null, null);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper
				.getRecommendationInputDTO("10/29/2017", "01/06/2018", "10/08/2017", "01/06/2018", PRConstants.MW_X_WEEKS_RECOMMENDATION, 2);
		
		RecommendationInputHelper.calDatails.put("10/08/2017", RecommendationInputHelper.getCalendarId(2001));
		RecommendationInputHelper.calDatails.put("01/06/2018", RecommendationInputHelper.getCalendarId(2002));
		RecommendationInputHelper.calDatails.put("10/29/2017", RecommendationInputHelper.getCalendarId(2003));
		
		
		recommendationWeek.setupRecommendationWeeks(recommendationInputDTO, RecommendationInputHelper.calDatails);
		
		recommendationWeek.logRecWeeksDetail(recommendationInputDTO);

		RecommendationInputDTO expectedDTO = new RecommendationInputDTO();

		expectedDTO.setActualEndCalendarId(2002);
		expectedDTO.setActualStartCalendarId(2001);
		expectedDTO.setStartCalendarId(2003);
		expectedDTO.setEndCalendarId(2002);
		expectedDTO.setBaseWeek("10/15/2017");
		
		assertEquals("Acutal start calendar id not set", expectedDTO.getActualStartCalendarId(),
				recommendationInputDTO.getActualStartCalendarId());

		assertEquals("Acutal end calendar id not set", expectedDTO.getActualEndCalendarId(),
				recommendationInputDTO.getActualEndCalendarId());

		assertEquals("Start calendar id not set", expectedDTO.getStartCalendarId(),
				recommendationInputDTO.getStartCalendarId());

		assertEquals("End calendar id not set", expectedDTO.getEndCalendarId(),
				recommendationInputDTO.getEndCalendarId());
		assertEquals("Base incorrect", expectedDTO.getBaseWeek(),
				recommendationInputDTO.getBaseWeek());

		logger.info("*************************************************************");
	}
}