package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//import org.apache.log4j.Logger;
//import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

//import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.StrategyWhatIfService;

public class StrategyWhatIfJUnitTest {
//	private static Logger logger = Logger.getLogger("StrategyWhatIfJUnitTest");
	ObjectMapper mapper = new ObjectMapper();
	
	@Before
    public void init() {
//		PropertyConfigurator.configure("log4j-pricing-engine.properties");		 
    }
	
	/**
	 * There are 2 strategies already defined for a category. 
	 * one is at category level, other at check list level
	 * User overrides both of the strategies
	 * StrategyMap must have overridden strategies
	 * @throws Exception
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase1() throws Exception, OfferManagementException {
		StrategyWhatIfService strategyWhatIfService = new StrategyWhatIfService();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		List<PRStrategyDTO> whatIfStrategies = new ArrayList<PRStrategyDTO>();
		StrategyKey strategyKey = new StrategyKey(6, 6, 4, 1423);
		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		
		//Category level strategy
		strategies.add(TestHelper.getStrategy(1000, 6, 6, 4, 1423, "", "", false, 0, 0, 0));
		
		//Check list level strategy
		strategies.add(TestHelper.getStrategy(1001, 6, 6, 4, 1423, "", "", false, 10, 0, 0));
		
		strategyMap.put(strategyKey, strategies);
		
		// Category level strategy modified in the what-if
		whatIfStrategies.add(TestHelper.getStrategy(2000, 6, 6, 4, 1423, "", "", false, 0, 0, 0));
		
		// Check list level strategy modified in the what-if
		whatIfStrategies.add(TestHelper.getStrategy(2001, 6, 6, 4, 1423, "", "", false, 10, 0, 0));
		
		strategyWhatIfService.replaceStrategies(strategyMap, whatIfStrategies);
		
		String expStrategyId = "-2000-2001", actualStrategyId = "";

		for (List<PRStrategyDTO> strategyList : strategyMap.values()) {
			for (PRStrategyDTO strategy : strategyList) {
				actualStrategyId = actualStrategyId + "-" + strategy.getStrategyId();
			}
		}
		assertEquals("Not Matching", actualStrategyId, expStrategyId);
	}
	
	/**
	 * There is 1 strategies already defined for a category. 
	 * at category level
	 * User overrides that strategy and add one more at check list level
	 * StrategyMap must have overridden cat strategy and added
	 * check list strategy
	 * @throws Exception
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase2() throws Exception, OfferManagementException {
		StrategyWhatIfService strategyWhatIfService = new StrategyWhatIfService();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		List<PRStrategyDTO> whatIfStrategies = new ArrayList<PRStrategyDTO>();
		StrategyKey strategyKey = new StrategyKey(6, 6, 4, 1423);
		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		
		//Category level strategy
		strategies.add(TestHelper.getStrategy(1000, 6, 6, 4, 1423, "", "", false, 0, 0, 0));
		
		strategyMap.put(strategyKey, strategies);
		
		// Category level strategy modified in the what-if
		whatIfStrategies.add(TestHelper.getStrategy(2000, 6, 6, 4, 1423, "", "", false, 0, 0, 0));
		
		// Check list level strategy modified in the what-if
		whatIfStrategies.add(TestHelper.getStrategy(2001, 6, 6, 4, 1423, "", "", false, 10, 0, 0));
		
		strategyWhatIfService.replaceStrategies(strategyMap, whatIfStrategies);
		
		String expStrategyId = "-2000-2001", actualStrategyId = "";

		for (List<PRStrategyDTO> strategyList : strategyMap.values()) {
			for (PRStrategyDTO strategy : strategyList) {
				actualStrategyId = actualStrategyId + "-" + strategy.getStrategyId();
			}
		}
		assertEquals("Not Matching", actualStrategyId, expStrategyId);
	}
	
	/**
	 * There are 2 strategies already defined for a category. 
	 * one is at category level, other at check list level
	 * User is not overriding any strategies
	 * StrategyMap must have existing strategies
	 * @throws Exception
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase3() throws Exception, OfferManagementException {
		StrategyWhatIfService strategyWhatIfService = new StrategyWhatIfService();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		List<PRStrategyDTO> whatIfStrategies = new ArrayList<PRStrategyDTO>();
		StrategyKey strategyKey = new StrategyKey(6, 6, 4, 1423);
		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		
		//Category level strategy
		strategies.add(TestHelper.getStrategy(1000, 6, 6, 4, 1423, "", "", false, 0, 0, 0));
		
		//Check list level strategy
		strategies.add(TestHelper.getStrategy(1001, 6, 6, 4, 1423, "", "", false, 10, 0, 0));
		
		strategyMap.put(strategyKey, strategies);
		
		strategyWhatIfService.replaceStrategies(strategyMap, whatIfStrategies);
		
		String expStrategyId = "-1000-1001", actualStrategyId = "";

		for (List<PRStrategyDTO> strategyList : strategyMap.values()) {
			for (PRStrategyDTO strategy : strategyList) {
				actualStrategyId = actualStrategyId + "-" + strategy.getStrategyId();
			}
		}
		assertEquals("Not Matching", actualStrategyId, expStrategyId);
	}
	
	/**
	 * There are 2 strategies already defined for a category. 
	 * one is at category level, other at check list level
	 * User overrides both of the strategies, add one more check list
	 * StrategyMap must have overridden strategies
	 * @throws Exception
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase4() throws Exception, OfferManagementException {
		StrategyWhatIfService strategyWhatIfService = new StrategyWhatIfService();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		List<PRStrategyDTO> whatIfStrategies = new ArrayList<PRStrategyDTO>();
		StrategyKey strategyKey = new StrategyKey(6, 6, 4, 1423);
		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		
		//Category level strategy
		strategies.add(TestHelper.getStrategy(1000, 6, 6, 4, 1423, "", "", false, 0, 0, 0));
		
		//Check list level strategy
		strategies.add(TestHelper.getStrategy(1001, 6, 6, 4, 1423, "", "", false, 10, 0, 0));
		
		strategyMap.put(strategyKey, strategies);
		
		// Category level strategy modified in the what-if
		whatIfStrategies.add(TestHelper.getStrategy(2000, 6, 6, 4, 1423, "", "", false, 0, 0, 0));
		
		// Check list level strategy modified in the what-if
		whatIfStrategies.add(TestHelper.getStrategy(2001, 6, 6, 4, 1423, "", "", false, 10, 0, 0));
		
		// New Check list level in what-if
		whatIfStrategies.add(TestHelper.getStrategy(2002, 6, 6, 4, 1423, "", "", false, 20, 0, 0));
		
		strategyWhatIfService.replaceStrategies(strategyMap, whatIfStrategies);
		
		String expStrategyId = "-2000-2001-2002", actualStrategyId = "";

		for (List<PRStrategyDTO> strategyList : strategyMap.values()) {
			for (PRStrategyDTO strategy : strategyList) {
				actualStrategyId = actualStrategyId + "-" + strategy.getStrategyId();
			}
		}
		assertEquals("Not Matching", actualStrategyId, expStrategyId);
	}
}
