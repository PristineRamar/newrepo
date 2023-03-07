package com.pristine.test.offermgmt;

//import org.apache.log4j.PropertyConfigurator;
import org.junit.*;

import com.pristine.util.PropertyManager;

public class ApplyStrategyJUnitTest {

	public static final Integer COST_INCREASE = 1;
	public static final Integer COST_DECREASE = -1;
	public static final Integer COST_NO_CHANGE = 0;
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	public static final Integer ITEM_CODE_TEST_1234 = 1234;
	public static final Integer REALTED_ITEM_CODE_BRAND_TEST_1245 = 12345;
	public static final Integer REALTED_ITEM_CODE_SIZE_TEST_1246 = 12346;
	
	@Before
    public void init() {
//		PropertyConfigurator.configure("log4j-pricing-engine.properties");
		PropertyManager.initialize("recommendation.properties");
    }
	
//Scenarios
	/* Cost Change (No relation) Margin 
	 * 		No conflicts, Only margin is considered
	 * 		Order - (Margin, PI, Multi Comp), (Multi Comp, PI, Margin), (PI, Multi Comp, Margin)
	 * 
	 * Cost Change (National or Size or Both relation) Margin 
	 * 		Conflicts, only margin is considered
	 * 		Order - (Margin, Brand, Size, PI, Multi Comp), (PI, Margin, Brand, Size, Multi Comp)
	 * Cost Change (Store Brand Relation) Store Brand along with Margin, conflicts against store brand
	 * 
	 */
	
	
		
				

				
			 
			
				
				
				
				
				
				
				
				

				 
	 

}
