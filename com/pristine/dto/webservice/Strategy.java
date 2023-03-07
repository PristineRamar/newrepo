package com.pristine.dto.webservice;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Strategy {
	public List<Long> strategyId = new ArrayList<Long>();
	public String predictedUserId;
	public int locationLevelId;
	public int locationId;
	public int productLevelId;
	public int productId;
	public int runOnlyTempStrat;
}
