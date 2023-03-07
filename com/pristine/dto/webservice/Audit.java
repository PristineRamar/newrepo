package com.pristine.dto.webservice;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Audit {
	public String strategyId;
	public String predictedUserId;
	
	public String locationLevelId;
	public String locationId;
	public String productLevelId;
	public String productId;
	
	public String reportId;
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		if(strategyId != null)
			sb.append("Strategy Id " + strategyId + "\t" + "Predicted user Id " + predictedUserId);
		if(locationId != null){
			sb.append("Location Level Id " + locationLevelId + "\t");
			sb.append("Location Id " + locationId + "\t");
			sb.append("Product Level Id " + productLevelId + "\t");
			sb.append("Product Id " + productId + "\t");
			sb.append("Predicted User Id " + predictedUserId);
		}	
		return sb.toString();
	}
}
