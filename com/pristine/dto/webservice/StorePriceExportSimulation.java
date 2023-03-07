/**
 * 
 */
package com.pristine.dto.webservice;

import javax.xml.bind.annotation.XmlRootElement;


/**
 * This class is used to store the data passed between 
 * the StorePriceExport program and the front end.
 */
@XmlRootElement
public class StorePriceExportSimulation {
	
	public String effectiveDate;
	public String exportType;
	public String salesFloorItemLimit;
	public String localTime;

}
