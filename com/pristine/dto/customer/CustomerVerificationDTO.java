package com.pristine.dto.customer;

import java.util.Date;

public class CustomerVerificationDTO
{
	public Date		reportDate = null;
	public Date		predictionWeekDate = null;
	
	public int		customerId = -1;
	public String	customerNo = null;
	public int		storeId = -1;
	
	public int		itemCode = -1;
	public int		lirId = -1;
	public int		segmentId = -1;
	
	public boolean	enabled = false;
	public boolean	predicted = false;
	public boolean	purchasedItemIn53rdWeek = false;
	public boolean	purchasedLIGIn53rdWeek = false;
	public boolean	purchasedSegmentIn53rdWeek = false;
}
