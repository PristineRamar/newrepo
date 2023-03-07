package com.pristine.dto.customer;

import java.util.Date;

public class CustomerPurchaseDTO
{
	public Date		reportDate = null;
	
	public int		customerId = -1;
	public int		storeId = -1;
	public Date		lastPurchaseDate = null;
	
	public int		itemCode = -1;
	public int		lirId = -1;
	public int		segmentId = -1;
	public int		brandId = -1;
	
	public int		probabilityCount = 0;
	public int		totalCount = 0;
	public double	quantity = 0;
	
	public boolean	enabled = false;
	public boolean	predicted = false;
}
