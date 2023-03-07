package com.pristine.dto.customer;

import java.util.*;

public class CustomerDTO
{
	public int		id;
	public String	customerNo;					// retailer's customer number
	public String	segmentNo = null;			// retailer's customer segment
	public char		gender = 'U';				// 'M' or 'F' ('U' - unknown)
	public int		storeId = -1;
	public int		secondStoreId = -1;
	public int		familyCount;
	public String	shoppingDay = null;
	public int		shoppingDayCount = 0;
	public String	shopping2Day = null;
	public int		shopping2DayCount = 0;
	public String	shopping3Day = null;
	public int		shopping3DayCount = 0;
	public String	shoppingNone2Day = null;
	public int		shoppingNone2DayCount = 0;
	public int		gapInWeeks;
	public int		visitCount = 0;
	public int		totalCount = 0;
	public Date		lastVisitDate = null;
	public int[]	primaryDays = null;
	public double	weeklyAverageRegular = 0;
	public double	weeklyAverageSale = 0;
	public boolean	active = true;
}
