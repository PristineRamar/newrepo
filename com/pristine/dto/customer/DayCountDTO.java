package com.pristine.dto.customer;

public class DayCountDTO
{
	public DayCountDTO () {}
	
	public DayCountDTO (DayCountDTO dto)
	{
		day = dto.day;
		count = dto.count;
		spend = dto.spend;
	}
	
	//public String customerNo = null;
	public int		day		= 0;
	public int		count	= 0;
	public double	spend 	= 0;
}
