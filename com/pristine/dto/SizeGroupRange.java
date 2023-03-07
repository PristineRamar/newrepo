package com.pristine.dto;

public class SizeGroupRange
{
	public String	_SizeGroup; 
	public double	_Min;
	public double	_Max;
	
	public SizeGroupRange (String sizeGroup)
	{
		_SizeGroup = sizeGroup;
		String[] sizes = sizeGroup.split(",");
		_Min = Double.parseDouble(sizes[0]);
		_Max = Double.parseDouble(sizes[sizes.length-1]);
	}
}
