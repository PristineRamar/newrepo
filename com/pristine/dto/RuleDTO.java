/*
 * Author : Naimish Start Date : Jul 22, 2009
 * 
 * Change Description Changed By Date
 * --------------------------------------------------------------
 */

package com.pristine.dto;

public class RuleDTO
{
	public int getId() 			{ return id; }
	public void setId(int v) 	{id = v; }
	private int id;

	public String getName()			{ return name; }
	public void setName(String v)	{ name = v; }
	private String name;

	public String getDescription()			{ return description; }
	public void setDescription(String v)	{ description = v; }
	private String description;

	public boolean isEnabled()			{ return isEnabled; }
	public void setEnabled(boolean v)	{ isEnabled = v; }
	private boolean	isEnabled;

	public String getFunctionalArea()			{ return functionalArea; }
	public void setFunctionalArea(String v)	{ functionalArea = v; }
	private String functionalArea;
	
	public int getPriority() 		{ return priority; }
	public void setPriority(int v) 	{priority = v; }
	private int priority;
}
