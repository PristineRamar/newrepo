package com.pristine.dto;

import java.util.HashMap;

public class RuleDTOList
{
	public RuleDTOList ()
	{
		rulesMap = new HashMap<Integer, RuleDTO>();
	}
	
	public void add (RuleDTO rule)
	{
		rulesMap.put(rule.getId(), rule);
	}
	
	public RuleDTO getRule (int ruleId)
	{
		return rulesMap.get(ruleId);
	}

	public boolean isEnabled (int ruleId)
	{
		RuleDTO rule = rulesMap.get(ruleId);
		return rule != null ? rule.isEnabled() : false; 
	}

	//
	// Getters/Setters
	//
	
	//
	// Data members
	//
	private HashMap<Integer, RuleDTO>	rulesMap = null;
}
