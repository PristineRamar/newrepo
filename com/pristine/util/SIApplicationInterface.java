package com.pristine.util;

import java.sql.Connection;

import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.RulesDAO;

public interface SIApplicationInterface
{
	public int getNumberOfRegularDays ();
	public int getNumberOfSaleDays ();
	public Connection getConnection ();
	public RulesDAO getRulesDB ();
	public CompetitiveDataDAO getCompetitiveDataDB ();
	public int getScheduleId ();
}
