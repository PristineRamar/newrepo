/*
 * Author : Suresh 
 * Start Date : Jul 22, 2009
 * 
 * Change Description 			Changed By 				Date
 * --------------------------------------------------------------
 */

package com.pristine.exception;

import org.apache.log4j.Logger;

@SuppressWarnings("serial")
public class GeneralException extends Throwable
{
	private String	message;
	static Logger	logger	= Logger.getLogger(GeneralException.class);
	public static int LEVEL_SEVERE = 3;
	public static int LEVEL_WARNING = 2;
	
	private int errorLevel = LEVEL_WARNING;

	public int getErrorLevel() {
		return errorLevel;
	}

	public void setErrorLevel(int errorLevel) {
		this.errorLevel = errorLevel;
	}

	public GeneralException(String msg, Throwable t)
	{
		super(msg, t);

		this.message = msg;
	}
	
	public GeneralException(String msg, Throwable t, int errLevel)
	{
		super(msg, t);

		errorLevel = errLevel;
		this.message = msg;
	}


	public GeneralException(String msg)
	{
		super(msg);

		this.message = msg;
	}
	
	public GeneralException(String msg, int errLevel)
	{
		super(msg);

		this.message = msg;
		errorLevel = errLevel;
	}

	public void logException(Logger log)
	{
		log.error(message, this);
	}
}