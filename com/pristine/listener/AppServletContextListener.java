package com.pristine.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.util.offermgmt.PRCacheManager;
import com.pristine.util.PropertyManager;

public class AppServletContextListener implements ServletContextListener{
	static Logger logger	= Logger.getLogger(AppServletContextListener.class);
	
	@Override
	public void contextDestroyed(ServletContextEvent contextEvent) {
		PRCacheManager.shutdown();
//		com.pristine.util.offermgmt.promotion.PRCacheManager.shutdown();
	}

	@Override
	public void contextInitialized(ServletContextEvent contextEvent) {
		System.out.println("Inside contextInitialized");
		String configPath = contextEvent.getServletContext().getInitParameter("configPath");
		System.out.println("Property File at - " + configPath + "recommendation.properties");
		PropertyManager.initialize(configPath + "recommendation.properties");
		PropertyConfigurator.configure(configPath + "log4j-pricing-engine.properties");
		PRCacheManager.initialize();
//		com.pristine.util.offermgmt.promotion.PRCacheManager.initialize();
	}

}
