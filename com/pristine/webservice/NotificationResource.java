package com.pristine.webservice;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dto.offermgmt.NotificationDetailInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.NotificationService;

@Path("/notificationResource")
public class NotificationResource extends BaseResource {
	private static Logger logger = Logger.getLogger("NotificationResource");

	public NotificationResource() throws GeneralException {
		setLog4jProperties();
	}
	
	@Path("/addNotifications")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String addNotifications(String notificationDetails) {
		int notificationStatus = 0;
		ObjectMapper mapper = new ObjectMapper();
		NotificationService notificationService =  new NotificationService();
		try {
			List<NotificationDetailInputDTO> notificationDetailUIDTOs = mapper.readValue(notificationDetails,
					new TypeReference<List<NotificationDetailInputDTO>>() {});
			notificationStatus = notificationService.addNotificationsUI(notificationDetailUIDTOs);
		} catch (IOException | SQLException | GeneralException e) {
			notificationStatus = 1;
			logger.error("Exception in addNotifications()");
			e.printStackTrace();
		}
		return String.valueOf(notificationStatus);
	}
}
