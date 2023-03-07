package com.pristine.util;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.pristine.exception.GeneralException;

/*
 * Added by kirthi
 * Utility to transfer file to client's server via SFTP
 */
public class SFTPFileTransferUtil {

	private static Logger logger = Logger.getLogger("SFTPFileTransferUtil");

	public boolean transferExportedFileThroughSFTP(String sourceDirectory) throws GeneralException {
	
		PropertyManager.initialize("recommendation.properties");

		String SFTPHOST = PropertyManager.getProperty("SFTPHOST");
		int SFTPPORT = 22;
		String SFTPUSER = PropertyManager.getProperty("SFTPUSER");
		String SFTPPASS = PropertyManager.getProperty("SFTPPASS");
		String SFTPDESTDIR = PropertyManager.getProperty("SFTPDESTINATIONDIR");

		//String renameBln = PropertyManager.getProperty("RENAME_SFTP_FILE");
		//boolean renameRequire = Boolean.parseBoolean(renameBln);
		boolean fileTransferred = false;

		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MMddyyyy");
		LocalDateTime now = LocalDateTime.now();
		
		Session session = null;
		// Channel channel = null;
		ChannelSftp channelSftp = null;
		logger.info("Preparing the host information for sftp.");
		try {
			JSch jsch = new JSch();
			session = jsch.getSession(SFTPUSER, SFTPHOST/* , SFTPPORT */);
			session.setPassword(SFTPPASS);
			Properties config = new Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);
			session.connect();
			logger.info("Host connected.");
			channelSftp = (ChannelSftp) session.openChannel("sftp");
			channelSftp.connect();
			logger.info("SFTP Channel Opened and Connected.");
			
			File directoryPath = new File(sourceDirectory);
		    File contents[] = directoryPath.listFiles();
		    
			if (contents.length > 0) {
				//make date folder under SFTPDESTDIR
				//channelSftp.mkdir(SFTPDESTDIR + "//" + dtf.format(now));
				for (File file : contents) {					
					
					try {
						// To Transfer the file
						channelSftp.put(sourceDirectory + "/" + file.getName(),
								SFTPDESTDIR + "//" + file.getName());
						logger.info(
								"File transfered successfully to host - From: " + sourceDirectory + "/" + file.getName()
										+ " To: " + SFTPDESTDIR + "//" + file.getName());

						fileTransferred = true;
					} catch (Exception e) {
						fileTransferred = false;
						logger.error("File Failed to trasfer in SFTP");
					}
					// To check the file is tranferred
					/*Vector filesTransList = channelSftp.ls(SFTPDESTDIR + "//" + dtf.format(now) + "//");
					for (int i = 0; i < filesTransList.size(); i++) {
						ChannelSftp.LsEntry lsEntry = (LsEntry) filesTransList.get(i);
						if (file.getName().equals(lsEntry.getFilename())) {
							fileTransferred = true;
							break;
						}
					}*/

					// To rename the file only if file transferred
					/*if (fileTransferred && renameRequire) {
						channelSftp.rename(SFTPDESTDIR + "//" + file.getName(),
								SFTPDESTDIR + "//" + fileName +"_"+ dtf.format(now) + ".zip");
						logger.info("File renamed with date suffix. ");
					}*/
				}
			}
			else {
				logger.info("transferExportedFileThroughSFTP() - No files in " +sourceDirectory+" to transfer via SFTP");
			}
			

		} catch (Exception ex) {
			logger.error("Exception found while tranfer the response - " + ex.toString());
			ex.printStackTrace();
			throw new GeneralException("Exception found while tranfer the response - " + ex.toString());
		} finally {
			channelSftp.exit();
			logger.info("SFTP Channel exited.");
			channelSftp.disconnect();
			// channel.disconnect();
			logger.info("SFTPChannel disconnected.");

			session.disconnect();
			logger.info("Host Session disconnected.");
		}
		return fileTransferred;
	}
}
