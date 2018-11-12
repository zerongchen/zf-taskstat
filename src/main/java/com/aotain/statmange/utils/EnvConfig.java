package com.aotain.statmange.utils;


import lombok.Getter;
import lombok.Setter;

import java.io.*;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

@Setter
@Getter
public class EnvConfig {
		
	private String driverClass;
	private String url;
	private String username;
	private String password;


	private static EnvConfig instance;

	private EnvConfig() {
		try {
			String proFilePath = System.getProperty("ZF_HOME")+ File.separator+"config"+File.separator+"config.properties";
			InputStream in = new BufferedInputStream(new FileInputStream(
					proFilePath));

			ResourceBundle config = new PropertyResourceBundle(in);
			driverClass = config.getString("jdbc.driverClass");
			url = config.getString("jdbc.url");
			username = config.getString("jdbc.username");
			password = config.getString("jdbc.password");

			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public synchronized static EnvConfig getInstance() {

		if (instance == null) {
			instance = new EnvConfig();
		}
		return instance;
	}

	public String getDriverClass() {
		return driverClass;
	}

	public String getUrl() {
		return url;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}
}
