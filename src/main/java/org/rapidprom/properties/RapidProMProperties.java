package org.rapidprom.properties;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Load RapidProM properties such as location of ProM packages etc.
 *
 * @author svzelst
 */
public class RapidProMProperties {

	private static String RAPIDPROM_PROPERTIES_FILE = "/org/rapidprom/resources/rapidprom.properties";
	private static RapidProMProperties instance = null;
	private final Properties properties;

	private Deployment deployment = null;

	public enum Deployment {
		DEVELOPMENT, LIVE;
	}

	private RapidProMProperties() {
		properties = setup();
	}

	public static RapidProMProperties instance() {
		if (instance == null) {
			instance = new RapidProMProperties();
		}
		return instance;
	}

	private Properties setup() {
		Properties properties = new Properties();
		InputStream propertiesIS = RapidProMProperties.class
				.getResourceAsStream(RAPIDPROM_PROPERTIES_FILE);
		try {
			properties.load(propertiesIS);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return properties;
	}

	public Properties getProperties() {
		return properties;
	}

	public Deployment getDeployment() {
		if (deployment == null) {
			String deploymentProp = properties
					.getProperty("deployment");
			if (deploymentProp != null) {
				if (deploymentProp.equals("live")) {
					deployment = Deployment.LIVE;
				} else {
					deployment = Deployment.DEVELOPMENT;
				}
			}
		}
		return deployment;
	}

	public String getExtensionName() {
		return properties.getProperty("extension.name");
	}

}
