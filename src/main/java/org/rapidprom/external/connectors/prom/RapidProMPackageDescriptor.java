package org.rapidprom.external.connectors.prom;

import java.util.ArrayList;
import java.util.List;

import org.processmining.framework.packages.PackageDescriptor;

/**
 * Placeholder class for RapidProM. This class defines RapidProM as a "prom"
 * package. This is in fact not the case, though to properly load plugins from
 * the source code, the corresponding PluginManager class needs a
 * PackageDescriptor to register the plugin to.
 * 
 * @author svzelst
 *
 */
public class RapidProMPackageDescriptor extends PackageDescriptor {

	private static final String NAME = "rapidprom";
	//TODO: read this from the gradle.properties file
	private static final String VERSION = "4.0.0";
	//TODO: use a util function that returns the appropriate value here
	private static final OS OS_VAR = OS.ALL;
	private static final String DESCRIPTION = "Placeholder package for RapidProM";
	private static final String ORGANISATION = "Eindhoven University of Technology";
	private static final String AUTHOR = "A. Bolt, S.J. van Zelst";
	private static final String MAINTAINER = "A. Bolt, S.J. van Zelst";
	private static final String LICENSE = "AGPLv3";
	private static final String URL = "http://rapidprom.org";
	private static final String LOGO_URL = "http://www.promtools.org/lib/exe/fetch.php?w=300&tok=d1d68b&media=rapidprom:logo.png";
	private static final String KEYWORDS = "";
	private static final boolean AUTO_INSTALLED = true;
	private static final boolean HAS_PLUGINS = true;
	private static final List<String> DEPENDENCIES = new ArrayList<>();
	private static final List<String> CONFLICTS = new ArrayList<>();

	public RapidProMPackageDescriptor() {
		super(NAME, VERSION, OS_VAR, DESCRIPTION, ORGANISATION, AUTHOR, MAINTAINER, LICENSE, URL, LOGO_URL, KEYWORDS,
				AUTO_INSTALLED, HAS_PLUGINS, DEPENDENCIES, CONFLICTS);
	}

}
