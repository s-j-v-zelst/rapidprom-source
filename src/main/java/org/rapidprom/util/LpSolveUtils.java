package org.rapidprom.util;

import org.apache.wicket.util.file.File;

import com.rapidminer.tools.SystemInfoUtilities.JVMArch;
import com.rapidminer.tools.SystemInfoUtilities.OperatingSystem;

public class LpSolveUtils {

	private static final String BASE_PATH = "lpsolve" + File.separator + "resources" + File.separator;

	public static final String LPSOLVE_JAR = "lpsolve55j.jar";
	public static final String BINARIES_FOLDER = "lib";

	/**
	 * Returns the path to the binaries and jar file of LpSolve, based on OS and
	 * JVM Architecture. To get the jar file, use: String jar =
	 * getOSBasedLpSolvePath + LPSOLVE_JAR;.
	 * 
	 * @param os
	 * @param jvmArch
	 * @return
	 */
	public static String getOSBasedLpSolvePath(OperatingSystem os, JVMArch jvmArch) {
		return BASE_PATH + getOSFolder(os, jvmArch);
	}

	public static String getOSFolder(OperatingSystem os, JVMArch jvmArch) {
		String result = "";
		switch (os) {
		case OSX:
			result = "mac" + jvmArchToString(jvmArch) + File.separator;
			break;
		case WINDOWS:
			result = "win" + jvmArchToString(jvmArch) + File.separator;
			break;
		case OTHER:
		case SOLARIS:
		case UNIX:
		default:
			result = "ux" + jvmArchToString(jvmArch) + File.separator;
			break;
		}
		return result;
	}

	public static String jvmArchToString(JVMArch jvmArch) {
		String res = "";
		switch (jvmArch) {
		case SIXTY_FOUR:
			res = "64";
			break;
		case THIRTY_TWO:
		default:
			res = "32";
			break;
		}
		return res;
	}
}
