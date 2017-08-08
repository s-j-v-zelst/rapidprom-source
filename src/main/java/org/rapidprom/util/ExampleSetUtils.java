package org.rapidprom.util;

import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.MDInteger;

public class ExampleSetUtils {

	public static ExampleSetMetaData constructExampleSetMetaData(final ExampleSetMetaData metaData,
			final String[] names, final int[] types, final String[] roles, final MDInteger[] missing) {
		for (int i = 0; i < names.length; i++) {
			AttributeMetaData amd = new AttributeMetaData(names[i], types[i]);
			amd.setRole(roles[i]);
			amd.setNumberOfMissingValues(missing[i]);
			metaData.addAttribute(amd);
		}
		return metaData;
	}
}
