package org.rapidprom.ioobjects;

import java.util.Set;

import org.processmining.framework.plugin.PluginContext;
import org.rapidprom.ioobjects.abstr.AbstractRapidProMIOObject;

public class SetStringIOObject extends AbstractRapidProMIOObject<Set<String>> {

	private static final long serialVersionUID = 1983539554210072486L;

	public SetStringIOObject(Set<String> t, PluginContext context) {
		super(t, context);
	}
}

