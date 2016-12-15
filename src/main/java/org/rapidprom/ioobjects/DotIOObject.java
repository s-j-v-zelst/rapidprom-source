package org.rapidprom.ioobjects;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.plugins.graphviz.dot.Dot;
import org.rapidprom.ioobjects.abstr.AbstractRapidProMIOObject;

public class DotIOObject extends AbstractRapidProMIOObject<Dot> {

	private static final long serialVersionUID = -4574922526705299348L;

	public DotIOObject(Dot t, PluginContext context) {
		super(t, context);
	}

}