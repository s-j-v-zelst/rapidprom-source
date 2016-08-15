package org.rapidprom.ioobjects;

import org.deckfour.xes.model.XLog;
import org.processmining.framework.plugin.PluginContext;
import org.rapidprom.ioobjects.abstr.AbstractRapidProMIOObject;

public class XLogIOObject extends AbstractRapidProMIOObject<XLog> {

	private static final long serialVersionUID = -1323690731245887615L;

	public XLogIOObject(XLog t, PluginContext context) {
		super(t, context);
	}

}
