package org.rapidprom.ioobjects;

import org.deckfour.xes.model.XLog;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.xesalignmentextension.XAlignmentExtension;
import org.processmining.xesalignmentextension.XAlignmentExtension.XAlignedLog;

public class XAlignedLogIOObject extends XLogIOObject {

	private static final long serialVersionUID = 8660803862380246121L;

	public XAlignedLogIOObject(XLog log, PluginContext context) {
		super(log, context);
		if (XAlignmentExtension.instance().extractFitness(log) == null) {
			throw new IllegalArgumentException(
					"Trying to instanciate an XAlignedLogIOObject with a XLog without alignment information!");
		}
	}

	public XAlignedLog getAsAlignedLog() {
		return XAlignmentExtension.instance().extendLog(getArtifact());
	}

}