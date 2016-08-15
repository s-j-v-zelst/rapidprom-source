package org.rapidprom.ioobjects;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.processtree.ProcessTree;
import org.rapidprom.ioobjects.abstr.AbstractRapidProMIOObject;

public class ProcessTreeIOObject extends AbstractRapidProMIOObject<ProcessTree> {

	private static final long serialVersionUID = 780816193914598555L;

	public ProcessTreeIOObject(ProcessTree t, PluginContext context) {
		super(t, context);
	}

}
