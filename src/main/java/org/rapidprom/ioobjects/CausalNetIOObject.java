package org.rapidprom.ioobjects;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.models.cnet.CausalNet;
import org.rapidprom.ioobjects.abstr.AbstractRapidProMIOObject;

public class CausalNetIOObject extends AbstractRapidProMIOObject<CausalNet>{

	private static final long serialVersionUID = -6715830623930619256L;
	
	public CausalNetIOObject(CausalNet t, PluginContext context) {
		super(t, context);
	}
	
}
