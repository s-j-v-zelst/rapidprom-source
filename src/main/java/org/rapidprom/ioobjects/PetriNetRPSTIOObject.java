package org.rapidprom.ioobjects;

import org.processmining.framework.plugin.PluginContext;
import org.rapidprom.ioobjects.abstr.AbstractRapidProMIOObject;
import org.rapidprom.operators.decomposition.rpst.PetriNetRPST;

public class PetriNetRPSTIOObject extends AbstractRapidProMIOObject<PetriNetRPST> {

	private static final long serialVersionUID = -2713816479218544045L;

	public PetriNetRPSTIOObject(PetriNetRPST rpst, PluginContext context) {
		super(rpst, context);
	}

}