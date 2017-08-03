package org.rapidprom.ioobjects;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.pnanalysis.models.PetriNetMetrics;
import org.rapidprom.ioobjects.abstr.AbstractRapidProMIOObject;

public class PetriNetMetricsIOObejct extends AbstractRapidProMIOObject<PetriNetMetrics> {

	private static final long serialVersionUID = -7292084286139591411L;

	public PetriNetMetricsIOObejct(PetriNetMetrics t, PluginContext context) {
		super(t, context);
	}

}
