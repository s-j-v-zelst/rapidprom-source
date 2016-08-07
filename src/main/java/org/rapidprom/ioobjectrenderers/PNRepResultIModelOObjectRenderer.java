package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.plugins.pnalignanalysis.visualization.projection.PNLogReplayProjectedVisPanel;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.PNRepResultIOObject;

import javassist.tools.rmi.ObjectNotFoundException;

public class PNRepResultIModelOObjectRenderer extends AbstractRapidProMIOObjectRenderer<PNRepResultIOObject> {

	@Override
	public String getName() {
		return "PNRepResult (Project on Model) renderer";
	}

	@Override
	protected JComponent runVisualization(PNRepResultIOObject ioObject) {
		try {
			return new PNLogReplayProjectedVisPanel(ioObject.getPluginContext(), ioObject.getPn().getArtifact(),
					ioObject.getPn().getInitialMarking(), ioObject.getXLog(), ioObject.getMapping(),
					ioObject.getArtifact());
		} catch (ObjectNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

}