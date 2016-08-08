package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.plugins.petrinet.replayresult.visualization.PNLogReplayResultVisPanel;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.PNRepResultIOObject;

public class PNRepResultIOObjectLogRenderer extends AbstractRapidProMIOObjectRenderer<PNRepResultIOObject> {

	@Override
	public String getName() {
		return "PNRepResult (Project on Log) renderer";
	}

	@Override
	protected JComponent runVisualization(PNRepResultIOObject ioObject) {
		return new PNLogReplayResultVisPanel(ioObject.getPn().getArtifact(), ioObject.getXLog(), ioObject.getArtifact(),
				ioObject.getPluginContext().getProgress());
	}

}