package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;
import javax.swing.JLabel;

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
		if (ioObject.getArtifact() == null) {
			return new JLabel("No alignment could be computed");
		}		
		return new PNLogReplayResultVisPanel(ioObject.getXLog(), ioObject.getArtifact(),
				ioObject.getPluginContext().getProgress());
	}

}