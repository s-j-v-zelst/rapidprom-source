package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.plugins.graphviz.dot.Dot;
import org.processmining.plugins.graphviz.visualisation.DotPanel;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.DotIOObject;

public class DotIOObjectRenderer extends AbstractRapidProMIOObjectRenderer<DotIOObject> {

	@Override
	public String getName() {
		return "Dot renderer";
	}

	@Override
	protected JComponent runVisualization(DotIOObject artifact) {
		Dot dot = artifact.getArtifact();
		return new DotPanel(dot);
	}

}