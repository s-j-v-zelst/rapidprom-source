package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.plugins.inductiveVisualMiner.plugins.ProcessTreeVisualisationPlugin;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.ProcessTreeIOObject;

public class ProcessTreeIOObjectDotRenderer extends AbstractRapidProMIOObjectRenderer<ProcessTreeIOObject> {

	@Override
	public String getName() {
		return "Process Tree (Dot) renderer";
	}

	@Override
	protected JComponent runVisualization(ProcessTreeIOObject ioObject) {
		ProcessTreeVisualisationPlugin visualizer = new ProcessTreeVisualisationPlugin();
		return visualizer.fancy(null, ioObject.getArtifact());
	}
}