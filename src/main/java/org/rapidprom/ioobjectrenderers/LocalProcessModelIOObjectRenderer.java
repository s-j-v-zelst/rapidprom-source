package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.lpm.visualization.VisualizeLocalProcessModelDotPlugin;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.LocalProcessModelIOObject;

public class LocalProcessModelIOObjectRenderer extends AbstractRapidProMIOObjectRenderer<LocalProcessModelIOObject> {

	@Override
	public String getName() {
		return "LocalProcessModel Object Renderer";
	}

	@Override
	protected JComponent runVisualization(LocalProcessModelIOObject ioObject) {
		VisualizeLocalProcessModelDotPlugin visualizer = new VisualizeLocalProcessModelDotPlugin();
		return visualizer.visualize(ioObject.getPluginContext(), ioObject.getArtifact());
	}

}
