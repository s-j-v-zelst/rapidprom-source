package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.lpm.util.VisualizeLocalProcessModelDotPlugin;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.LocalProcessModelIOObject;

public class LocalProcessModelIOObjectRenderer
		extends AbstractRapidProMIOObjectRenderer<LocalProcessModelIOObject> {

	@Override
	public String getName() {
		return "LocalProcessModel Object Renderer";
	}

	@Override
	protected JComponent runVisualization(LocalProcessModelIOObject ioObject) {
		return VisualizeLocalProcessModelDotPlugin.visualize(ioObject.getPluginContext(), ioObject.getArtifact());
	}

}
