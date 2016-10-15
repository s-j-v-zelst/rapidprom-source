package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.contexts.uitopia.UIPluginContext;
import org.processmining.lpm.util.VisualizeLocalProcessModelRankingDotPlugin;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.LocalProcessModelRankingIOObject;

public class LocalProcessModelRankingIOObjectRenderer
		extends AbstractRapidProMIOObjectRenderer<LocalProcessModelRankingIOObject> {

	@Override
	public String getName() {
		return "LocalProcessModel Object Renderer";
	}

	@Override
	protected JComponent runVisualization(LocalProcessModelRankingIOObject ioObject) {
		VisualizeLocalProcessModelRankingDotPlugin visualizer = new VisualizeLocalProcessModelRankingDotPlugin();
		return visualizer.visualize(ioObject.getPluginContext(), ioObject.getArtifact());
	}

}
