package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.models.causalgraph.XEventClassifierAwareSimpleCausalGraph;
import org.processmining.plugins.causalgraph.CausalGraphVisualizerPlugin;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.XEventClassifierAwareSimpleCausalGraphIOObject;

public class XEventClassifierAwareSimpleCausalGraphIOObjectRenderer
		extends AbstractRapidProMIOObjectRenderer<XEventClassifierAwareSimpleCausalGraphIOObject> {

	@Override
	public String getName() {
		return "Causal Graph Renderer";
	}

	@Override
	protected JComponent runVisualization(XEventClassifierAwareSimpleCausalGraphIOObject artifact) {
		XEventClassifierAwareSimpleCausalGraph causalGraph = artifact.getArtifact();
		CausalGraphVisualizerPlugin visualizerProM = new CausalGraphVisualizerPlugin();
		return visualizerProM.visualize(artifact.getPluginContext(), causalGraph);
	}

}
