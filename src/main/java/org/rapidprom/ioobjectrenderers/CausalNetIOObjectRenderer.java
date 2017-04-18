package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.models.cnet.CausalNet;
import org.processmining.models.dcnet.DataAwareCausalNet;
import org.processmining.plugins.dcnet.DataAwareCausalNetVisualizerPlugin;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.CausalNetIOObject;

public class CausalNetIOObjectRenderer extends AbstractRapidProMIOObjectRenderer<CausalNetIOObject> {

	public String getName() {
		return "Causal Net renderer";
	}

	@Override
	protected JComponent runVisualization(CausalNetIOObject artifact) {
		CausalNet cnet = artifact.getArtifact();
		return new DataAwareCausalNetVisualizerPlugin().visualize(artifact.getPluginContext(),
				DataAwareCausalNet.Factory.fromCausalNet(cnet));
	}

}
