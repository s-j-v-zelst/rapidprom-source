package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.datapetrinets.DataPetriNetsWithMarkings;
import org.processmining.datapetrinets.visualization.graphviz.DPNGraphvizVisualizer;
import org.processmining.logenhancement.abstraction.model.AbstractionModel;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.AbstractionModelIOObject;

public class AbstractionModelIOObjectRenderer extends AbstractRapidProMIOObjectRenderer<AbstractionModelIOObject> {

	@Override
	public String getName() {
		return "Abstraction model renderer";
	}

	@Override
	protected JComponent runVisualization(AbstractionModelIOObject artifact) {
		AbstractionModel abstractionModel = artifact.getArtifact();
		DataPetriNetsWithMarkings dpn = abstractionModel.getCombinedDPN();
		return new DPNGraphvizVisualizer(dpn, dpn.getInitialMarking(), dpn.getFinalMarkings());
	}

}