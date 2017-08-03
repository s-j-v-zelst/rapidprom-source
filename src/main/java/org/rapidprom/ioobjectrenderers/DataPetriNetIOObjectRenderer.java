package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.datapetrinets.DataPetriNet;
import org.processmining.datapetrinets.visualization.graphviz.DPNGraphvizVisualizer;
import org.processmining.models.semantics.petrinet.Marking;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.DataPetriNetIOObject;

import javassist.tools.rmi.ObjectNotFoundException;

public class DataPetriNetIOObjectRenderer extends AbstractRapidProMIOObjectRenderer<DataPetriNetIOObject> {

	@Override
	public String getName() {
		return "Data Petri net renderer";
	}

	@Override
	protected JComponent runVisualization(DataPetriNetIOObject artifact) {
		DataPetriNet dpn = artifact.getArtifact();
		try {
			return new DPNGraphvizVisualizer(dpn, artifact.getInitialMarking(),
					new Marking[] { artifact.getFinalMarking() });
		} catch (ObjectNotFoundException e) {
			return new DPNGraphvizVisualizer(dpn, null, null);
		}
	}

}