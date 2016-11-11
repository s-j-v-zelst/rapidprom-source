package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.datapetrinets.visualization.graphviz.DPNGraphvizConverter;
import org.processmining.datapetrinets.visualization.graphviz.DPNGraphvizConverter.DPNAsDot;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.graphbased.directed.petrinetwithdata.newImpl.PetriNetWithData;
import org.processmining.models.graphbased.directed.petrinetwithdata.newImpl.PetriNetWithDataFactory;
import org.processmining.models.semantics.petrinet.Marking;
import org.processmining.plugins.graphviz.visualisation.DotPanel;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.PetriNetIOObject;

import javassist.tools.rmi.ObjectNotFoundException;

public class PetriNetIOGraphvizObjectRenderer extends AbstractRapidProMIOObjectRenderer<PetriNetIOObject> {

	@Override
	public String getName() {
		return "Petri Net renderer (graphviz)";
	}

	@Override
	protected JComponent runVisualization(PetriNetIOObject artifact) {
		Petrinet pn = artifact.getArtifact();
		PetriNetWithDataFactory factory = new PetriNetWithDataFactory(pn, pn.getLabel());
		PetriNetWithData dpn = factory.getRetValue();

		Marking initialMarking = null;
		try {
			initialMarking = artifact.getInitialMarking();
			initialMarking = factory.convertMarking(initialMarking);
		} catch (ObjectNotFoundException e) {
		}
		Marking[] finalMarkings = null;
		try {
			Marking[] markings = artifact.getFinalMarkingAsArray();
			finalMarkings = new Marking[markings.length];
			for (int i = 0; i < markings.length; i++) {
				finalMarkings[i] = factory.convertMarking(markings[i]);
			}
		} catch (ObjectNotFoundException e) {
		}
		DPNAsDot dot = DPNGraphvizConverter.convertDPN(dpn, dpn.getInitialMarking(), dpn.getFinalMarkings());
		return opaqueBg(new DotPanel(dot.getDot()));
	}

	private JComponent opaqueBg(JComponent c) {
		c.setOpaque(true);
		return c;
	}

}
