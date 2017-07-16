package org.rapidprom.operators.decomposition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.processmining.acceptingpetrinet.models.AcceptingPetriNet;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.util.collection.MultiSet;
import org.processmining.models.graphbased.directed.DirectedGraphElement;
import org.processmining.models.graphbased.directed.petrinet.analysis.TransitionInvariantSet;
import org.processmining.models.graphbased.directed.petrinet.elements.Place;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.models.semantics.petrinet.Marking;
import org.processmining.plugins.petrinet.structuralanalysis.invariants.TransitionInvariantCalculator;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.ioobjects.PetriNetRPSTIOObject;
import org.rapidprom.operators.decomposition.rpst.PetriNetRPST;
import org.rapidprom.operators.decomposition.rpst.PetriNetRPSTNode;
import org.rapidprom.operators.decomposition.rpst.RPSTNode2PetriNet;

import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;

public class ExtractRPSTLeafsAsPetriNetsOperator extends Operator {
	
	private static final String ADD_CYCLES_PARAMETER_KEY = "Add cycles";	

	private final InputPort input = getInputPorts().createPort("rpst (ProM Petri Net RPST)",
			PetriNetRPSTIOObject.class);
	private final OutputPort output = getOutputPorts().createPort("model collection (ProM Petri Net Collection)");

	public ExtractRPSTLeafsAsPetriNetsOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, IOObjectCollection.class));
	}

	@Override
	public void doWork() throws OperatorException {
		PetriNetRPSTIOObject rpstIO = input.getData(PetriNetRPSTIOObject.class);

		PluginContext context = rpstIO.getPluginContext();
		PetriNetRPST rpst = rpstIO.getArtifact();

		Collection<AcceptingPetriNet> pnCollection = createPNFromRPSTLeafs(context, rpst);

		IOObjectCollection<PetriNetIOObject> collection = new IOObjectCollection<>();
		for (AcceptingPetriNet pn : pnCollection) {
			if (!pn.getFinalMarkings().isEmpty()) {
				collection.add(new PetriNetIOObject(pn.getNet(), pn.getInitialMarking(),
						pn.getFinalMarkings().iterator().next(), context));
			} else {
				throw new OperatorException("Final marking of " + pn.getNet().getLabel() + " is missing");
			}
		}

		output.deliver(collection);
	}

	private Collection<AcceptingPetriNet> createPNFromRPSTLeafs(PluginContext context, PetriNetRPST rpst) {

		// Compute the T-Invariants of the net
		TransitionInvariantCalculator invCalculator = new TransitionInvariantCalculator();
		TransitionInvariantSet invariants = invCalculator.calculate(context, rpst.getNet().getNet());
		Set<Transition> tInvCovered = new HashSet<>();
		for (MultiSet<Transition> inv : invariants) {
			tInvCovered.addAll(inv);
		}

		Collection<AcceptingPetriNet> leafs = new ArrayList<>();
		for (PetriNetRPSTNode node : rpst.getNodes()) {
			if (rpst.getChildren(node).isEmpty()) {
				Map<DirectedGraphElement, DirectedGraphElement> map = new HashMap<>();
				AcceptingPetriNet acceptingNet = RPSTNode2PetriNet.convertToAcceptingPetriNet(node,
						rpst.getNet().getInitialMarking(), rpst.getNet().getFinalMarkings(), map);

				boolean cyclic;
				if (tInvCovered.containsAll(node.getTrans())) {
					cyclic = true;
				} else {
					cyclic = false;
				}
				
				workflowingNet(acceptingNet, node, map, cyclic);

				leafs.add(acceptingNet);
			}
		}

		return leafs;
	}

	private void workflowingNet(AcceptingPetriNet anet, PetriNetRPSTNode node,
			Map<DirectedGraphElement, DirectedGraphElement> map, boolean cyclic) {

		// Initial Place and initial Marking
		Place in;
		if (node.getEntry() instanceof Place) {
			in = (Place) map.get(node.getEntry());
			if (!anet.getInitialMarking().contains(in)) {
				anet.getInitialMarking().add(in);
			}
		} else {
			Transition t = (Transition) map.get(node.getEntry());
			in = anet.getNet().addPlace("pin");
			anet.getNet().addArc(in, t);
			anet.getInitialMarking().add(in);
		}

		// Final Place and final Marking
		Place out;
		if (node.getExit() instanceof Place) {
			out = (Place) map.get(node.getExit());
			for (Marking finalM : anet.getFinalMarkings()) {
				if (!finalM.contains(out)) {
					finalM.add(out);
				}
			}
		} else {
			Transition t = (Transition) map.get(node.getExit());
			out = anet.getNet().addPlace("pout");
			anet.getNet().addArc(t, out);
			for (Marking finalM : anet.getFinalMarkings()) {
				finalM.add(out);
			}
		}

		// Cyclic Behavior
		if (cyclic && getParameterAsBoolean(ADD_CYCLES_PARAMETER_KEY)) {
			Transition invSC = anet.getNet().addTransition("invShortCircuit");
			invSC.setInvisible(true);
			anet.getNet().addArc(out, invSC);
			anet.getNet().addArc(invSC, in);
		}

	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeBoolean(ADD_CYCLES_PARAMETER_KEY, "Add cycles for those fragments that that part of a T-invariant.", false));
		return params;
	}
	
}
