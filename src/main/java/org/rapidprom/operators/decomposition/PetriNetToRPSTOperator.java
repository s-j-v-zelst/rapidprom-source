package org.rapidprom.operators.decomposition;

import org.processmining.acceptingpetrinet.models.impl.AcceptingPetriNetImpl;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.ioobjects.PetriNetRPSTIOObject;
import org.rapidprom.operators.decomposition.rpst.PetriNetRPST;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import javassist.tools.rmi.ObjectNotFoundException;

public class PetriNetToRPSTOperator extends Operator {

	private final InputPort input = getInputPorts().createPort("model (ProM Petri Net)", PetriNetIOObject.class);
	private final OutputPort output = getOutputPorts().createPort("rpst (ProM Petri Net RPST)");

	public PetriNetToRPSTOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, PetriNetRPSTIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {
		PetriNetIOObject pnIO = input.getData(PetriNetIOObject.class);
		Petrinet pn = pnIO.getArtifact();

		AcceptingPetriNetImpl acceptingPN;
		try {
			acceptingPN = new AcceptingPetriNetImpl(pn, pnIO.getInitialMarking(), pnIO.getFinalMarkingAsArray());
		} catch (ObjectNotFoundException e) {
			acceptingPN = new AcceptingPetriNetImpl(pnIO.getPluginContext(), pn);
		}

		output.deliver(new PetriNetRPSTIOObject(new PetriNetRPST(acceptingPN), pnIO.getPluginContext()));
	}

}