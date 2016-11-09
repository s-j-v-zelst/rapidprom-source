package org.rapidprom.operators.conversion;

import org.processmining.datapetrinets.DataPetriNet;
import org.rapidprom.ioobjects.DataPetriNetIOObject;
import org.rapidprom.ioobjects.PetriNetIOObject;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import javassist.tools.rmi.ObjectNotFoundException;

public class PetriNetToDataPetriNetConversionOperator extends Operator {

	private final InputPort input = getInputPorts().createPort("model (ProM Petri Net)", PetriNetIOObject.class);
	private final OutputPort output = getOutputPorts().createPort("model (ProM Data Petri net)");

	public PetriNetToDataPetriNetConversionOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, DataPetriNetIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {
		PetriNetIOObject pnIO = input.getData(PetriNetIOObject.class);
		try {
			output.deliver(new DataPetriNetIOObject(DataPetriNet.Factory.fromPetrinet(pnIO.getArtifact()),
					pnIO.getInitialMarking(), pnIO.getFinalMarking(), pnIO.getPluginContext()));
		} catch (ObjectNotFoundException e) {
			output.deliver(new DataPetriNetIOObject(DataPetriNet.Factory.fromPetrinet(pnIO.getArtifact()), null, null,
					pnIO.getPluginContext()));
		}
	}

}