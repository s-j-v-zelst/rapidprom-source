package org.rapidprom.operators.conversion;

import org.processmining.datapetrinets.DataPetriNet;
import org.processmining.datapetrinets.DataPetriNet.PetrinetWithMarkings;
import org.rapidprom.ioobjects.DataPetriNetIOObject;
import org.rapidprom.ioobjects.PetriNetIOObject;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;

public class DataPetriNetToPetriNetConversionOperator extends Operator {

	private final InputPort input = getInputPorts().createPort("model (ProM Data Petri net)",
			DataPetriNetIOObject.class);
	private final OutputPort output = getOutputPorts().createPort("model (ProM Petri net)");

	public DataPetriNetToPetriNetConversionOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, PetriNetIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {
		DataPetriNetIOObject pnIO = input.getData(DataPetriNetIOObject.class);
		DataPetriNet dpn = pnIO.getArtifact();
		PetrinetWithMarkings pnWithmarkings = DataPetriNet.Factory.toPetrinetWithMarkings(dpn);
		output.deliver(new PetriNetIOObject(pnWithmarkings.getNet(), pnWithmarkings.getInitialMarking(),
				pnWithmarkings.getFinalMarkings()[0], pnIO.getPluginContext()));
	}

}