package org.rapidprom.operators.conversion;

import org.processmining.dataawarecnetminer.plugins.CNetToPetriNetPlugin;
import org.processmining.datapetrinets.DataPetriNet.PetrinetWithMarkings;
import org.processmining.models.cnet.CausalNet;
import org.rapidprom.ioobjects.CausalNetIOObject;
import org.rapidprom.ioobjects.PetriNetIOObject;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;

public class CausalNetToPetriNetConversionOperator extends Operator {

	private InputPort input = getInputPorts().createPort("model (ProM Causal net)", CausalNetIOObject.class);
	private OutputPort output = getOutputPorts().createPort("model (ProM Petri Net)");

	public CausalNetToPetriNetConversionOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, PetriNetIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {

		CausalNetIOObject cnetIO = input.getData(CausalNetIOObject.class);
		CausalNet causalNet = cnetIO.getArtifact();
		PetrinetWithMarkings pNet = CNetToPetriNetPlugin.convertCNetToPN(causalNet);

		PetriNetIOObject finalPetriNet = new PetriNetIOObject(pNet.getNet(), pNet.getInitialMarking(),
				pNet.getFinalMarkings()[0], cnetIO.getPluginContext());
		output.deliver(finalPetriNet);

	}
}
