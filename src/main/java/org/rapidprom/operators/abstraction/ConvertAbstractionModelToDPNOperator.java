package org.rapidprom.operators.abstraction;

import org.processmining.datapetrinets.DataPetriNetsWithMarkings;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.logenhancement.abstraction.model.AbstractionModel;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.AbstractionModelIOObject;
import org.rapidprom.ioobjects.DataPetriNetIOObject;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;

/**
 * Converts the {@link AbstractionModelIOObject} to an
 * {@link DataPetriNetIOObject}.
 * 
 * @author F. Mannhardt
 *
 */
public class ConvertAbstractionModelToDPNOperator extends Operator {

	private InputPort inputAbstractionModel = getInputPorts().createPort("abstraction model (ProM Abstraction Model)",
			AbstractionModelIOObject.class);
	private OutputPort output = getOutputPorts().createPort("model (ProM Data Petri Net)");

	public ConvertAbstractionModelToDPNOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, DataPetriNetIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {
		AbstractionModelIOObject obj = inputAbstractionModel.getData(AbstractionModelIOObject.class);
		AbstractionModel abstractionModel = obj.getArtifact();
		DataPetriNetsWithMarkings dpn = abstractionModel.getCombinedDPN();
		output.deliver(new DataPetriNetIOObject(dpn, dpn.getInitialMarking(), dpn.getFinalMarkings()[0], getContext()));

	}

	private PluginContext getContext() throws UserError {
		return RapidProMGlobalContext.instance().getPluginContext();
	}

}