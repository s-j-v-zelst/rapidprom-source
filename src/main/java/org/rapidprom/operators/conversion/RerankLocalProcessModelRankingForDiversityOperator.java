package org.rapidprom.operators.conversion;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.LocalProcessModelRankingIOObject;
import org.processmining.lpm.postprocess.ReweighForDiversification;
import org.processmining.lpm.util.LocalProcessModelRanking;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.tools.LogService;

public class RerankLocalProcessModelRankingForDiversityOperator extends Operator{

	private InputPort input = getInputPorts().createPort("model (ProM LocalProcessModelRanking)", LocalProcessModelRankingIOObject.class);
	private OutputPort output = getOutputPorts().createPort("model (ProM LocalProcessModelRanking)");

	public static final String 
	PARAMETER_1_KEY = "Diversity threshold",
	PARAMETER_1_DESCR = "Diversity threshold";
	
	public RerankLocalProcessModelRankingForDiversityOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, LocalProcessModelRankingIOObject.class));
	}

	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: Filter local process model ranking for diversity");
		long time = System.currentTimeMillis();


		LocalProcessModelRanking filteredRanking = ReweighForDiversification.rerankHeadless(input.getData(LocalProcessModelRankingIOObject.class).getArtifact(), getParameterAsDouble(PARAMETER_1_KEY));

		output.deliver(new LocalProcessModelRankingIOObject(filteredRanking, RapidProMGlobalContext.instance().getPluginContext()));

		logger.log(Level.INFO,
				"End: Filter local process model ranking for diversity (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}
	
	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();
		ParameterTypeDouble parameter1 = new ParameterTypeDouble(PARAMETER_1_KEY, PARAMETER_1_DESCR, 0, 1, 0.5);
		parameterTypes.add(parameter1);
		return parameterTypes;
	}
}
