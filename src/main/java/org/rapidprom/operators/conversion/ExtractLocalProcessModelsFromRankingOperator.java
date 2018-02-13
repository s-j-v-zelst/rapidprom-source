package org.rapidprom.operators.conversion;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.processmining.acceptingpetrinet.models.AcceptingPetriNet;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.models.semantics.petrinet.Marking;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.PetriNetIOObject;

import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.tools.LogService;

import org.rapidprom.ioobjects.LocalProcessModelRankingIOObject;
import org.processmining.lpm.util.LocalProcessModelRanking;

public class ExtractLocalProcessModelsFromRankingOperator extends Operator {

	private InputPort input = getInputPorts().createPort("model (ProM LocalProcessModelRanking)", LocalProcessModelRankingIOObject.class);
	private OutputPort output = getOutputPorts().createPort("model collections (ProM Petri Net)");

	public static final String 
	PARAMETER_1_KEY = "Number of LPMs to extract from ranking",
	PARAMETER_1_DESCR = "Number of LPMs to extract from ranking";
	
	public ExtractLocalProcessModelsFromRankingOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, IOObjectCollection.class));
	}

	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: extract Local Process Models from ranking");
		long time = System.currentTimeMillis();

		PluginContext pluginContext = RapidProMGlobalContext.instance().getPluginContext();
		
		LocalProcessModelRanking ranking = input.getData(LocalProcessModelRankingIOObject.class).getArtifact();
		IOObjectCollection<PetriNetIOObject> collection = new IOObjectCollection<PetriNetIOObject>();
		for(int i=0; i<Math.min(getParameterAsInt(PARAMETER_1_KEY), ranking.getSize()); i++){
			AcceptingPetriNet apn = ranking.getNet(i).getAcceptingPetriNet();
			Marking finalMarking = apn.getFinalMarkings().toArray(new Marking[1])[0];
			collection.add(new PetriNetIOObject(apn.getNet(), apn.getInitialMarking(), finalMarking, pluginContext));
		}

		output.deliver(collection);
		
		logger.log(Level.INFO,
				"End: extract Local Process Models from ranking (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}
	
	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();
		ParameterTypeInt parameter1 = new ParameterTypeInt(PARAMETER_1_KEY, PARAMETER_1_DESCR, 1, 100, 3);
		parameter1.setExpert(false);
		parameterTypes.add(parameter1);
		return parameterTypes;
	}
}
