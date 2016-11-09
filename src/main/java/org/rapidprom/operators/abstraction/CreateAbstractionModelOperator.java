package org.rapidprom.operators.abstraction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.processmining.datapetrinets.DataPetriNet;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.logenhancement.abstraction.PatternBasedLogAbstractionPlugin;
import org.processmining.logenhancement.abstraction.model.AbstractionModel;
import org.processmining.logenhancement.abstraction.model.AbstractionPattern;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionVisitorException;
import org.processmining.logenhancement.abstraction.model.syntax.ParseException;
import org.processmining.models.graphbased.directed.petrinet.PetrinetGraph;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.AbstractionModelIOObject;
import org.rapidprom.ioobjects.DataPetriNetIOObject;

import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPortExtender;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.UndefinedParameterError;

public class CreateAbstractionModelOperator extends Operator {

	private static final String ABSTRACTION_MODEL_KEY = "Abstraction Model",
			ABSTRACTION_MODEL_DESCR = "Specify how patterns are composed to an integrated abstraction model. "
					+ "If not otherwise specified all patterns can occur in parallel to each other.";

	private final InputPortExtender inExtender = new InputPortExtender("patterns (Data Petri nets)", getInputPorts(),
			new MetaData(DataPetriNetIOObject.class), true);

	private OutputPort output = getOutputPorts().createPort("abstraction model (ProM Abstraction Model)");

	public CreateAbstractionModelOperator(OperatorDescription description) {
		super(description);
		inExtender.start();
		getTransformer().addRule(new GenerateNewMDRule(output, AbstractionModelIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {

		DataPetriNet[] patterns = getPatterns();
		Map<String, AbstractionPattern> patternMap = createPatternMap(patterns);

		try {
			String abstractionModelCode = getAbstractionModelCode(patternMap);

			AbstractionModel abstractionModel = PatternBasedLogAbstractionPlugin.composePatterns(abstractionModelCode,
					patternMap, true);

			output.deliver(new AbstractionModelIOObject(abstractionModel, getContext()));

		} catch (ParseException | CompositionVisitorException e) {
			throw new OperatorException("Failed building abstraction model!", e);
		}
	}

	private Map<String, AbstractionPattern> createPatternMap(DataPetriNet[] patterns) throws OperatorException {
		Map<DataPetriNet, String> patternNames = new HashMap<>();
		for (DataPetriNet net : patterns) {
			if (!patternNames.containsKey(net)) {
				patternNames.put(net, net.getLabel());
			} else {
				throw new OperatorException("Invalid input, two identical patterns with the name " + net.getLabel());
			}
		}
		return PatternBasedLogAbstractionPlugin.buildPatterns(patternNames);
	}

	private String getAbstractionModelCode(Map<String, AbstractionPattern> abstractionPatterns)
			throws UndefinedParameterError {
		String modelAsString = getParameterAsString(ABSTRACTION_MODEL_KEY);
		if (modelAsString != null && !modelAsString.isEmpty()) {
			return modelAsString;
		}
		return PatternBasedLogAbstractionPlugin.buildInitialModel(abstractionPatterns);
	}

	private DataPetriNet[] getPatterns() throws UserError, OperatorException {
		Collection<PetrinetGraph> patterns = new ArrayList<>();
		@SuppressWarnings("rawtypes")
		List<IOObjectCollection> data = inExtender.getData(IOObjectCollection.class, false);
		for (IOObjectCollection<?> list : data) {
			for (Object obj : list.getObjectsRecursive()) {
				if (obj instanceof DataPetriNetIOObject) {
					patterns.add(((DataPetriNetIOObject) obj).getArtifact());
				} else {
					throw new OperatorException("Unknown pattern type " + obj);
				}
			}
		}
		return PatternBasedLogAbstractionPlugin.transformNets(patterns.toArray(new PetrinetGraph[patterns.size()]));
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeString(ABSTRACTION_MODEL_KEY, ABSTRACTION_MODEL_DESCR, true, false));
		return params;
	}

	private PluginContext getContext() throws UserError {
		return RapidProMGlobalContext.instance().getPluginContext();
	}

}