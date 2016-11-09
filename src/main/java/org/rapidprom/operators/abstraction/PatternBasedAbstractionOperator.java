package org.rapidprom.operators.abstraction;

import java.util.List;
import org.deckfour.xes.model.XLog;
import org.processmining.datapetrinets.DataPetriNetsWithMarkings;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.logenhancement.abstraction.PatternBasedLogAbstractionPlugin;
import org.processmining.logenhancement.abstraction.PatternStructureException;
import org.processmining.logenhancement.abstraction.model.AbstractionModel;
import org.processmining.plugins.balancedconformance.config.BalancedProcessorConfiguration;
import org.processmining.plugins.balancedconformance.controlflow.ControlFlowAlignmentException;
import org.processmining.plugins.balancedconformance.dataflow.exception.DataAlignmentException;
import org.processmining.plugins.connectionfactories.logpetrinet.TransEvClassMapping;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.AbstractionModelIOObject;
import org.rapidprom.ioobjects.TransEvMappingIOObject;
import org.rapidprom.ioobjects.XLogIOObject;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;

public class PatternBasedAbstractionOperator extends Operator {

	private static final String CONCURRENT_THREADS_KEY = "Number of Threads",
			CONCURRENT_THREADS_DESCR = "Specify the number of threads used to calculate alignments in parallel."
					+ " With each extra thread, more memory is used but less cpu time is required.",
			KEEP_UNMATCHED_EVENTS_KEY = "Keep Unmatched Events",
			KEEP_UNMATCHED_EVENTS_DESCR = "Do you want to keep low-level events that cannot be matched to any activity pattern? "
					+ "This can be useful if your event log already contains some events at the desired level of abstraction.",
			ERROR_RATE_KEY = "Error Rate", ERROR_RATE_DESCR = "Specify the acceptable 'matching error' ([0.0,1.0]";

	private InputPort inputXLog = getInputPorts().createPort("event log (ProM Event Log)", XLogIOObject.class);
	private InputPort inputAbstractionModel = getInputPorts().createPort("abstraction model (ProM Abstraction Model)",
			AbstractionModelIOObject.class);
	private InputPort inputMapping = getInputPorts().createPort("mapping (ProM Transition/Event Class Mapping)",
			TransEvMappingIOObject.class);

	private OutputPort output = getOutputPorts().createPort("event log (ProM Event Log)");

	public PatternBasedAbstractionOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, XLogIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {

		XLog log = getXLog();
		AbstractionModel abstractionModel = getAbstractionModel();
		TransEvClassMapping mapping = getMapping();

		try {
			boolean keepUnmatchedEvents = getParameterAsBoolean(KEEP_UNMATCHED_EVENTS_KEY);
			double errorRateLimit = getParameterAsDouble(ERROR_RATE_KEY);

			DataPetriNetsWithMarkings model = abstractionModel.getCombinedDPN();
			BalancedProcessorConfiguration alignmentConfig = BalancedProcessorConfiguration.newDefaultInstance(model,
					model.getInitialMarking(), model.getFinalMarkings(), log, mapping.getEventClassifier(), 1, 1, 1, 1);

			alignmentConfig.setActivityMapping(mapping);
			alignmentConfig.getMapEvClass2Cost().put(mapping.getDummyEventClass(), 0); // TODO

			XLog abstractedLog = PatternBasedLogAbstractionPlugin.abstractPatterns(getContext(), log, abstractionModel,
					keepUnmatchedEvents, errorRateLimit, alignmentConfig);

			output.deliver(new XLogIOObject(abstractedLog, getContext()));

		} catch (ControlFlowAlignmentException | DataAlignmentException | PatternStructureException e) {
			throw new OperatorException("Failed abstraction!", e);
		}

	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();

		params.add(new ParameterTypeBoolean(KEEP_UNMATCHED_EVENTS_KEY, KEEP_UNMATCHED_EVENTS_DESCR, true, false));
		params.add(new ParameterTypeDouble(ERROR_RATE_KEY, ERROR_RATE_DESCR, 0.0, 1.0, 0.0));

		params.add(new ParameterTypeInt(CONCURRENT_THREADS_KEY, CONCURRENT_THREADS_DESCR, 1,
				Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors(), true));

		return params;
	}

	private TransEvClassMapping getMapping() throws UserError {
		return inputMapping.getData(TransEvMappingIOObject.class).getArtifact();
	}

	private AbstractionModel getAbstractionModel() throws UserError {
		return inputAbstractionModel.getData(AbstractionModelIOObject.class).getArtifact();
	}

	private XLog getXLog() throws UserError {
		return inputXLog.getData(XLogIOObject.class).getArtifact();
	}

	private PluginContext getContext() throws UserError {
		return RapidProMGlobalContext.instance().getPluginContext();
	}

}