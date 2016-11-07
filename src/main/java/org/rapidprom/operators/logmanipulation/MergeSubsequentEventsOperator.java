package org.rapidprom.operators.logmanipulation;

import java.util.List;
import java.util.Set;

import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.classification.XEventClasses;
import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.model.XLog;
import org.processmining.log.utils.XUtils;
import org.processmining.logenhancement.transforming.MergeSubsequentEvents;
import org.processmining.logenhancement.transforming.MergeSubsequentEvents.MergeFilter;
import org.processmining.logenhancement.transforming.MergeSubsequentEvents.MergeType;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMLogManipulationOperator;

import com.google.common.collect.ImmutableSet;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;

/**
 * Merges subsequent events in an event log based on several heuristics. Taken
 * from the LogEnhancement package of ProM.
 * 
 * @author F. Mannhardt
 *
 */
public class MergeSubsequentEventsOperator extends AbstractRapidProMLogManipulationOperator {

	private static final String PARAMETER_MERGE_TYPE_KEY = "Merge type",
			PARAMETER_MERGE_TYPE_DESCR = "Select how events are going to be merged.",
			PARAMETER_MERGE_FILTER_KEY = "Comparison method",
			PARAMETER_MERGE_FILTER_DESCR = "Select how to determine whether two events are the same.";

	private OutputPort outputEventLog = getOutputPorts().createPort("event log (ProM Event Log)");

	public MergeSubsequentEventsOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputEventLog, XLogIOObject.class));
	}

	public void doWork() throws OperatorException {

		XLog logOriginal = getXLog();
		XEventClasses eventClasses = getEventClasses();
		XFactory factory = getFactory();
		Set<XEventClass> consideredClasses = getConsideredEventClasses();
		MergeType mergeType = MergeType.valueOf(getParameter(PARAMETER_MERGE_TYPE_KEY));
		MergeFilter mergeFilter = MergeFilter.valueOf(getParameter(PARAMETER_MERGE_FILTER_KEY));

		//TODO configurable
		Set<String> relevantAttributes = XUtils.getEventAttributeKeys(logOriginal);
		
		XLog logModified = new MergeSubsequentEvents().doMergeSubsequentEvents(logOriginal, eventClasses, factory,
				consideredClasses, relevantAttributes, mergeFilter, mergeType);

		XLogIOObject result = new XLogIOObject(logModified, getPluginContext());
		outputEventLog.deliver(result);
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterTypeCategory parameterMerge = new ParameterTypeCategory(PARAMETER_MERGE_TYPE_KEY,
				PARAMETER_MERGE_TYPE_DESCR, enumValuesToStringArray(MergeType.values()), 0, false);
		parameterTypes.add(parameterMerge);

		ParameterTypeCategory parameterFilter = new ParameterTypeCategory(PARAMETER_MERGE_FILTER_KEY,
				PARAMETER_MERGE_FILTER_DESCR, enumValuesToStringArray(MergeFilter.values()), 0, false);
		parameterTypes.add(parameterFilter);

		return parameterTypes;
	}

}