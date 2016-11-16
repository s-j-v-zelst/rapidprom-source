package org.rapidprom.operators.abstr;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.deckfour.xes.classification.XEventAndClassifier;
import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.classification.XEventClasses;
import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.classification.XEventLifeTransClassifier;
import org.deckfour.xes.classification.XEventNameClassifier;
import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.model.XLog;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.log.utils.XUtils;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.parameter.ParameterTypeXEventClassifierCategory;
import org.rapidprom.util.ObjectUtils;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.UndefinedParameterError;

public abstract class AbstractRapidProMLogManipulationOperator extends Operator {

	private InputPort inputXLog = getInputPorts().createPort("event log (ProM Event Log)", XLogIOObject.class);

	private static final String PARAMETER_KEY_EVENT_CLASSIFIER = "event_classifier";
	private static final String PARAMETER_DESC_EVENT_CLASSIFIER = "Specifies how to identify events within the event log, as defined in http://www.xes-standard.org/";

	private static XEventClassifier[] PARAMETER_DEFAULT_CLASSIFIERS = new XEventClassifier[] {
			new XEventAndClassifier(new XEventNameClassifier(), new XEventLifeTransClassifier()),
			new XEventNameClassifier()};

	public AbstractRapidProMLogManipulationOperator(OperatorDescription description) {
		super(description);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeXEventClassifierCategory(
				PARAMETER_KEY_EVENT_CLASSIFIER, PARAMETER_DESC_EVENT_CLASSIFIER,
				ObjectUtils.toString(PARAMETER_DEFAULT_CLASSIFIERS),
				PARAMETER_DEFAULT_CLASSIFIERS, 0, false, inputXLog));
		return params;
	}

	protected XEventClassifier getXEventClassifier() throws UndefinedParameterError {
		ParameterTypeXEventClassifierCategory eClassParam = (ParameterTypeXEventClassifierCategory) getParameterType(
				PARAMETER_KEY_EVENT_CLASSIFIER);
		try {
			return eClassParam.valueOf(getParameterAsInt(PARAMETER_KEY_EVENT_CLASSIFIER));
		} catch (IndexOutOfBoundsException e) {
			throw new UndefinedParameterError("The index chosen is no longer available");
		}

	}

	protected XLog getXLog() throws UserError {
		return ((XLogIOObject) inputXLog.getData(XLogIOObject.class)).getArtifact();
	}

	protected XFactory getFactory() {
		return XFactoryRegistry.instance().currentDefault();
	}

	protected PluginContext getPluginContext() throws UserError {
		return ((XLogIOObject) inputXLog.getData(XLogIOObject.class)).getPluginContext();
	}

	protected boolean isConsidered(XEventClass eventClass) {
		return true;
	}

	protected XEventClasses getEventClasses() throws UndefinedParameterError, UserError {
		return XUtils.createEventClasses(getXEventClassifier(), getXLog());
	}

	protected Set<XEventClass> getConsideredEventClasses() throws UndefinedParameterError, UserError {
		Set<XEventClass> consideredClasses = new HashSet<XEventClass>();
		for (XEventClass eventClass : getEventClasses().getClasses()) {
			if (isConsidered(eventClass)) {
				consideredClasses.add(eventClass);
			}
		}
		return consideredClasses;
	}
	
	protected <T extends Enum<?>> String[] enumValuesToStringArray(T[] values) {
		String[] valuesAsString = new String[values.length];
		int i = 0;
		for (T type : values) {
			valuesAsString[i++] = (type).name();
		}
		return valuesAsString;
	}

}