package org.rapidprom.operators.filtering.abstr;

import java.util.List;

import org.deckfour.xes.classification.XEventAndClassifier;
import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.classification.XEventLifeTransClassifier;
import org.deckfour.xes.classification.XEventNameClassifier;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.parameter.ParameterTypeXEventClassifierCategory;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.UndefinedParameterError;

public abstract class AbstractFilteringOperator extends Operator {

	public AbstractFilteringOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputLogPort, XLogIOObject.class));
	}

	private final OutputPort outputLogPort = getOutputPorts().createPort("log (Filtered Log)");
	private final InputPort inputLogPort = getInputPorts().createPort("event log ", XLogIOObject.class);

	private static final String PARAMETER_KEY_EVENT_CLASSIFIER = "event_classifier";
	private static final String PARAMETER_DESC_EVENT_CLASSIFIER = "Specifies how to identify events within the event log, as defined in http://www.xes-standard.org/";
	private static XEventClassifier[] PARAMETER_DEFAULT_CLASSIFIERS = new XEventClassifier[] {
			new XEventAndClassifier(new XEventNameClassifier(), new XEventLifeTransClassifier()) };

	public OutputPort getOutputLogPort() {
		return outputLogPort;
	}

	public InputPort getInputLogPort() {
		return inputLogPort;
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeXEventClassifierCategory(PARAMETER_KEY_EVENT_CLASSIFIER,
				PARAMETER_DESC_EVENT_CLASSIFIER, new String[] { PARAMETER_DEFAULT_CLASSIFIERS[0].toString() },
				PARAMETER_DEFAULT_CLASSIFIERS, 0, false, getInputLogPort()));
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

}
