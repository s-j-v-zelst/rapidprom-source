package org.rapidprom.operators.abstr;

import java.util.List;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.UndefinedParameterError;

/**
 * Discovery operator that allows to give a user-defined label to the discovered
 * process model.
 * 
 * @author F. Mannhardt
 *
 */
abstract public class AbstractLabelAwareRapidProMDiscoveryOperator extends AbstractRapidProMEventLogBasedOperator {

	private static final String PARAMETER_LABEL_KEY = "Model label";
	private static final String PARAMETER_LABEL_DESC = "The label assigned to the discovered model.";

	public AbstractLabelAwareRapidProMDiscoveryOperator(OperatorDescription description) {
		super(description);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeString(PARAMETER_LABEL_KEY, PARAMETER_LABEL_DESC, true, true));
		return params;
	}

	protected String getLabel() throws UndefinedParameterError {
		return getParameterAsString(PARAMETER_LABEL_KEY);
	}

}
