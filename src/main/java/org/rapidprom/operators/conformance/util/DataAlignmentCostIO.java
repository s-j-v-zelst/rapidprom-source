package org.rapidprom.operators.conformance.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.plugins.DataConformance.framework.ReplayableActivity;
import org.processmining.plugins.DataConformance.framework.VariableMatchCost;
import org.processmining.plugins.DataConformance.framework.VariableMatchCosts;
import org.rapidprom.exceptions.ExampleSetReaderException;

import com.google.common.math.DoubleMath;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.tools.Ontology;

/**
 * Reads and writes costs for use with an data alignment from an Example set.
 * 
 * @author F. Mannhardt
 *
 */
public class DataAlignmentCostIO {

	public static final String TRANSITION_COLUMN = "transition";
	public static final String VARIABLE_COLUMN = "variable";
	public static final String COST_MISSING_COLUMN = "costMissing";
	public static final String COST_WRONG_COLUMN = "costWrong";

	public VariableMatchCosts readCostsFromExampleSet(ExampleSet costs, int defaultCostFaulty,
			int defaultCostNotWriting, Collection<Transition> transitions, Set<String> variables)
			throws ExampleSetReaderException {

		Set<ReplayableActivity> activitySet = new HashSet<>();
		for (final Transition t : transitions) {
			activitySet.add(new ReplayableActivity() {

				@Override
				public String getLabel() {
					return t.getLabel();
				}
			});
		}

		List<VariableMatchCost> variableCosts = new ArrayList<>();

		Attribute transAttr = costs.getAttributes().get(TRANSITION_COLUMN);
		Attribute variableAttr = costs.getAttributes().get(VARIABLE_COLUMN);
		Attribute costMissingAttr = costs.getAttributes().get(COST_MISSING_COLUMN);
		Attribute costWrongAttr = costs.getAttributes().get(COST_WRONG_COLUMN);

		if (transAttr == null || variableAttr == null || costMissingAttr == null || costWrongAttr == null) {
			throw new ExampleSetReaderException("Missing columns!");
		}

		for (Example element : costs) {
			String transition = element.getValueAsString(transAttr);
			String variable = element.getValueAsString(variableAttr);

			double costMissing = element.getNumericalValue(costMissingAttr);
			double costWrong = element.getNumericalValue(costWrongAttr);

			if (!DoubleMath.isMathematicalInteger(costMissing)) {
				throw new ExampleSetReaderException("Only supports integer costs!");
			}
			if (!DoubleMath.isMathematicalInteger(costWrong)) {
				throw new ExampleSetReaderException("Only supports integer costs!");
			}

			VariableMatchCost cost = new VariableMatchCost((int) costWrong, (int) costMissing);

			if (transition == null && variable == null) {
				throw new ExampleSetReaderException("Missing transition and variable!");
			} else if (transition == null) {
				// for all transition
				cost.setVariable(variable);
			} else {
				// for specific variable / transition combination
				cost.setVariable(variable);
				cost.setActivity(transition);
			}

			variableCosts.add(cost);
		}

		return new VariableMatchCosts(variableCosts, activitySet, variables);
	}

	public ExampleSet writeCostsToExampleSet(VariableMatchCosts variableCosts) {

		Attribute transAttr = AttributeFactory.createAttribute(TRANSITION_COLUMN, Ontology.NOMINAL);
		Attribute eventClAttr = AttributeFactory.createAttribute(VARIABLE_COLUMN, Ontology.NOMINAL);
		Attribute costMissingAttr = AttributeFactory.createAttribute(COST_MISSING_COLUMN, Ontology.NUMERICAL);
		Attribute costWrongAttr = AttributeFactory.createAttribute(COST_WRONG_COLUMN, Ontology.NUMERICAL);

		MemoryExampleTable table = new MemoryExampleTable(transAttr, eventClAttr, costMissingAttr, costWrongAttr);
		//DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
		// TODO

		return table.createExampleSet();
	}

}
