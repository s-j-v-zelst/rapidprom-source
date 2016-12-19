package org.rapidprom.operators.conformance.util;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.classification.XEventClasses;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.rapidprom.exceptions.ExampleSetReaderException;

import com.google.common.collect.Ordering;
import com.google.common.math.DoubleMath;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.tools.Ontology;

/**
 * Reads and writes costs for use with an alignment from an Example set.
 * 
 * @author F. Mannhardt
 *
 */
public class AlignmentCostIO {

	public static final String COST_COLUMN = "cost";
	public static final String EVENTCLASS_COLUMN = "eventclass";
	public static final String TRANSITION_COLUMN = "transition";

	public void readCostsFromExampleSet(ExampleSet costs, XEventClasses eventClasses,
			Map<String, Transition> transitions, Map<XEventClass, Integer> logCost, Map<Transition, Integer> modelCost)
			throws ExampleSetReaderException {

		Attribute transAttr = costs.getAttributes().get(TRANSITION_COLUMN);
		Attribute eventClAttr = costs.getAttributes().get(EVENTCLASS_COLUMN);
		Attribute costAttr = costs.getAttributes().get(COST_COLUMN);
		
		if (costAttr == null || transAttr == null || eventClAttr == null) {
			throw new ExampleSetReaderException("Missing columns!");
		}
		
		for (Example element : costs) {
			String transition = element.getValueAsString(transAttr);
			String eventClass = element.getValueAsString(eventClAttr);
			double cost = element.getNumericalValue(costAttr);
			if (!DoubleMath.isMathematicalInteger(cost)) {
				throw new ExampleSetReaderException("Only supports integer costs!");
			}
			if (transition == null && eventClass == null) {
				throw new ExampleSetReaderException("Missing transition and event class!");
			} else if (transition == null) {
				// log cost
				XEventClass e = eventClasses.getByIdentity(eventClass);
				if (e != null) {
					logCost.put(e, (int) cost);
				} else {
					throw new ExampleSetReaderException("Could not find event class " + eventClass);
				}
			} else if (eventClass == null) {
				// model cost
				Transition t = transitions.get(transition);
				if (t != null) {
					modelCost.put(t, (int) cost);
				} else {
					throw new ExampleSetReaderException("Could not find transition " + transition);
				}
			} else {
				// sync cost
				// not supported
			}
		}
	}

	private static final Comparator<Entry<XEventClass, Integer>> EVENTCLASS_ORDER = new Comparator<Entry<XEventClass, Integer>>() {

		@Override
		public int compare(Entry<XEventClass, Integer> o1, Entry<XEventClass, Integer> o2) {
			return o1.getKey().getId().compareTo(o2.getKey().getId());
		}
	};

	private static final Comparator<Entry<Transition, Integer>> TRANSITION_ORDER = new Comparator<Entry<Transition, Integer>>() {

		@Override
		public int compare(Entry<Transition, Integer> o1, Entry<Transition, Integer> o2) {
			return o1.getKey().getLabel().compareTo(o2.getKey().getLabel());
		}
	};

	public ExampleSet writeCostsToExampleSet(Map<XEventClass, Integer> logCost, Map<Transition, Integer> modelCost) {

		Attribute transAttr = AttributeFactory.createAttribute(TRANSITION_COLUMN, Ontology.NOMINAL);
		Attribute eventClAttr = AttributeFactory.createAttribute(EVENTCLASS_COLUMN, Ontology.NOMINAL);
		Attribute costAttr = AttributeFactory.createAttribute(COST_COLUMN, Ontology.NUMERICAL);

		MemoryExampleTable table = new MemoryExampleTable(transAttr, eventClAttr, costAttr);

		DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

		for (Entry<XEventClass, Integer> entry : Ordering.from(EVENTCLASS_ORDER).sortedCopy(logCost.entrySet())) {
			DataRow row = factory.create(3);
			row.set(transAttr, Double.NaN);
			row.set(eventClAttr, eventClAttr.getMapping().mapString(entry.getKey().getId()));
			row.set(costAttr, entry.getValue());
			table.addDataRow(row);
		}

		for (Entry<Transition, Integer> entry : Ordering.from(TRANSITION_ORDER).sortedCopy(modelCost.entrySet())) {
			DataRow row = factory.create(3);
			row.set(transAttr, transAttr.getMapping().mapString(entry.getKey().getLabel()));
			row.set(eventClAttr, Double.NaN);
			row.set(costAttr, entry.getValue());
			table.addDataRow(row);
		}

		return table.createExampleSet();
	}

}
