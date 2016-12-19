package org.rapidprom.operators.conformance.util;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.rapidprom.exceptions.ExampleSetReaderException;

import com.google.common.collect.Ordering;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.tools.Ontology;

public class VariableMappingIO {

	public static final String VARIABLE_COLUMN = "variable";
	public static final String ATTRIBUTE_COLUMN = "attribute";

	/**
	 * @param data
	 * @return a mapping from variable name to attribute name
	 * @throws ExampleSetReaderException
	 */
	public Map<String, String> readVariableMapping(ExampleSet data) throws ExampleSetReaderException {

		Attribute variableAttr = data.getAttributes().get(VARIABLE_COLUMN);
		Attribute attributeAttr = data.getAttributes().get(ATTRIBUTE_COLUMN);

		if (variableAttr == null || attributeAttr == null) {
			throw new ExampleSetReaderException("Missing columns!");
		}

		Map<String, String> variableMapping = new HashMap<>();

		for (Example element : data) {
			String variable = element.getValueAsString(variableAttr);
			String attribute = element.getValueAsString(attributeAttr);

			if (variable == null && attribute == null) {
				throw new ExampleSetReaderException("Missing transition and variable!");
			} else if (attribute == null) {
				// no mapping
			} else {
				// for specific variable / transition combination
				variableMapping.put(variable, attribute);
			}
		}

		return variableMapping;
	}

	private static final Comparator<Entry<String, String>> VARIABLE_ORDER = new Comparator<Entry<String, String>>() {

		@Override
		public int compare(Entry<String, String> o1, Entry<String, String> o2) {
			return o1.getKey().compareTo(o2.getKey());
		}
	};

	/**
	 * @param variableMapping
	 * @return an example set of the variable mapping
	 */
	public ExampleSet writeVariableMapping(Map<String, String> variableMapping) {
		Attribute variableAttr = AttributeFactory.createAttribute(VARIABLE_COLUMN, Ontology.NOMINAL);
		Attribute attributeAttr = AttributeFactory.createAttribute(ATTRIBUTE_COLUMN, Ontology.NOMINAL);

		MemoryExampleTable table = new MemoryExampleTable(variableAttr, attributeAttr);

		DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

		for (Entry<String, String> entry : Ordering.from(VARIABLE_ORDER).sortedCopy(variableMapping.entrySet())) {
			DataRow row = factory.create(3);
			row.set(variableAttr, variableAttr.getMapping().mapString(entry.getKey()));
			row.set(attributeAttr, attributeAttr.getMapping().mapString(entry.getValue()));
			table.addDataRow(row);
		}

		return table.createExampleSet();
	}

}
