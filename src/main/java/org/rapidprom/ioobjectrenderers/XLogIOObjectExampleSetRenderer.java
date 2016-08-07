package org.rapidprom.ioobjectrenderers;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.swing.JComponent;

import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.util.XLogUtils;
import org.rapidprom.util.XLogUtils.AttributeTypes;
import org.rapidprom.util.XLogUtils.TableModelXLog;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.gui.renderer.data.ExampleSetDataRenderer;
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.operator.ports.metadata.SetRelation;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.math.container.Range;

public class XLogIOObjectExampleSetRenderer extends AbstractRapidProMIOObjectRenderer<XLogIOObject> {

	private Attribute[] attributes;

	@Override
	public String getName() {
		return "XLog (ExampleSet)  renderer";
	}

	@Override
	protected JComponent runVisualization(XLogIOObject ioObject) {

		XLogIOObject object = (XLogIOObject) ioObject;

		MemoryExampleTable table = null;
		ExampleSet es = null;

		try {
			TableModelXLog convertLogToStringTable = XLogUtils.convertLogToStringTable(object.getArtifact(), true);

			table = createStructureTable(convertLogToStringTable);
			es = fillTable(table, convertLogToStringTable);

		} catch (Exception error) {
			error.printStackTrace();
		}
		ExampleSetDataRenderer renderer = new ExampleSetDataRenderer();
		return (JComponent) renderer.getVisualizationComponent(es, null);
	}

	private MemoryExampleTable createStructureTable(TableModelXLog convertedLog) {
		ExampleSetMetaData metaData = new ExampleSetMetaData();
		List<Attribute> attributes = new LinkedList<Attribute>();
		for (int i = 0; i < convertedLog.getColumnCount(); i++) {
			String columnName = convertedLog.getColumnName(i);
			AttributeTypes columnType = convertedLog.getColumnType(i);
			AttributeMetaData amd = null;
			if (columnType.equals(AttributeTypes.CONTINUOUS)) {
				attributes.add(AttributeFactory.createAttribute(columnName, Ontology.NUMERICAL));
				amd = new AttributeMetaData(columnName, Ontology.NUMERICAL);
				amd.setRole(AttributeColumn.REGULAR);
				amd.setNumberOfMissingValues(new MDInteger(0));
				List<Double> minAndMaxValueColumn = getMinAndMaxValueColumn(convertedLog, columnName);
				amd.setValueRange(new Range(minAndMaxValueColumn.get(0), minAndMaxValueColumn.get(1)),
						SetRelation.EQUAL);
			} else if (columnType.equals(AttributeTypes.DISCRETE)) {
				attributes.add(AttributeFactory.createAttribute(columnName, Ontology.NOMINAL));
				amd = new AttributeMetaData(columnName, Ontology.NOMINAL);
				amd.setRole(AttributeColumn.REGULAR);
				amd.setNumberOfMissingValues(new MDInteger(0));
			} else if (columnType.equals(AttributeTypes.DATE)) {
				attributes.add(AttributeFactory.createAttribute(columnName, Ontology.DATE_TIME));
				amd = new AttributeMetaData(columnName, Ontology.DATE_TIME);
				amd.setRole(AttributeColumn.REGULAR);
				amd.setNumberOfMissingValues(new MDInteger(0));
				List<Double> minAndMaxValueColumn = getMinAndMaxValueColumn(convertedLog, columnName);
				amd.setValueRange(new Range(minAndMaxValueColumn.get(0), minAndMaxValueColumn.get(1)),
						SetRelation.EQUAL);
			} else if (columnType.equals(AttributeTypes.STRING)) {
				attributes.add(AttributeFactory.createAttribute(columnName, Ontology.NOMINAL));
				amd = new AttributeMetaData(columnName, Ontology.NOMINAL);
				amd.setRole(AttributeColumn.REGULAR);
				amd.setNumberOfMissingValues(new MDInteger(0));
			} else if (columnType.equals(AttributeTypes.BOOLEAN)) {
				attributes.add(AttributeFactory.createAttribute(columnName, Ontology.BINOMINAL));
				amd = new AttributeMetaData(columnName, Ontology.NOMINAL);
				amd.setRole(AttributeColumn.REGULAR);
				amd.setNumberOfMissingValues(new MDInteger(0));
			}
			metaData.addAttribute(amd);
		}
		// convert the list to array
		Attribute[] attribArray = new Attribute[attributes.size()];
		for (int i = 0; i < attributes.size(); i++) {
			attribArray[i] = attributes.get(i);
		}
		metaData.setNumberOfExamples(convertedLog.getRowCount());

		this.attributes = attribArray;
		MemoryExampleTable memoryExampleTable = new MemoryExampleTable(attributes);
		return memoryExampleTable;
	}

	private ExampleSet fillTable(MemoryExampleTable table, TableModelXLog convertedLog) {
		DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
		// now add per row
		for (int i = 0; i < convertedLog.getRowCount(); i++) {
			// fill strings
			String[] strings = new String[convertedLog.getColumnCount()];
			for (int j = 0; j < convertedLog.getColumnCount(); j++) {
				strings[j] = convertedLog.getValueAt(i, j).toString();
			}
			DataRow dataRow = factory.create(strings, attributes);
			table.addDataRow(dataRow);
		}
		ExampleSet createExampleSet = table.createExampleSet();
		return createExampleSet;
	}

	private List<Double> getMinAndMaxValueColumn(TableModelXLog convertedLog, String nameCol) {
		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;
		int intCol = convertedLog.getNameForColumn(nameCol);
		for (int i = 0; i < convertedLog.getRowCount(); i++) {
			Object valueAt = convertedLog.getValueAt(i, intCol);
			if (valueAt instanceof String) {
				try {
					double parseDouble = Double.parseDouble((String) valueAt);
					min = parseDouble < min ? parseDouble : min;
					max = parseDouble > max ? parseDouble : max;
				} catch (Exception e) {
					// do nothing with it.
				}
			}
		}
		List<Double> doubleList = new ArrayList<Double>();
		doubleList.add(min);
		doubleList.add(max);
		return doubleList;
	}
}
