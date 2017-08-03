package org.rapidprom.operators.analysis;

import java.util.List;
import org.processmining.dataawarecnetminer.plugins.CausalNetGEDComparison;
import org.rapidprom.ioobjects.CausalNetIOObject;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.tools.Ontology;

/**
 * Compares two Causal nets (C-nets) based on graph edit distance
 * 
 * @author F. Mannhardt
 *
 */
public class GEDCausalNetsOperator extends Operator {

	private static final String VALUE = "Value";
	private static final String NAME = "Name";

	private InputPort inputCNetA = getInputPorts().createPort("model A (ProM Causal net)", CausalNetIOObject.class);
	private InputPort inputCNetB = getInputPorts().createPort("model B (ProM Causal net)", CausalNetIOObject.class);

	private OutputPort outputDistance = getOutputPorts().createPort("GED distance (Example set)");

	public GEDCausalNetsOperator(OperatorDescription description) {
		super(description);
		ExampleSetMetaData metaData = new ExampleSetMetaData();
		AttributeMetaData amd1 = new AttributeMetaData(NAME, Ontology.STRING);
		amd1.setRole(AttributeColumn.REGULAR);
		amd1.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd1);
		AttributeMetaData amd2 = new AttributeMetaData(VALUE, Ontology.NUMERICAL);
		amd2.setRole(AttributeColumn.REGULAR);
		amd2.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd2);
		getTransformer().addRule(new GenerateNewMDRule(outputDistance, metaData));
	}

	@Override
	public void doWork() throws OperatorException {

		CausalNetIOObject netA = inputCNetA.getData(CausalNetIOObject.class);
		CausalNetIOObject netB = inputCNetB.getData(CausalNetIOObject.class);

		double distance = CausalNetGEDComparison.graphEditDistance(netA.getArtifact(), netB.getArtifact());

		Attribute name = AttributeFactory.createAttribute(NAME, Ontology.STRING);
		Attribute value = AttributeFactory.createAttribute(VALUE, Ontology.NUMERICAL);
		MemoryExampleTable table = new MemoryExampleTable(name, value);

		Attribute[] attributes = new Attribute[] { name, value };
		DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

		table.addDataRow(factory.create(new Object[] { "ged", distance }, attributes));

		outputDistance.deliver(table.createExampleSet());

	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		return params;
	}

}