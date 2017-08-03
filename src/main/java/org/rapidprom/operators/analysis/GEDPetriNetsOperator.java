package org.rapidprom.operators.analysis;

import java.util.List;
import org.processmining.models.graphbased.directed.DirectedGraph;
import org.processmining.models.graphbased.directed.DirectedGraphEdge;
import org.processmining.models.graphbased.directed.DirectedGraphNode;
import org.processmining.petrinets.analysis.gedsim.algorithms.GraphEditDistanceSimilarityAlgorithm;
import org.processmining.petrinets.analysis.gedsim.algorithms.impl.GraphEditDistanceSimilarityAStar;
import org.processmining.petrinets.analysis.gedsim.params.GraphEditDistanceSimilarityParameters;
import org.rapidprom.ioobjects.PetriNetIOObject;
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
 * Compares two Petri nets based on graph edit distance
 * 
 * @author F. Mannhardt
 *
 */
public class GEDPetriNetsOperator extends Operator {

	private static final String VALUE = "Value";
	private static final String NAME = "Name";

	private InputPort inputNetA = getInputPorts().createPort("model A (ProM Petri net)", PetriNetIOObject.class);
	private InputPort inputNetB = getInputPorts().createPort("model B (ProM Petri net)", PetriNetIOObject.class);

	private OutputPort outputDistance = getOutputPorts().createPort("GED distance (Example set)");

	public GEDPetriNetsOperator(OperatorDescription description) {
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

		PetriNetIOObject netA = inputNetA.getData(PetriNetIOObject.class);
		PetriNetIOObject netB = inputNetB.getData(PetriNetIOObject.class);

		GraphEditDistanceSimilarityParameters parameters = new GraphEditDistanceSimilarityParameters();
		parameters.setUsePureDistance(true);
		parameters.setLedCutOff(0.0);

		GraphEditDistanceSimilarityAlgorithm<DirectedGraph<? extends DirectedGraphNode, ? extends DirectedGraphEdge<?, ?>>> aStarGed = new GraphEditDistanceSimilarityAStar<>(
				parameters);
		
		double distance = aStarGed.compute(netA.getArtifact(), netB.getArtifact());
		
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