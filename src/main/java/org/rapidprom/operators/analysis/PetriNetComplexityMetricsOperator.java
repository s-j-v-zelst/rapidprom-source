package org.rapidprom.operators.analysis;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.pnanalysis.metrics.impl.PetriNetCardosoMetric;
import org.processmining.pnanalysis.metrics.impl.PetriNetCyclomaticMetric;
import org.processmining.pnanalysis.metrics.impl.PetriNetDensityMetric;
import org.processmining.pnanalysis.metrics.impl.PetriNetNofArcsMetric;
import org.processmining.pnanalysis.metrics.impl.PetriNetNofPlacesMetric;
import org.processmining.pnanalysis.metrics.impl.PetriNetNofTransitionsMetric;
import org.processmining.pnanalysis.metrics.impl.PetriNetStructurednessMetric;
import org.processmining.pnanalysis.models.PetriNetMetrics;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.operators.util.RapidProMProcessSetupError;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ProcessSetupError.Severity;
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.tools.Ontology;

import javassist.tools.rmi.ObjectNotFoundException;


public class PetriNetComplexityMetricsOperator extends Operator {

	private static final String VALUE = "Value";
	private static final String NAME = "Name";

	private InputPort input = getInputPorts().createPort("model (ProM Petri Net)", PetriNetIOObject.class);
	private OutputPort outputMetrics = getOutputPorts().createPort("metrics (Example set)");

	public PetriNetComplexityMetricsOperator(OperatorDescription description) {
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
		getTransformer().addRule(new GenerateNewMDRule(outputMetrics, metaData));
	}

	@Override
	public void doWork() throws OperatorException {

		PetriNetIOObject petriNetIO = input.getData(PetriNetIOObject.class);

		try {
			PetriNetMetrics metrics = new PetriNetMetrics(getContext(), petriNetIO.getArtifact(),
					petriNetIO.getInitialMarking());

			Attribute name = AttributeFactory.createAttribute(NAME, Ontology.STRING);
			Attribute value = AttributeFactory.createAttribute(VALUE, Ontology.NUMERICAL);
			MemoryExampleTable table = new MemoryExampleTable(name, value);

			Attribute[] attributes = new Attribute[] { name, value };
			DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

			table.addDataRow(createMetricRow(metrics, PetriNetCardosoMetric.NAME, attributes, factory));
			table.addDataRow(createMetricRow(metrics, PetriNetCyclomaticMetric.NAME, attributes, factory));
			table.addDataRow(createMetricRow(metrics, PetriNetStructurednessMetric.NAME, attributes, factory));
			table.addDataRow(createMetricRow(metrics, PetriNetDensityMetric.NAME, attributes, factory));
			table.addDataRow(createMetricRow(metrics, PetriNetNofArcsMetric.NAME, attributes, factory));
			table.addDataRow(createMetricRow(metrics, PetriNetNofTransitionsMetric.NAME, attributes, factory));
			table.addDataRow(createMetricRow(metrics, PetriNetNofPlacesMetric.NAME, attributes, factory));

			outputMetrics.deliver(table.createExampleSet());

		} catch (ObjectNotFoundException e) {
			addError(new RapidProMProcessSetupError(Severity.ERROR, getPortOwner(), e));
		}
	}

	private DataRow createMetricRow(PetriNetMetrics metrics, String metricName, Attribute[] attributes,
			DataRowFactory factory) {
		return factory.create(new Object[] { metricName, metrics.getMetricValue(metricName) }, attributes);
	}

	private PluginContext getContext() throws UserError {
		return RapidProMGlobalContext.instance().getPluginContext();
	}

}
