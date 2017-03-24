package org.rapidprom.operators.analysis;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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

import com.rapidminer.example.Attribute;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.tools.Ontology;

import javassist.tools.rmi.ObjectNotFoundException;

public class PetriNetMetricsOperatorImpl extends Operator {

	private InputPort inputPetriNet = getInputPorts().createPort("model (Petri Net)", PetriNetIOObject.class);

	private OutputPort outputMetricsExampleSet = getOutputPorts().createPort("metrics (Example Set)");

	private ExampleSetMetaData outputMetricsMetaData = null;

	private final String[] METRICS = new String[] { PetriNetCardosoMetric.NAME, PetriNetCyclomaticMetric.NAME,
			PetriNetDensityMetric.NAME, PetriNetNofArcsMetric.NAME, PetriNetNofPlacesMetric.NAME,
			PetriNetNofTransitionsMetric.NAME, PetriNetStructurednessMetric.NAME };

	public PetriNetMetricsOperatorImpl(OperatorDescription description) {
		super(description);
		outputMetricsMetaData = new ExampleSetMetaData();
		for (String metric : METRICS) {
			AttributeMetaData amd = new AttributeMetaData(metric, Ontology.NUMERICAL);
			amd.setNumberOfMissingValues(new MDInteger(0));
			outputMetricsMetaData.addAttribute(amd);
		}
		getTransformer().addRule(new GenerateNewMDRule(outputMetricsExampleSet, outputMetricsMetaData));
	}

	@Override
	public void doWork() throws OperatorException {
		PetriNetIOObject net = getPetriNet();
		try {
			PetriNetMetrics metrics = new PetriNetMetrics(RapidProMGlobalContext.instance().getPluginContext(),
					net.getArtifact(), net.getInitialMarking());

			List<Attribute> metricAttributes = new ArrayList<>();
			for (String metric : METRICS) {
				metricAttributes.add(AttributeFactory.createAttribute(metric, Ontology.NUMERICAL));
			}
			MemoryExampleTable metricsTable = new MemoryExampleTable(metricAttributes);

			Object[] metricValues = new Object[METRICS.length];
			for (int i = 0; i < METRICS.length; i++) {
				metricValues[i] = metrics.getMetricValue(METRICS[i]);
			}
			metricsTable.addDataRow(new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.').create(metricValues,
					metricAttributes.toArray(new Attribute[metricAttributes.size()])));

			outputMetricsExampleSet.deliver(metricsTable.createExampleSet());
		} catch (ObjectNotFoundException e) {
			throw new OperatorException(e.getMessage());
		}

	}

	private PetriNetIOObject getPetriNet() throws UserError {
		return inputPetriNet.getData(PetriNetIOObject.class);
	}

}
