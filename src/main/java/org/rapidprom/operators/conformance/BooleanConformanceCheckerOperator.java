package org.rapidprom.operators.conformance;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.model.XLog;
import org.processmining.models.semantics.petrinet.Marking;
import org.processmining.plugins.balancedconformance.BalancedDataXAlignmentPlugin;
import org.processmining.plugins.balancedconformance.config.BalancedProcessorConfiguration;
import org.processmining.plugins.balancedconformance.controlflow.ControlFlowAlignmentException;
import org.processmining.plugins.balancedconformance.dataflow.exception.DataAlignmentException;
import org.processmining.plugins.balancedconformance.observer.DataConformancePlusObserverNoOpImpl;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMDiscoveryOperator;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

import javassist.tools.rmi.ObjectNotFoundException;

public class BooleanConformanceCheckerOperator extends AbstractRapidProMDiscoveryOperator {

	private static final String NAMECOL = "case id", VALUECOL = "fits model?";

	private InputPort inputPN = getInputPorts().createPort("model (ProM Petri Net)", PetriNetIOObject.class);

	private OutputPort output = getOutputPorts().createPort("example set (Data Table)");

	private ExampleSetMetaData metaData = null;

	public BooleanConformanceCheckerOperator(OperatorDescription description) {
		super(description);

		this.metaData = new ExampleSetMetaData();
		AttributeMetaData amd1 = new AttributeMetaData(NAMECOL, Ontology.STRING);
		amd1.setRole(AttributeColumn.REGULAR);
		amd1.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd1);
		AttributeMetaData amd2 = new AttributeMetaData(VALUECOL, Ontology.NUMERICAL);
		amd2.setRole(AttributeColumn.REGULAR);
		amd2.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd2);
		metaData.setNumberOfExamples(1);
		getTransformer().addRule(new GenerateNewMDRule(output, this.metaData));
	}

	@Override
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: replay log on petri net for boolean conformance checking");
		long time = System.currentTimeMillis();

		XLog log = getXLog();
		PetriNetIOObject petriNet = inputPN.getData(PetriNetIOObject.class);

		final Map<String, Boolean> nonFittingTraces = new HashMap<String, Boolean>();
		for (int i = 0; i < log.size(); i++)
			nonFittingTraces.put(XConceptExtension.instance().extractName(log.get(i)), true);

		BalancedDataXAlignmentPlugin balanced = new BalancedDataXAlignmentPlugin();

		Marking[] finalMarking = new Marking[1];
		BalancedProcessorConfiguration conformanceSettings;
		try {
			finalMarking[0] = petriNet.getFinalMarking();
			conformanceSettings = BalancedProcessorConfiguration.newDefaultInstance(petriNet.getArtifact(),
					petriNet.getInitialMarking(), finalMarking, log, getXEventClassifier(), 1, 1, 1, 1);
			conformanceSettings.setMaxCostFactor(1);
			conformanceSettings.setObserver(new DataConformancePlusObserverNoOpImpl() {

				public void foundImpossibleAlignments(Collection<ImpossibleTrace> impossibleTraces) {
					for (ImpossibleTrace trace : impossibleTraces)
						nonFittingTraces.put(XConceptExtension.instance().extractName(trace.getTrace()), false);
				}
			});

			balanced.alignLog(petriNet.getArtifact(), log, conformanceSettings);

		} catch (ObjectNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ControlFlowAlignmentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DataAlignmentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		ExampleSet es = null;
		MemoryExampleTable table = null;
		List<Attribute> attributes = new LinkedList<Attribute>();
		attributes.add(AttributeFactory.createAttribute(BooleanConformanceCheckerOperator.NAMECOL, Ontology.STRING));
		attributes
				.add(AttributeFactory.createAttribute(BooleanConformanceCheckerOperator.VALUECOL, Ontology.NUMERICAL));
		table = new MemoryExampleTable(attributes);

		Attribute[] attribArray = new Attribute[attributes.size()];
		for (int i = 0; i < attributes.size(); i++) {
			attribArray[i] = attributes.get(i);
		}

		DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
		Object[] vals = new Object[2];

		for (String s : nonFittingTraces.keySet()) {

			vals[0] = s;

			if (nonFittingTraces.get(s))
				vals[1] = 1;
			else
				vals[1] = 0;
			DataRow dataRow = factory.create(vals, attribArray);
			table.addDataRow(dataRow);
		}
		es = table.createExampleSet();
		output.deliver(es);
		logger.log(Level.INFO, "End: replay log on petri net for boolean conformance checking ("
				+ (System.currentTimeMillis() - time) / 1000 + " sec)");

	}

}
