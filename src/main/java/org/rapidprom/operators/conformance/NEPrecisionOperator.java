package org.rapidprom.operators.conformance;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javassist.tools.rmi.ObjectNotFoundException;

import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.info.XLogInfo;
import org.deckfour.xes.info.XLogInfoFactory;
import org.deckfour.xes.info.impl.XLogInfoImpl;
import org.deckfour.xes.model.XLog;
import org.processmining.acceptingpetrinet.models.AcceptingPetriNet;
import org.processmining.acceptingpetrinet.models.impl.AcceptingPetriNetImpl;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.plugins.kutoolbox.logmappers.PetrinetLogMapper;
import org.processmining.plugins.neconformance.plugins.PetrinetEvaluatorPlugin;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.ioobjects.XLogIOObject;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

public class NEPrecisionOperator extends Operator {

	private static final String PARAMETER_1 = "Pruning (cut) threshold for the Automaton";
	
	private InputPort input1 = getInputPorts().createPort("model (PetriNet)", PetriNetIOObject.class);
	private InputPort input2 = getInputPorts().createPort("log (XLog)", XLogIOObject.class);


	private OutputPort outputMetrics = getOutputPorts().createPort("example set (Data Table)");

	//private ExampleSetMetaData metaData = null;

	private final String NAMECOL = "Name";
	private final String VALUECOL = "Value";

	public NEPrecisionOperator(OperatorDescription description) {
		super(description);
	}

	private static PetrinetLogMapper constructPetrinetLogMapper(final Petrinet net, final XLog log) {
		PetrinetLogMapper mapper = new PetrinetLogMapper(XLogInfoImpl.STANDARD_CLASSIFIER, log, net);

		//XEventClass dummyClass = new XEventClass("DUMMY", 99999);
		XLogInfo summary = XLogInfoFactory.createLogInfo(log, XLogInfoImpl.STANDARD_CLASSIFIER);
		
		for (Transition t : net.getTransitions()) {
			for (XEventClass evClass : summary.getEventClasses().getClasses()) {
				String id = evClass.getId();

				if (t.getLabel().equals(id) || (t.getLabel()+"+").equals(id) || (t.getLabel()+"+complete").equals(id)) {
					mapper.put(t, evClass);
					break;
				}
			}
		}

		return mapper;
	}
	
	public static double getNegativeEventsPrecision(int replayer, int inducer, final AcceptingPetriNet apn, final XLog log){
		PetrinetLogMapper mapper = constructPetrinetLogMapper(apn.getNet(), log);
		PetrinetEvaluatorPlugin.SUPPRESS_OUTPUT = true;
		
		// CoBeFra default settings for Negative Event Precision
		boolean useWeighted = true;
		boolean useBothRatios = false;
		boolean useCutOff = false;
		boolean multiThreaded = false;
		int negWindow = -1;
		int genWindow = -1;
		boolean unmappedRecall = true;
		boolean unmappedPrecision = true;
		boolean unmappedGeneralization = true;
		// CoBeFra default settings for Negative Event Precision

		return PetrinetEvaluatorPlugin.getMetricValue(log, apn.getNet(), apn.getInitialMarking(), mapper, 
				replayer, inducer, useWeighted, useBothRatios, useCutOff, negWindow, genWindow, 
				unmappedRecall, unmappedPrecision, unmappedGeneralization, multiThreaded, "precision");
	}
	
	@Override
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO,
				"Start: precision");
		long time = System.currentTimeMillis();

		PetriNetIOObject net = input1.getData(PetriNetIOObject.class);
		XLogIOObject log = input2.getData(XLogIOObject.class);
		
		double precision = 0d;
		try {
			precision = getNegativeEventsPrecision(0, 0, new AcceptingPetriNetImpl(net.getArtifact(), net.getInitialMarking(), net.getFinalMarkingAsArray()), log.getArtifact());
		} catch (ObjectNotFoundException e) {
			e.printStackTrace();
		}
			
		ExampleSet es = null;
		MemoryExampleTable table = null;
		List<Attribute> attributes = new LinkedList<Attribute>();
		attributes.add(AttributeFactory.createAttribute(this.NAMECOL, Ontology.STRING));
		attributes.add(AttributeFactory.createAttribute(this.VALUECOL, Ontology.NUMERICAL));
		table = new MemoryExampleTable(attributes);

		fillTableWithRow(table, "Precision", precision, attributes);
		
		es = table.createExampleSet();
		outputMetrics.deliver(es);

		logger.log(Level.INFO, "End: precision ("+ (System.currentTimeMillis() - time) / 1000 + " sec)");

	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterTypeDouble parameterType1 = new ParameterTypeDouble(PARAMETER_1, PARAMETER_1, 0d, 1d, 0d);
		parameterTypes.add(parameterType1);

		return parameterTypes;
	}

	private void fillTableWithRow(MemoryExampleTable table, String name,
			Object value, List<Attribute> attributes) {
		// fill table
		DataRowFactory factory = new DataRowFactory(
				DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
		Object[] vals = new Object[2];
		vals[0] = name;
		vals[1] = value;
		// convert the list to array
		Attribute[] attribArray = new Attribute[attributes.size()];
		for (int i = 0; i < attributes.size(); i++) {
			attribArray[i] = attributes.get(i);
		}
		DataRow dataRow = factory.create(vals, attribArray);
		table.addDataRow(dataRow);
	}
}