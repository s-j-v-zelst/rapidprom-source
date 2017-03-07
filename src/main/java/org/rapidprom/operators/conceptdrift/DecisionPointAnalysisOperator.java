package org.rapidprom.operators.conceptdrift;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.models.graphbased.AttributeMap;
import org.processmining.models.graphbased.directed.transitionsystem.State;
import org.processmining.plugins.tsanalyzer2.AnnotatedTransitionSystem;
import org.processmining.processcomparator.algorithms.EventToStateTranslator;
import org.processmining.processcomparator.algorithms.TransitionSystemUtils;
import org.processmining.processcomparator.model.TsSettingsObject;
import org.processmining.variantfinder.algorithms.VariantFinder;
import org.processmining.variantfinder.models.SplittingPoint;
import org.processmining.variantfinder.parameters.Settings;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.TransitionSystemIOObject;
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
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

public class DecisionPointAnalysisOperator extends Operator {

	public static final String PARAMETER_1_KEY = "Confidence Threshold", PARAMETER_1_DESCR = "",
			PARAMETER_2_KEY = "Min. percentage in Leaf",
			PARAMETER_2_DESCR = "The minimum percentage of instances (cases) that shoud land on a leaf, otherwise it will be pruned";

	private InputPort input1 = getInputPorts().createPort("event log", XLogIOObject.class);
	private InputPort input2 = getInputPorts().createPort("transition system", TransitionSystemIOObject.class);

	private OutputPort output = getOutputPorts().createPort("drift points");

	public DecisionPointAnalysisOperator(OperatorDescription description) {
		super(description);

		ExampleSetMetaData md1 = new ExampleSetMetaData();
		AttributeMetaData amd1 = new AttributeMetaData("Drift ID", Ontology.NUMERICAL);
		amd1.setRole(AttributeColumn.REGULAR);
		amd1.setNumberOfMissingValues(new MDInteger(0));
		md1.addAttribute(amd1);
		AttributeMetaData amd4 = new AttributeMetaData("Time (ms)", Ontology.NUMERICAL);
		amd4.setRole(AttributeColumn.REGULAR);
		amd4.setNumberOfMissingValues(new MDInteger(0));
		md1.addAttribute(amd4);

		getTransformer().addRule(new GenerateNewMDRule(output, md1));
	}

	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: detecting concept drift");
		long time = System.currentTimeMillis();

		XLogIOObject log = input1.getData(XLogIOObject.class);

		TransitionSystemIOObject ts = input2.getData(TransitionSystemIOObject.class);

		PluginContext pluginContext = RapidProMGlobalContext.instance().getPluginContext();

		for (State s : ts.getArtifact().getNodes())
			s.setLabel(s.getAttributeMap().get(AttributeMap.TOOLTIP).toString());

		AnnotatedTransitionSystem ats = TransitionSystemUtils.createATS(pluginContext, ts.getArtifact(),
				log.getArtifact());

		EventToStateTranslator.setTS(ts.getArtifact());
		EventToStateTranslator.setSettings(new TsSettingsObject(ts.getSettings()));

		Set<SplittingPoint> processVariants = VariantFinder.findProcessVariants(log.getArtifact(), getSettigns(),
				pluginContext, ats);

		SplittingPoint winner = processVariants.iterator().next();

		fillDriftPoints(winner.getSplittingPoint());

		logger.log(Level.INFO, "End: detecting concept drift (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();
		
		ParameterTypeDouble par1 = new ParameterTypeDouble(PARAMETER_1_KEY, PARAMETER_1_DESCR, 0, 1, 1);
		ParameterTypeDouble par2 = new ParameterTypeDouble(PARAMETER_2_KEY, PARAMETER_2_DESCR, 0, 1, 0.05);

		parameterTypes.add(par1);
		parameterTypes.add(par2);
		
		return parameterTypes;
	}

	public Settings getSettigns() {
		
		Settings settings = null;
		try {
			settings = new Settings(getParameterAsDouble(PARAMETER_1_KEY), getParameterAsDouble(PARAMETER_2_KEY));
		} catch (UndefinedParameterError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return settings;

	}

	@SuppressWarnings("deprecation")
	private void fillDriftPoints(List<Object> drifts) {
		ExampleSet es = null;
		MemoryExampleTable table = null;
		List<Attribute> attributes = new LinkedList<Attribute>();
		attributes.add(AttributeFactory.createAttribute("Drift ID", Ontology.NUMERICAL));
		attributes.add(AttributeFactory.createAttribute("Time (ms)", Ontology.NUMERICAL));
		table = new MemoryExampleTable(attributes);

		if (drifts != null) {
			int traceID = 0;
			Iterator<Object> iterator = drifts.iterator();
			DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

			while (iterator.hasNext()) {
				Object next = iterator.next();
				Object[] vals = new Object[2];
				vals[0] = traceID;
				vals[1] = parseToDate((String) next).getTime();

				DataRow dataRow = factory.create(vals, attributes.toArray(new Attribute[attributes.size()]));
				table.addDataRow(dataRow);
				traceID++;
			}
		} else {
			DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
			Object[] vals = new Object[2];
			vals[0] = "?";
			vals[1] = Double.NaN;

			DataRow dataRow = factory.create(vals, attributes.toArray(new Attribute[attributes.size()]));
			table.addDataRow(dataRow);
		}

		es = table.createExampleSet();
		output.deliver(es);
	}

	public static Date parseToDate(String date) {
		SimpleDateFormat df2 = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		Date result = null;
		try {
			result = df2.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

}
