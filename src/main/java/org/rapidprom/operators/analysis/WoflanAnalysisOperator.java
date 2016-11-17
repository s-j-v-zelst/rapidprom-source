package org.rapidprom.operators.analysis;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.plugins.petrinet.behavioralanalysis.woflan.Woflan;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.ioobjects.WoflanDiagnosisIOObject;
import org.rapidprom.operators.util.ExecutorServiceRapidProM;

import com.google.common.util.concurrent.SimpleTimeLimiter;
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
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

public class WoflanAnalysisOperator extends Operator {

	private static final String PARAMETER_0_KEY = "Enable Time limit",
			PARAMETER_0_DESCR = "Tries to evaluate soundness within a given time period.",
			PARAMETER_1_KEY = "Time limit (sec)", PARAMETER_1_DESCR = "Time limit before the analysis is cancelled. "
					+ "Helpful when analyzing large Petri nets.";

	private final String NAMECOL = "Name";
	private final String VALUECOL = "Value";

	private InputPort input = getInputPorts().createPort("model (ProM Petri Net)", PetriNetIOObject.class);
	// private OutputPort outputWoflan = getOutputPorts().createPort("woflan
	// diagnosis (ProM WoflanDiagnosis)");
	private OutputPort outputWoflanString = getOutputPorts().createPort("woflan diagnosis (String)");

	private ExampleSetMetaData metaData = null;

	public WoflanAnalysisOperator(OperatorDescription description) {
		super(description);
		// getTransformer().addRule(new GenerateNewMDRule(outputWoflan,
		// WoflanDiagnosisIOObject.class));
		this.metaData = new ExampleSetMetaData();
		AttributeMetaData amd1 = new AttributeMetaData(NAMECOL, Ontology.STRING);
		amd1.setRole(AttributeColumn.REGULAR);
		amd1.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd1);
		AttributeMetaData amd2 = new AttributeMetaData(VALUECOL, Ontology.STRING);
		amd2.setRole(AttributeColumn.REGULAR);
		amd2.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd2);
		getTransformer().addRule(new GenerateNewMDRule(outputWoflanString, this.metaData));
	}

	public void doWork() throws OperatorException {

		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: woflan analysis");
		long time = System.currentTimeMillis();

		WoflanDiagnosisIOObject woflanDiagnosisIOObject = null;
		PluginContext pluginContext = RapidProMGlobalContext.instance().getFutureResultAwarePluginContext(Woflan.class);
		SimpleTimeLimiter limiter = new SimpleTimeLimiter(new ExecutorServiceRapidProM(pluginContext));

		MemoryExampleTable table = null;
		List<Attribute> attributes = new LinkedList<Attribute>();
		attributes.add(AttributeFactory.createAttribute(this.NAMECOL, Ontology.STRING));
		attributes.add(AttributeFactory.createAttribute(this.VALUECOL, Ontology.STRING));
		table = new MemoryExampleTable(attributes);

		String highLevelResult = "";
		String lowLevelResult = "";
		try {

			if (getParameterAsBoolean(PARAMETER_0_KEY))
				woflanDiagnosisIOObject = limiter.callWithTimeout(new WOFLANER(pluginContext),
						getParameterAsInt(PARAMETER_1_KEY), TimeUnit.SECONDS, true);
			else
				woflanDiagnosisIOObject = limiter.callWithTimeout(new WOFLANER(pluginContext), Long.MAX_VALUE,
						TimeUnit.SECONDS, true);

			highLevelResult = woflanDiagnosisIOObject.getArtifact().isSound() ? "Sound" : "Unsound";
			lowLevelResult = woflanDiagnosisIOObject.getArtifact().toString().replace("\n","");

		} catch (TimeoutException e1) {

			highLevelResult = "Timeout";
			lowLevelResult = " Woflan could not evaluate soundness in the given time.";
			logger.log(Level.WARNING, lowLevelResult);
		} catch (Exception e) {
			highLevelResult = "Error";
			lowLevelResult = " Woflan could not evaluate the Petri net";
			logger.log(Level.SEVERE, lowLevelResult);
			e.printStackTrace();
		}

		fillTableWithRow(table, "High-Level Diagnosis", highLevelResult, attributes);
		fillTableWithRow(table, "Detailed Diagnosis", lowLevelResult, attributes);

		ExampleSet es = table.createExampleSet();
		outputWoflanString.deliver(es);

		logger.log(Level.INFO, "End: woflan analysis (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	public List<ParameterType> getParameterTypes() {

		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterTypeBoolean parameter0 = new ParameterTypeBoolean(PARAMETER_0_KEY, PARAMETER_0_DESCR, true);
		parameterTypes.add(parameter0);

		ParameterTypeInt parameter1 = new ParameterTypeInt(PARAMETER_1_KEY, PARAMETER_1_DESCR, 0, 10000, 60);
		parameterTypes.add(parameter1);

		return parameterTypes;
	}

	class WOFLANER implements Callable<WoflanDiagnosisIOObject> {

		private final PluginContext pluginContext;

		public WOFLANER(PluginContext input) {
			pluginContext = input;
		}

		@Override
		public WoflanDiagnosisIOObject call() throws Exception {
			PetriNetIOObject petriNet = input.getData(PetriNetIOObject.class);
			Woflan woflan = new Woflan();
			return new WoflanDiagnosisIOObject(woflan.diagnose(pluginContext, petriNet.getArtifact()), pluginContext);
		}
	}

	private void fillTableWithRow(MemoryExampleTable table, String name, Object value, List<Attribute> attributes) {
		// fill table
		DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
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
