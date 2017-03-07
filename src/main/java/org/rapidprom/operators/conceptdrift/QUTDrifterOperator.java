package org.rapidprom.operators.conceptdrift;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.processmining.variantfinder.algorithms.QUTDrifter;

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
import com.rapidminer.operator.nio.file.FileObject;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeFile;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

public class QUTDrifterOperator extends Operator {

	public static final String PARAMETER_1_KEY = "Method:", PARAMETER_1_DESCR = "";

	private InputPort input = getInputPorts().createPort("file", FileObject.class);
	private OutputPort output = getOutputPorts().createPort("drift points");

	public QUTDrifterOperator(OperatorDescription description) {
		super(description);
		// TODO Auto-generated constructor stub
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
		logger.log(Level.INFO, "Start: detecting concept drift (QUT)");
		long time = System.currentTimeMillis();

		List<Date> driftPoints = QUTDrifter.doer(input.getData(FileObject.class).getFile().getAbsolutePath(),
				getParameterAsString(PARAMETER_1_KEY));

		fillDriftPoints(driftPoints);

		logger.log(Level.INFO,
				"End: detecting concept drift (QUT) (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterTypeCategory par1 = new ParameterTypeCategory(PARAMETER_1_KEY, PARAMETER_1_DESCR,
				new String[] { QUTDrifter.RUNS, QUTDrifter.EVENTS }, 0);
		parameterTypes.add(par1);

		return parameterTypes;
	}

	private void fillDriftPoints(List<Date> drifts) {
		ExampleSet es = null;
		MemoryExampleTable table = null;
		List<Attribute> attributes = new LinkedList<Attribute>();
		attributes.add(AttributeFactory.createAttribute("Drift ID", Ontology.NUMERICAL));
		attributes.add(AttributeFactory.createAttribute("Time (ms)", Ontology.NUMERICAL));
		table = new MemoryExampleTable(attributes);

		if (drifts != null) {
			int traceID = 0;
			Iterator<Date> iterator = drifts.iterator();
			DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

			while (iterator.hasNext()) {
				Date next = iterator.next();
				Object[] vals = new Object[2];
				vals[0] = traceID;
				vals[1] = next.getTime();

				DataRow dataRow = factory.create(vals, attributes.toArray(new Attribute[attributes.size()]));
				table.addDataRow(dataRow);
				traceID++;
			}
		}

		es = table.createExampleSet();
		output.deliver(es);
	}

}
