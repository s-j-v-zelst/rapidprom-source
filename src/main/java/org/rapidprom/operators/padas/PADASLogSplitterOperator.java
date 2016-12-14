package org.rapidprom.operators.padas;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.set.SimpleExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DoubleArrayDataRow;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

public class PADASLogSplitterOperator extends Operator {

	public static final String PARAMETER_1 = "Log Name";
	public static final String PARAMETER_2 = "Process Name";
	public static final String PARAMETER_3 = "Settings"; // FIXME

	private InputPort inputDMClasses = getInputPorts().createPort (
			"Classes definition", ExampleSet.class);
	
	private InputPort inputDMKeys = getInputPorts().createPort(
			"Keys definition", ExampleSet.class);

	private InputPort inputEventsCollection = getInputPorts().createPort(
			"Events collection", new IOObjectCollection<ExampleSet>().getClass());
	
	private OutputPort outputLog = getOutputPorts().createPort(
			"Log mapping");
	
	private MetaData outputMD = new MetaData(ExampleSet.class);
	
	public PADASLogSplitterOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputLog, ExampleSet.class));
		//outputMMSet.deliverMD(outputMD);
	}
	
	@Override
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: padas log splitter");
		long time = System.currentTimeMillis();
		
		boolean failed = true;
		String msgFailure = ""; // FIXME
		
		Attribute atLogId = AttributeFactory.createAttribute(
				PADASConstants.LOGS_LOG_ID_FIELD,
				Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
		Attribute atLogProcId = AttributeFactory.createAttribute(
				PADASConstants.LOGS_PROC_ID_FIELD,
				Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
		Attribute atLogTraceId = AttributeFactory.createAttribute(
				PADASConstants.LOGS_TRACE_ID_FIELD,
				Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
		Attribute atLogEvId = AttributeFactory.createAttribute(
				PADASConstants.LOGS_EVENT_ID_FIELD,
				Ontology.ATTRIBUTE_VALUE_TYPE.NUMERICAL);
		
		List<Attribute> listOfAtts = new ArrayList<>();
		listOfAtts.add(atLogId);
		listOfAtts.add(atLogProcId);
		listOfAtts.add(atLogTraceId);
		listOfAtts.add(atLogEvId);

		String logName = getParameter(PARAMETER_1);
		String procName = getParameter(PARAMETER_2);
		
		MemoryExampleTable extLog = new MemoryExampleTable(listOfAtts);

		ExampleSet exsDMClasses = inputDMClasses.getData(ExampleSet.class);
		ExampleSet exsDMKeys = inputDMKeys.getData(ExampleSet.class);
		
		try {
		
			DataModelMM dmm = new DataModelMM(null, exsDMClasses, exsDMKeys); // FIXME

			if (inputEventsCollection.isConnected()) {
				IOObjectCollection<ExampleSet> evCol = inputEventsCollection
						.getData(IOObjectCollection.class);
				if (evCol != null) {
					for (ExampleSet evExSet : evCol.getObjects()) {
						for (Example example : evExSet) {
							double evId = example.getId();

							String traceId = "trace01";

							double[] doubleArray = new double[listOfAtts
									.size()];
							doubleArray[0] = atLogId.getMapping()
									.mapString(logName);
							doubleArray[1] = atLogProcId.getMapping()
									.mapString(procName);
							doubleArray[2] = atLogTraceId.getMapping()
									.mapString(traceId);
							doubleArray[3] = evId;
							extLog.addDataRow(
									new DoubleArrayDataRow(doubleArray));
						}
					}
				}

				failed = false;
			}

			/**/
		
		} catch (Exception e) {
			e.printStackTrace();
			throw new OperatorException(e.getMessage());
		}
		
		if (!failed) {
			outputLog.deliverMD(outputMD);
			ExampleSet exsLog = new SimpleExampleSet(extLog);
			outputLog.deliver(exsLog);
		} else {
			outputLog.deliverMD(outputMD);
			outputLog.deliver(null);
			throw new OperatorException(msgFailure);
		}
		logger.log(Level.INFO,
				"End: padas log splitter ("
						+ (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterType logNameType = new ParameterTypeString(PARAMETER_1,PARAMETER_1,"",true);
		parameterTypes.add(logNameType);
		ParameterType procNameType = new ParameterTypeString(PARAMETER_2,PARAMETER_2,"",true);
		parameterTypes.add(procNameType);
//		ParameterType Type = new ParameterTypeString(PARAMETER_1,PARAMETER_1,"",true);
//		parameterTypes.add(logIdType);
//		ParameterType type =
//				new ParameterTypeConfiguration(DAPOQLangDialogCreator.class, this);
//        type.setExpert(false);
//        parameterTypes.add(type);
		
		return parameterTypes;
	}
	
}
