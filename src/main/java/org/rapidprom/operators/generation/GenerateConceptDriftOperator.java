package org.rapidprom.operators.generation;

import java.util.List;

import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.ioobjects.XLogIOObject;

import com.rapidminer.example.ExampleSet;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.tools.Ontology;

public class GenerateConceptDriftOperator extends Operator {

	public static final String PARAMETER_20_KEY = "# drifts",
			PARAMETER_20_DESCR = "the number of drifts that will be included in the resulting event log",

			// traces between drifts
			PARAMETER_21_KEY = "CBD (cases between drifts)",
			PARAMETER_21_DESCR = "defines the distribution used to simulate the number of cases between drifts",
			PARAMETER_22_KEY = "CBD mean",
			PARAMETER_22_DESCR = "mean of the distribution used for cases between drifts",
			PARAMETER_23_KEY = "CBD std dev",
			PARAMETER_23_DESCR = "standard deviation of the distribution used for cases between drifts",
			PARAMETER_24_KEY = "CBD min",
			PARAMETER_24_DESCR = "minimum value of the distribution used for cases between drifts",
			PARAMETER_25_KEY = "CBD max",
			PARAMETER_25_DESCR = "max value of the distribution used for cases between drifts",

			// type of drifts
			PARAMETER_26_KEY = "(DT) Drift Type", PARAMETER_26_DESCR = "Type of drift: sudden, gradual, momentary",
			PARAMETER_27_KEY = "(DT) Function",
			PARAMETER_27_DESCR = "the function used to determine how fast/slow the drift is applied",
			PARAMETER_28_KEY = "(DT) slope", PARAMETER_28_DESCR = "the slope of the function",
			PARAMETER_29_KEY = "(DT) mean", PARAMETER_29_DESCR = "the mean of the function",

			PARAMETER_30_KEY = "Base PN %",
			PARAMETER_30_DESCR = "probability of sampling a trace from the	base petri 	net when not applying drift",
			PARAMETER_31_KEY = "Drift PN %",
			PARAMETER_31_DESCR = "probability of sampling a trace from the	drift petri	net when applying drift",

			PARAMETER_2_KEY = "max activities per trace",
			PARAMETER_2_DESCR = "the maximum number of activities that will be generated in a single trace (limit for models with loops)",
			PARAMETER_3_KEY = "start time", PARAMETER_3_DESCR = "initial time to start the simulation.",
			// time between arrivals
			PARAMETER_5_KEY = "TBA (time between trace arrivals)",
			PARAMETER_5_DESCR = "defines the time distribution used to simulate time between the start of two consecutive traces.",
			PARAMETER_6_KEY = "TBA mean",
			PARAMETER_6_DESCR = "mean of the time distribution used for time between arrivals",
			PARAMETER_7_KEY = "TBA std dev",
			PARAMETER_7_DESCR = "standard deviation of the time distribution used for time between arrivals",
			PARAMETER_8_KEY = "TBA min",
			PARAMETER_8_DESCR = "minimum value of the time distribution used for time between arrivals",
			PARAMETER_9_KEY = "TBA max",
			PARAMETER_9_DESCR = "max value of the time distribution used for time between arrivals",
			PARAMETER_10_KEY = "TBA time units",
			PARAMETER_10_DESCR = "time granularity used for generating time between arrivals",
			// time between events
			PARAMETER_11_KEY = "TBE (time between events)",
			PARAMETER_11_DESCR = "defines the time distribution used to simulate time between two consecutive events.",
			PARAMETER_12_KEY = "TBE mean",
			PARAMETER_12_DESCR = "mean of the time distribution used for time between events",
			PARAMETER_13_KEY = "TBE std dev",
			PARAMETER_13_DESCR = "standard deviation of the time distribution used for time between events",
			PARAMETER_14_KEY = "TBE min",
			PARAMETER_14_DESCR = "minimum value of the time distribution used for time between events",
			PARAMETER_15_KEY = "TBE max",
			PARAMETER_15_DESCR = "max value of the time distribution used for time between events",
			PARAMETER_16_KEY = "TBE time units",
			PARAMETER_16_DESCR = "time granularity used for generating time between events",

			PARAMETER_17_KEY = "seed", PARAMETER_17_DESCR = "seed used for random number generation";

	private InputPort input1 = getInputPorts().createPort("base petri net", PetriNetIOObject.class);
	private InputPort input2 = getInputPorts().createPort("drift petri net", PetriNetIOObject.class);

	private OutputPort output = getOutputPorts().createPort("event log");
	private OutputPort output2 = getOutputPorts().createPort("drift points");
	
	private ExampleSetMetaData metaData = null;

	public GenerateConceptDriftOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, XLogIOObject.class));
		
		this.metaData = new ExampleSetMetaData();
		AttributeMetaData amd1 = new AttributeMetaData("DriftID", Ontology.STRING);
		amd1.setRole(AttributeColumn.REGULAR);
		amd1.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd1);
		AttributeMetaData amd2 = new AttributeMetaData("Type", Ontology.STRING);
		amd2.setRole(AttributeColumn.REGULAR);
		amd2.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd2);
		AttributeMetaData amd3 = new AttributeMetaData("trace_index", Ontology.INTEGER);
		amd3.setRole(AttributeColumn.REGULAR);
		amd3.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd3);
		AttributeMetaData amd4 = new AttributeMetaData("timestamp", Ontology.DATE_TIME);
		amd4.setRole(AttributeColumn.REGULAR);
		amd4.setNumberOfMissingValues(new MDInteger(0));
		metaData.addAttribute(amd4);
		getTransformer().addRule(new GenerateNewMDRule(output2, this.metaData));
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		return parameterTypes;
	}
}
