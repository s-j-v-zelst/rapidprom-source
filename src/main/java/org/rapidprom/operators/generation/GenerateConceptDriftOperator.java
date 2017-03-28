package org.rapidprom.operators.generation;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;
import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryBufferedImpl;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.petrinetsimulator.algorithms.ConceptDrifter;
import org.processmining.petrinetsimulator.constants.SettingsConstants;
import org.processmining.petrinetsimulator.parameters.ConceptDriftSettings;
import org.processmining.petrinetsimulator.parameters.SimulationSettings;
import org.processmining.xeslite.lite.factory.XFactoryLiteImpl;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
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
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeDate;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.conditions.EqualTypeCondition;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

import cern.jet.random.AbstractContinousDistribution;
import cern.jet.random.Exponential;
import cern.jet.random.Normal;
import cern.jet.random.Uniform;
import cern.jet.random.engine.DRand;
import cern.jet.random.engine.RandomEngine;
import javassist.tools.rmi.ObjectNotFoundException;

public class GenerateConceptDriftOperator extends Operator {

	public static final String PARAMETER_1_KEY = "# drifts",
			PARAMETER_1_DESCR = "the number of drifts that will be included in the resulting event log",

			PARAMETER_2_KEY = "Type of Drift",
			PARAMETER_2_DESCR = "The types of drift than can be injected: sudden, gradual and momentary.",

			PARAMETER_3_KEY = "Drift Transition Function",
			PARAMETER_3_DESCR = "The function that is used to obtain sampling probabilities between two stable periods.",

			PARAMETER_4_KEY = "Sampling Prob. 1 (Base Model)",
			PARAMETER_4_DESCR = "The probability of sampling traces from the base model in even stable periods. Even and Odd stable periods alternate after each drift.",
			PARAMETER_5_KEY = "Sampling Prob. 2 (Base Model)",
			PARAMETER_5_DESCR = "The probability of sampling traces from the base model in odd stable periods. Even and Odd stable periods alternate after each drift.",

			PARAMETER_6_KEY = "DSP: Duration of a Stable Period",
			PARAMETER_6_DESCR = "The distribution used to generate the duration of a stable period in terms of time.",

			PARAMETER_6A_KEY = "mean (DSP)", PARAMETER_6A_DESCR = "the mean to the DSP distribution",
			PARAMETER_6B_KEY = "std. dev. (DSP)", PARAMETER_6B_DESCR = "the standard deviation of the DSP distribution",
			PARAMETER_6C_KEY = "min (DSP)", PARAMETER_6C_DESCR = "the minimum value of the DSP distribution",
			PARAMETER_6D_KEY = "max (DSP)", PARAMETER_6D_DESCR = "the maximum value of the DSP distribution",
			PARAMETER_6E_KEY = "Time units (DSP)", PARAMETER_6E_DESCR = "the time granularity for the DSP distribution",

			PARAMETER_7_KEY = "DDP: Duration of a Drift Period",
			PARAMETER_7_DESCR = "The distribution used to generate the duration of a drift period in terms of time.",

			PARAMETER_7A_KEY = "mean (DDP)", PARAMETER_7A_DESCR = "the mean to the DDP distribution",
			PARAMETER_7B_KEY = "std. dev. (DDP)", PARAMETER_7B_DESCR = "the standard deviation of the DDP distribution",
			PARAMETER_7C_KEY = "min (DDP)", PARAMETER_7C_DESCR = "the minimum value of the DDP distribution",
			PARAMETER_7D_KEY = "max (DDP)", PARAMETER_7D_DESCR = "the maximum value of the DDP distribution",
			PARAMETER_7E_KEY = "Time units (DDP)", PARAMETER_7E_DESCR = "the time granularity for the DDP distribution",

			PARAMETER_8_KEY = "Start Date", PARAMETER_8_DESCR = "The start time of the generated event log.",

			PARAMETER_9_KEY = "TBC: Time Between Cases",
			PARAMETER_9_DESCR = "The distribution used to generate the time gap between the start of two consecutive cases.",

			PARAMETER_9A_KEY = "mean (TBC)", PARAMETER_9A_DESCR = "the mean to the TBC distribution",
			PARAMETER_9B_KEY = "std. dev. (TBC)", PARAMETER_9B_DESCR = "the standard deviation of the TBC distribution",
			PARAMETER_9C_KEY = "min (TBC)", PARAMETER_9C_DESCR = "the minimum value of the TBC distribution",
			PARAMETER_9D_KEY = "max (TBC)", PARAMETER_9D_DESCR = "the maximum value of the TBC distribution",
			PARAMETER_9E_KEY = "Time units (TBC)", PARAMETER_9E_DESCR = "the time granularity for the TBC distribution",

			PARAMETER_10_KEY = "TBE: Time Between Events",
			PARAMETER_10_DESCR = "The distribution used to generate the time gap between the start of two consecutive events.",

			PARAMETER_10A_KEY = "mean (TBE)", PARAMETER_10A_DESCR = "the mean to the TBE distribution",
			PARAMETER_10B_KEY = "std. dev. (TBE)",
			PARAMETER_10B_DESCR = "the standard deviation of the TBE distribution", PARAMETER_10C_KEY = "min (TBE)",
			PARAMETER_10C_DESCR = "the minimum value of the TBE distribution", PARAMETER_10D_KEY = "max (TBE)",
			PARAMETER_10D_DESCR = "the maximum value of the TBE distribution", PARAMETER_10E_KEY = "Time units (TBE)",
			PARAMETER_10E_DESCR = "the time granularity for the TBE distribution",

			PARAMETER_11_KEY = "max events per case",
			PARAMETER_11_DESCR = "the maximum number of activities that will be generated in a single case (limit for models with loops)",
			PARAMETER_12_KEY = "seed", PARAMETER_12_DESCR = "seed used for random number generation";

	private InputPort input1 = getInputPorts().createPort("base petri net", PetriNetIOObject.class);
	private InputPort input2 = getInputPorts().createPort("drift petri net", PetriNetIOObject.class);

	private OutputPort output = getOutputPorts().createPort("event log");
	private OutputPort output2 = getOutputPorts().createPort("drift points");
	private OutputPort output3 = getOutputPorts().createPort("sampling probabilities over time");
	private OutputPort output4 = getOutputPorts().createPort("sampling probabilities over trace ID");

	public GenerateConceptDriftOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, XLogIOObject.class));

		ExampleSetMetaData md1 = new ExampleSetMetaData();
		AttributeMetaData amd1 = new AttributeMetaData("Drift ID", Ontology.NUMERICAL);
		amd1.setRole(AttributeColumn.REGULAR);
		amd1.setNumberOfMissingValues(new MDInteger(0));
		md1.addAttribute(amd1);
		AttributeMetaData amd2 = new AttributeMetaData("Drift Type", Ontology.STRING);
		amd2.setRole(AttributeColumn.REGULAR);
		amd2.setNumberOfMissingValues(new MDInteger(0));
		md1.addAttribute(amd2);
		AttributeMetaData amd3 = new AttributeMetaData("Drift Lifecycle", Ontology.STRING);
		amd3.setRole(AttributeColumn.REGULAR);
		amd3.setNumberOfMissingValues(new MDInteger(0));
		md1.addAttribute(amd3);
		AttributeMetaData amd4 = new AttributeMetaData("Time (ms)", Ontology.NUMERICAL);
		amd4.setRole(AttributeColumn.REGULAR);
		amd4.setNumberOfMissingValues(new MDInteger(0));
		md1.addAttribute(amd4);

		getTransformer().addRule(new GenerateNewMDRule(output2, md1));

		ExampleSetMetaData md2 = new ExampleSetMetaData();
		AttributeMetaData amd5 = new AttributeMetaData("Sampling probability", Ontology.REAL);
		amd5.setRole(AttributeColumn.REGULAR);
		amd5.setNumberOfMissingValues(new MDInteger(0));
		md2.addAttribute(amd5);
		AttributeMetaData amd6 = new AttributeMetaData("Time (ms)", Ontology.NUMERICAL);
		amd6.setRole(AttributeColumn.REGULAR);
		amd6.setNumberOfMissingValues(new MDInteger(0));
		md2.addAttribute(amd6);

		getTransformer().addRule(new GenerateNewMDRule(output3, md2));

		ExampleSetMetaData md3 = new ExampleSetMetaData();
		AttributeMetaData amd7 = new AttributeMetaData("Sampling probability", Ontology.REAL);
		amd7.setRole(AttributeColumn.REGULAR);
		amd7.setNumberOfMissingValues(new MDInteger(0));
		md3.addAttribute(amd7);
		AttributeMetaData amd8 = new AttributeMetaData("Trace ID", Ontology.NUMERICAL);
		amd8.setRole(AttributeColumn.REGULAR);
		amd8.setNumberOfMissingValues(new MDInteger(0));
		md3.addAttribute(amd8);

		getTransformer().addRule(new GenerateNewMDRule(output4, md3));
	}

	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: generating event log with concept drift");
		long time = System.currentTimeMillis();

		PluginContext pluginContext = RapidProMGlobalContext.instance().getPluginContext();

		PetriNetIOObject pNet_base = input1.getData(PetriNetIOObject.class);
		PetriNetIOObject pNet_drift = input2.getData(PetriNetIOObject.class);

		ConceptDrifter cd = null;
		ConceptDriftSettings cds = getSettingsObject();
		
		XFactory factory = new XFactoryLiteImpl();
		try {

			cd = new ConceptDrifter(pNet_base.getArtifact(), pNet_base.getInitialMarking(), pNet_drift.getArtifact(),
					pNet_drift.getInitialMarking(), cds, factory);
		} catch (ObjectNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		output.deliver(new XLogIOObject(cd.generateLogWithDrift(), pluginContext));

		fillDriftPoints(cd.getDriftPoints(), cds.getDriftType());
		fillSampleProbabilitiesOverTime(cd.getTimePoints());
		fillSampleProbabilitiesOverTraceID(cd.getTracePoints());

//		DrawUtils.printProbabilityHistogram(cd.getTimePoints(), "Time");
//		DrawUtils.printProbabilityHistogram(cd.getTracePoints(), "Trace ID");

		logger.log(Level.INFO, "End: generating event log with concept drift ("
				+ (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterTypeInt parameter1 = new ParameterTypeInt(PARAMETER_1_KEY, PARAMETER_1_DESCR, 1, 100, 5);

		ParameterTypeCategory parameter2 = new ParameterTypeCategory(PARAMETER_2_KEY, PARAMETER_2_DESCR,
				SettingsConstants.getDrifts().toArray(new String[SettingsConstants.getDrifts().size()]), 0);

		ParameterTypeCategory parameter3 = new ParameterTypeCategory(PARAMETER_3_KEY, PARAMETER_3_DESCR,
				SettingsConstants.getDriftTypes().toArray(new String[SettingsConstants.getDriftTypes().size()]), 0);
		parameter3.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_2_KEY,
				SettingsConstants.getDrifts().toArray(new String[SettingsConstants.getDrifts().size()]), true,
				new int[] { 1, 2 }));

		ParameterTypeDouble parameter4 = new ParameterTypeDouble(PARAMETER_4_KEY, PARAMETER_4_DESCR, 0, 1, 1);
		ParameterTypeDouble parameter5 = new ParameterTypeDouble(PARAMETER_5_KEY, PARAMETER_5_DESCR, 0, 1, 0);

		ParameterTypeCategory parameter6 = new ParameterTypeCategory(PARAMETER_6_KEY, PARAMETER_6_DESCR,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				0);
		ParameterTypeDouble parameter6A = new ParameterTypeDouble(PARAMETER_6A_KEY, PARAMETER_6A_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter6A.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_6_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 0, 1 }));
		ParameterTypeDouble parameter6B = new ParameterTypeDouble(PARAMETER_6B_KEY, PARAMETER_6B_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter6B.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_6_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 1 }));
		ParameterTypeDouble parameter6C = new ParameterTypeDouble(PARAMETER_6C_KEY, PARAMETER_6C_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter6C.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_6_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 2 }));
		ParameterTypeDouble parameter6D = new ParameterTypeDouble(PARAMETER_6D_KEY, PARAMETER_6D_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter6D.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_6_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 2 }));
		ParameterTypeCategory parameter6E = new ParameterTypeCategory(PARAMETER_6E_KEY, PARAMETER_6E_DESCR,
				SettingsConstants.getTimeUnits().toArray(new String[SettingsConstants.getTimeUnits().size()]), 5);
		parameter6E.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_6_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 0, 1, 2 }));

		ParameterTypeCategory parameter7 = new ParameterTypeCategory(PARAMETER_7_KEY, PARAMETER_7_DESCR,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				0);
		parameter7.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_2_KEY,
				SettingsConstants.getDrifts().toArray(new String[SettingsConstants.getDrifts().size()]), true,
				new int[] { 1, 2 }));
		ParameterTypeDouble parameter7A = new ParameterTypeDouble(PARAMETER_7A_KEY, PARAMETER_7A_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter7A.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_7_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 0, 1 }));
		ParameterTypeDouble parameter7B = new ParameterTypeDouble(PARAMETER_7B_KEY, PARAMETER_7B_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter7B.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_7_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 1 }));
		ParameterTypeDouble parameter7C = new ParameterTypeDouble(PARAMETER_7C_KEY, PARAMETER_7C_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter7C.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_7_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 2 }));
		ParameterTypeDouble parameter7D = new ParameterTypeDouble(PARAMETER_7D_KEY, PARAMETER_7D_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter7D.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_7_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 2 }));
		ParameterTypeCategory parameter7E = new ParameterTypeCategory(PARAMETER_7E_KEY, PARAMETER_7E_DESCR,
				SettingsConstants.getTimeUnits().toArray(new String[SettingsConstants.getTimeUnits().size()]), 4);
		parameter7E.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_7_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 0, 1, 2 }));

		ParameterTypeDate parameter8 = new ParameterTypeDate(PARAMETER_8_KEY, PARAMETER_8_DESCR,
				new Date(System.currentTimeMillis()));

		ParameterTypeCategory parameter9 = new ParameterTypeCategory(PARAMETER_9_KEY, PARAMETER_9_DESCR,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				0);
		ParameterTypeDouble parameter9A = new ParameterTypeDouble(PARAMETER_9A_KEY, PARAMETER_9A_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter9A.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_9_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 0, 1 }));
		ParameterTypeDouble parameter9B = new ParameterTypeDouble(PARAMETER_9B_KEY, PARAMETER_9B_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter9B.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_9_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 1 }));
		ParameterTypeDouble parameter9C = new ParameterTypeDouble(PARAMETER_9C_KEY, PARAMETER_9C_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter9C.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_9_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 2 }));
		ParameterTypeDouble parameter9D = new ParameterTypeDouble(PARAMETER_9D_KEY, PARAMETER_9D_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter9D.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_9_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 2 }));
		ParameterTypeCategory parameter9E = new ParameterTypeCategory(PARAMETER_9E_KEY, PARAMETER_9E_DESCR,
				SettingsConstants.getTimeUnits().toArray(new String[SettingsConstants.getTimeUnits().size()]), 4);
		parameter9E.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_9_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 0, 1, 2 }));

		ParameterTypeCategory parameter10 = new ParameterTypeCategory(PARAMETER_10_KEY, PARAMETER_10_DESCR,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				0);
		ParameterTypeDouble parameter10A = new ParameterTypeDouble(PARAMETER_10A_KEY, PARAMETER_10A_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter10A.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_10_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 0, 1 }));
		ParameterTypeDouble parameter10B = new ParameterTypeDouble(PARAMETER_10B_KEY, PARAMETER_10B_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter10B.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_10_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 1 }));
		ParameterTypeDouble parameter10C = new ParameterTypeDouble(PARAMETER_10C_KEY, PARAMETER_10C_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter10C.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_10_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 2 }));
		ParameterTypeDouble parameter10D = new ParameterTypeDouble(PARAMETER_10D_KEY, PARAMETER_10D_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter10D.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_10_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 2 }));
		ParameterTypeCategory parameter10E = new ParameterTypeCategory(PARAMETER_10E_KEY, PARAMETER_10E_DESCR,
				SettingsConstants.getTimeUnits().toArray(new String[SettingsConstants.getTimeUnits().size()]), 4);
		parameter10E.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_10_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 0, 1, 2 }));

		ParameterTypeInt parameter11 = new ParameterTypeInt(PARAMETER_11_KEY, PARAMETER_11_DESCR, 1, Integer.MAX_VALUE,
				100);
		ParameterTypeInt parameter12 = new ParameterTypeInt(PARAMETER_12_KEY, PARAMETER_12_DESCR, 0, Integer.MAX_VALUE,
				1);

		parameterTypes.add(parameter1);
		parameterTypes.add(parameter2);
		parameterTypes.add(parameter3);
		parameterTypes.add(parameter4);
		parameterTypes.add(parameter5);
		parameterTypes.add(parameter6);
		parameterTypes.add(parameter6A);
		parameterTypes.add(parameter6B);
		parameterTypes.add(parameter6C);
		parameterTypes.add(parameter6D);
		parameterTypes.add(parameter6E);
		parameterTypes.add(parameter7);
		parameterTypes.add(parameter7A);
		parameterTypes.add(parameter7B);
		parameterTypes.add(parameter7C);
		parameterTypes.add(parameter7D);
		parameterTypes.add(parameter7E);
		parameterTypes.add(parameter8);
		parameterTypes.add(parameter9);
		parameterTypes.add(parameter9A);
		parameterTypes.add(parameter9B);
		parameterTypes.add(parameter9C);
		parameterTypes.add(parameter9D);
		parameterTypes.add(parameter9E);
		parameterTypes.add(parameter10);
		parameterTypes.add(parameter10A);
		parameterTypes.add(parameter10B);
		parameterTypes.add(parameter10C);
		parameterTypes.add(parameter10D);
		parameterTypes.add(parameter10E);
		parameterTypes.add(parameter11);
		parameterTypes.add(parameter12);

		return parameterTypes;
	}

	public ConceptDriftSettings getSettingsObject() throws OperatorException {

		AbstractContinousDistribution tbc = null, tbe = null, dsp = null, ddp = null;
		RandomEngine engine = new DRand(getParameterAsInt(PARAMETER_12_KEY));

		// everything goes to the miliseconds level

		switch (getParameterAsString(PARAMETER_6_KEY)) {
		case SettingsConstants.EXPONENTIAL:
			dsp = new Exponential(1.0 / getTimeInMiliseconds(getParameterAsDouble(PARAMETER_6A_KEY),
					getParameterAsString(PARAMETER_6E_KEY)), engine);
			break;
		case SettingsConstants.NORMAL:
			dsp = new Normal(
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_6A_KEY),
							getParameterAsString(PARAMETER_6E_KEY)),
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_6B_KEY),
							getParameterAsString(PARAMETER_6E_KEY)),
					engine);
			break;
		case SettingsConstants.UNIFORM:
			dsp = new Uniform(
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_6C_KEY),
							getParameterAsString(PARAMETER_6E_KEY)),
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_6D_KEY),
							getParameterAsString(PARAMETER_6E_KEY)),
					engine);
			break;
		}

		switch (getParameterAsString(PARAMETER_7_KEY)) {
		case SettingsConstants.EXPONENTIAL:
			ddp = new Exponential(1.0 / getTimeInMiliseconds(getParameterAsDouble(PARAMETER_7A_KEY),
					getParameterAsString(PARAMETER_7E_KEY)), engine);
			break;
		case SettingsConstants.NORMAL:
			ddp = new Normal(
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_7A_KEY),
							getParameterAsString(PARAMETER_7E_KEY)),
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_7B_KEY),
							getParameterAsString(PARAMETER_7E_KEY)),
					engine);
			break;
		case SettingsConstants.UNIFORM:
			ddp = new Uniform(
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_7C_KEY),
							getParameterAsString(PARAMETER_7E_KEY)),
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_7D_KEY),
							getParameterAsString(PARAMETER_7E_KEY)),
					engine);
			break;
		}

		switch (getParameterAsString(PARAMETER_9_KEY)) {
		case SettingsConstants.EXPONENTIAL:
			tbc = new Exponential(1.0 / getTimeInMiliseconds(getParameterAsDouble(PARAMETER_9A_KEY),
					getParameterAsString(PARAMETER_9E_KEY)), engine);
			break;
		case SettingsConstants.NORMAL:
			tbc = new Normal(
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_9A_KEY),
							getParameterAsString(PARAMETER_9E_KEY)),
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_9B_KEY),
							getParameterAsString(PARAMETER_9E_KEY)),
					engine);
			break;
		case SettingsConstants.UNIFORM:
			tbc = new Uniform(
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_9C_KEY),
							getParameterAsString(PARAMETER_9E_KEY)),
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_9D_KEY),
							getParameterAsString(PARAMETER_9E_KEY)),
					engine);
			break;
		}

		switch (getParameterAsString(PARAMETER_10_KEY)) {
		case SettingsConstants.EXPONENTIAL:
			tbe = new Exponential(1.0 / getTimeInMiliseconds(getParameterAsDouble(PARAMETER_10A_KEY),
					getParameterAsString(PARAMETER_10E_KEY)), engine);
			break;
		case SettingsConstants.NORMAL:
			tbe = new Normal(
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_10A_KEY),
							getParameterAsString(PARAMETER_10E_KEY)),
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_10B_KEY),
							getParameterAsString(PARAMETER_10E_KEY)),
					engine);
			break;
		case SettingsConstants.UNIFORM:
			tbe = new Uniform(
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_10C_KEY),
							getParameterAsString(PARAMETER_10E_KEY)),
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_10D_KEY),
							getParameterAsString(PARAMETER_10E_KEY)),
					engine);
			break;
		}

		SimulationSettings simuSettings = new SimulationSettings(0, 0, getParameterAsInt(PARAMETER_11_KEY),
				ParameterTypeDate.getParameterAsDate(PARAMETER_8_KEY, this).getTime(), tbc, tbe);

		return new ConceptDriftSettings(getParameterAsInt(PARAMETER_1_KEY), ddp, dsp,
				getParameterAsDouble(PARAMETER_4_KEY), getParameterAsDouble(PARAMETER_5_KEY),
				getParameterAsString(PARAMETER_2_KEY), getParameterAsString(PARAMETER_3_KEY), simuSettings);
	}

	private long getTimeInMiliseconds(double input, String timeUnit) {
		switch (timeUnit) {
		case SettingsConstants.SECONDS:
			return Math.round(input * 1000.0);
		case SettingsConstants.MINUTES:
			return Math.round(input * 1000.0 * 60);
		case SettingsConstants.HOURS:
			return Math.round(input * 1000.0 * 60 * 60);
		case SettingsConstants.DAYS:
			return Math.round(input * 1000.0 * 60 * 60 * 24);
		case SettingsConstants.MONTHS:
			return Math.round(input * 1000.0 * 60 * 60 * 24 * 30);
		case SettingsConstants.YEARS:
			return Math.round(input * 1000.0 * 60 * 60 * 24 * 30 * 12);
		default:
			return 0;
		}
	}

	@SuppressWarnings("deprecation")
	private void fillDriftPoints(Set<Date> drifts, String driftType) {
		ExampleSet es = null;
		MemoryExampleTable table = null;
		List<Attribute> attributes = new LinkedList<Attribute>();
		attributes.add(AttributeFactory.createAttribute("Drift ID", Ontology.NUMERICAL));
		attributes.add(AttributeFactory.createAttribute("Drift Type", Ontology.STRING));
		attributes.add(AttributeFactory.createAttribute("Drift Lifecycle", Ontology.STRING));
		attributes.add(AttributeFactory.createAttribute("Time (ms)", Ontology.NUMERICAL));
		table = new MemoryExampleTable(attributes);

		if (drifts != null) {
			boolean start = true;
			int traceID = 0;
			Iterator<Date> iterator = drifts.iterator();
			DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

			while (iterator.hasNext()) {
				Date next = iterator.next();
				Object[] vals = new Object[4];
				vals[0] = traceID;
				vals[1] = driftType;
				if (driftType.equals(SettingsConstants.SUDDEN))
					vals[2] = "complete";
				else {
					if (start)
						vals[2] = "start";
					else
						vals[2] = "complete";
					start = !start;
				}

				vals[3] = next.getTime();

				DataRow dataRow = factory.create(vals, attributes.toArray(new Attribute[attributes.size()]));
				table.addDataRow(dataRow);
				traceID++;
			}
		} else {
			DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
			Object[] vals = new Object[6];
			vals[0] = "?";
			vals[1] = Double.NaN;
			vals[2] = Double.NaN;
			vals[3] = Double.NaN;

			DataRow dataRow = factory.create(vals, attributes.toArray(new Attribute[attributes.size()]));
			table.addDataRow(dataRow);
		}

		es = table.createExampleSet();
		output2.deliver(es);
	}

	@SuppressWarnings("deprecation")
	private void fillSampleProbabilitiesOverTime(List<Pair<Long, Double>> samples) {
		ExampleSet es = null;
		MemoryExampleTable table = null;
		List<Attribute> attributes = new LinkedList<Attribute>();
		attributes.add(AttributeFactory.createAttribute("Sampling probability", Ontology.REAL));
		attributes.add(AttributeFactory.createAttribute("Time (ms)", Ontology.NUMERICAL));
		table = new MemoryExampleTable(attributes);

		if (samples != null) {
			Iterator<Pair<Long, Double>> iterator = samples.iterator();
			DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

			while (iterator.hasNext()) {
				Pair<Long, Double> next = iterator.next();
				Object[] vals = new Object[2];
				vals[0] = next.getRight();
				vals[1] = next.getLeft();
				DataRow dataRow = factory.create(vals, attributes.toArray(new Attribute[attributes.size()]));
				table.addDataRow(dataRow);
			}
		} else {
			DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
			Object[] vals = new Object[6];
			vals[0] = Double.NaN;
			vals[1] = Double.NaN;

			DataRow dataRow = factory.create(vals, attributes.toArray(new Attribute[attributes.size()]));
			table.addDataRow(dataRow);
		}

		es = table.createExampleSet();
		output3.deliver(es);
	}
	
	@SuppressWarnings("deprecation")
	private void fillSampleProbabilitiesOverTraceID(List<Pair<Long, Double>> samples) {
		ExampleSet es = null;
		MemoryExampleTable table = null;
		List<Attribute> attributes = new LinkedList<Attribute>();
		attributes.add(AttributeFactory.createAttribute("Sampling probability", Ontology.REAL));
		attributes.add(AttributeFactory.createAttribute("Trace ID", Ontology.NUMERICAL));
		table = new MemoryExampleTable(attributes);

		if (samples != null) {
			Iterator<Pair<Long, Double>> iterator = samples.iterator();
			DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

			while (iterator.hasNext()) {
				Pair<Long, Double> next = iterator.next();
				Object[] vals = new Object[2];
				vals[0] = next.getRight();
				vals[1] = next.getLeft();
				DataRow dataRow = factory.create(vals, attributes.toArray(new Attribute[attributes.size()]));
				table.addDataRow(dataRow);
			}
		} else {
			DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
			Object[] vals = new Object[6];
			vals[0] = Double.NaN;
			vals[1] = Double.NaN;

			DataRow dataRow = factory.create(vals, attributes.toArray(new Attribute[attributes.size()]));
			table.addDataRow(dataRow);
		}

		es = table.createExampleSet();
		output4.deliver(es);
	}
}
