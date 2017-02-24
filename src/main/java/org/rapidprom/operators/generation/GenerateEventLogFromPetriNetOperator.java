package org.rapidprom.operators.generation;

import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.model.XLog;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.petrinetsimulator.algorithms.Simulator;
import org.processmining.petrinetsimulator.constants.SettingsConstants;
import org.processmining.petrinetsimulator.parameters.SimulationSettings;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.ioobjects.XLogIOObject;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeDate;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.conditions.EqualTypeCondition;
import com.rapidminer.tools.LogService;

import cern.jet.random.AbstractContinousDistribution;
import cern.jet.random.Exponential;
import cern.jet.random.Normal;
import cern.jet.random.Uniform;
import cern.jet.random.engine.DRand;
import cern.jet.random.engine.RandomEngine;
import javassist.tools.rmi.ObjectNotFoundException;

public class GenerateEventLogFromPetriNetOperator extends Operator {

	public static final String PARAMETER_1_KEY = "number of traces",
			PARAMETER_1_DESCR = "the number of traces that will be generated in the resulting "
					+ "event log. trace uniqueness is not guaranteed.",
			PARAMETER_2_KEY = "max activities per trace",
			PARAMETER_2_DESCR = "the maximum number of activities that will be generated in a single trace (limit for models with loops)",
			PARAMETER_3_KEY = "start time",
			PARAMETER_3_DESCR = "initial time to start the simulation.",
			PARAMETER_4_KEY = "index of first trace",
			PARAMETER_4_DESCR = "this is for when you wnat to merge two simulated logs. this way, the indexes of traces can be incremental and unique.",
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

	private InputPort input = getInputPorts().createPort("petri net", PetriNetIOObject.class);

	private OutputPort output = getOutputPorts().createPort("event log");

	public GenerateEventLogFromPetriNetOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, XLogIOObject.class));
	}

	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: generating event log from petri net");
		long time = System.currentTimeMillis();

		PluginContext pluginContext = RapidProMGlobalContext.instance().getPluginContext();

		PetriNetIOObject pNet = input.getData(PetriNetIOObject.class);

		Simulator sim;
		XLog result = null;
		try {
			sim = new Simulator(pNet.getArtifact(), pNet.getInitialMarking(), getSettingsObject());
			result = sim.simulate();
		} catch (ObjectNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		output.deliver(new XLogIOObject(result, pluginContext));

		logger.log(Level.INFO,
				"End: generating event log from petri net (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterTypeInt parameter1 = new ParameterTypeInt(PARAMETER_1_KEY, PARAMETER_1_DESCR, 1, Integer.MAX_VALUE,
				1000);
		ParameterTypeInt parameter2 = new ParameterTypeInt(PARAMETER_2_KEY, PARAMETER_2_DESCR, 1, Integer.MAX_VALUE,
				100);
		ParameterTypeDate parameter3 = new ParameterTypeDate(PARAMETER_3_KEY, PARAMETER_3_DESCR,
				new Date(System.currentTimeMillis()));
		ParameterTypeInt parameter4 = new ParameterTypeInt(PARAMETER_4_KEY, PARAMETER_4_DESCR, 1, Integer.MAX_VALUE, 0);

		// TBA
		ParameterTypeCategory parameter5 = new ParameterTypeCategory(PARAMETER_5_KEY, PARAMETER_5_DESCR,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				0);
		ParameterTypeDouble parameter6 = new ParameterTypeDouble(PARAMETER_6_KEY, PARAMETER_6_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter6.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_5_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 0, 1 }));
		ParameterTypeDouble parameter7 = new ParameterTypeDouble(PARAMETER_7_KEY, PARAMETER_7_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter7.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_5_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 1 }));
		ParameterTypeDouble parameter8 = new ParameterTypeDouble(PARAMETER_8_KEY, PARAMETER_8_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter8.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_5_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 2 }));
		ParameterTypeDouble parameter9 = new ParameterTypeDouble(PARAMETER_9_KEY, PARAMETER_9_DESCR, 0,
				Double.MAX_VALUE, 10);
		parameter9.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_5_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 2 }));
		ParameterTypeCategory parameter10 = new ParameterTypeCategory(PARAMETER_10_KEY, PARAMETER_10_DESCR,
				SettingsConstants.getTimeUnits().toArray(new String[SettingsConstants.getTimeUnits().size()]), 4);

		// TBE
		ParameterTypeCategory parameter11 = new ParameterTypeCategory(PARAMETER_11_KEY, PARAMETER_11_DESCR,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				0);
		ParameterTypeDouble parameter12 = new ParameterTypeDouble(PARAMETER_12_KEY, PARAMETER_12_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter12.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_11_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 0, 1 }));
		ParameterTypeDouble parameter13 = new ParameterTypeDouble(PARAMETER_13_KEY, PARAMETER_13_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter13.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_11_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 1 }));
		ParameterTypeDouble parameter14 = new ParameterTypeDouble(PARAMETER_14_KEY, PARAMETER_14_DESCR, 0,
				Double.MAX_VALUE, 1);
		parameter14.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_11_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 2 }));
		ParameterTypeDouble parameter15 = new ParameterTypeDouble(PARAMETER_15_KEY, PARAMETER_15_DESCR, 0,
				Double.MAX_VALUE, 10);
		parameter15.registerDependencyCondition(new EqualTypeCondition(this, PARAMETER_11_KEY,
				SettingsConstants.getDistributions().toArray(new String[SettingsConstants.getDistributions().size()]),
				true, new int[] { 2 }));
		ParameterTypeCategory parameter16 = new ParameterTypeCategory(PARAMETER_16_KEY, PARAMETER_16_DESCR,
				SettingsConstants.getTimeUnits().toArray(new String[SettingsConstants.getTimeUnits().size()]), 3);

		ParameterTypeInt parameter17 = new ParameterTypeInt(PARAMETER_17_KEY, PARAMETER_17_DESCR, 1, Integer.MAX_VALUE,
				1);

		parameterTypes.add(parameter1);
		parameterTypes.add(parameter2);
		parameterTypes.add(parameter3);
		parameterTypes.add(parameter4);
		parameterTypes.add(parameter5);
		parameterTypes.add(parameter6);
		parameterTypes.add(parameter7);
		parameterTypes.add(parameter8);
		parameterTypes.add(parameter9);
		parameterTypes.add(parameter10);
		parameterTypes.add(parameter11);
		parameterTypes.add(parameter12);
		parameterTypes.add(parameter13);
		parameterTypes.add(parameter14);
		parameterTypes.add(parameter15);
		parameterTypes.add(parameter16);
		parameterTypes.add(parameter17);

		return parameterTypes;
	}

	public SimulationSettings getSettingsObject() throws OperatorException {

		AbstractContinousDistribution tba = null, tbe = null;
		RandomEngine engine = new DRand(getParameterAsInt(PARAMETER_17_KEY)); 
		System.out.println("works");
		
		switch (getParameterAsString(PARAMETER_5_KEY)) {
		default:
		case SettingsConstants.EXPONENTIAL:
			tba = new Exponential(1.0 / (double) getTimeInMiliseconds(getParameterAsDouble(PARAMETER_6_KEY),
					getParameterAsString(PARAMETER_10_KEY)), engine);
			break;
		case SettingsConstants.NORMAL:
			tba = new Normal(
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_6_KEY), getParameterAsString(PARAMETER_10_KEY)),
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_7_KEY), getParameterAsString(PARAMETER_10_KEY)),
					engine);
			break;
		case SettingsConstants.UNIFORM:
			tba = new Uniform(
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_8_KEY), getParameterAsString(PARAMETER_10_KEY)),
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_9_KEY), getParameterAsString(PARAMETER_10_KEY)),
					engine);
		}

		switch (getParameterAsString(PARAMETER_11_KEY)) {
		default:
		case SettingsConstants.EXPONENTIAL:
			tbe = new Exponential(1.0 / (double) getTimeInMiliseconds(getParameterAsDouble(PARAMETER_12_KEY),
					getParameterAsString(PARAMETER_16_KEY)), engine);
			break;
		case SettingsConstants.NORMAL:
			tbe = new Normal(
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_12_KEY),
							getParameterAsString(PARAMETER_16_KEY)),
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_13_KEY),
							getParameterAsString(PARAMETER_16_KEY)),
					engine);
			break;
		case SettingsConstants.UNIFORM:
			tbe = new Uniform(
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_14_KEY),
							getParameterAsString(PARAMETER_16_KEY)),
					getTimeInMiliseconds(getParameterAsDouble(PARAMETER_15_KEY),
							getParameterAsString(PARAMETER_16_KEY)),
					engine);
		}

		SimulationSettings settings = new SimulationSettings(getParameterAsInt(PARAMETER_4_KEY),
				getParameterAsInt(PARAMETER_1_KEY), getParameterAsInt(PARAMETER_2_KEY),
				ParameterTypeDate.getParameterAsDate(PARAMETER_3_KEY, this).getTime(), tba, tbe);

		return settings;
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

}
