package org.rapidprom.operators.conceptdrift;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.processmining.petrinetsimulator.constants.SettingsConstants;
import org.rapidprom.operators.ports.metadata.ExampleSetNumberOfAttributesPrecondition;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
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
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

public class MeasureDriftDetectionOperator extends Operator {

	public static final String PARAMETER_1_KEY = "time interval of correctness", PARAMETER_1_DESCR = " ",
			PARAMETER_2_KEY = "time units", PARAMETER_2_DESCR = " ";

	private InputPort theTruth = getInputPorts().createPort("truth (exampleset)", new ExampleSetMetaData());
	private InputPort driftInput = getInputPorts().createPort("drifts (exampleset)", new ExampleSetMetaData());
	private OutputPort output = getOutputPorts().createPort("results (exampleset)");

	public MeasureDriftDetectionOperator(OperatorDescription description) {
		super(description);
		theTruth.addPrecondition(new ExampleSetNumberOfAttributesPrecondition(theTruth, 1));
		driftInput.addPrecondition(new ExampleSetNumberOfAttributesPrecondition(driftInput, 1));

		ExampleSetMetaData md1 = new ExampleSetMetaData();
		AttributeMetaData amd1 = new AttributeMetaData("Name", Ontology.STRING);
		amd1.setRole(AttributeColumn.REGULAR);
		amd1.setNumberOfMissingValues(new MDInteger(0));
		md1.addAttribute(amd1);
		AttributeMetaData amd4 = new AttributeMetaData("Value", Ontology.NUMERICAL);
		amd4.setRole(AttributeColumn.REGULAR);
		amd4.setNumberOfMissingValues(new MDInteger(0));
		md1.addAttribute(amd4);

		getTransformer().addRule(new GenerateNewMDRule(output, md1));
	}

	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: measuring concept drift");
		long time = System.currentTimeMillis();

		ExampleSet truth = theTruth.getData(ExampleSet.class);
		ExampleSet drifts = driftInput.getData(ExampleSet.class);

		int TP = 0, FP = 0, FN = 0;
		List<Long> distances = new ArrayList<Long>();
		double RMSE = 0;

		long interval = getTimeInMiliseconds(getParameterAsDouble(PARAMETER_1_KEY),
				getParameterAsString(PARAMETER_2_KEY));

		// calculate if a drift is correctly detected.

		List<Long> driftList = new ArrayList<Long>();
		List<Long> detectedDrifts = new ArrayList<Long>();

		Iterator<Example> truthIterator = truth.iterator();
		while (truthIterator.hasNext()) {
			Example row = truthIterator.next();
			driftList.add((long) row.getValue(truth.getAttributes().get("Time (ms)")));
		}

		Iterator<Example> driftIterator = drifts.iterator();
		while (driftIterator.hasNext()) {
			Example row = driftIterator.next();
			detectedDrifts.add((long) row.getValue(drifts.getAttributes().get("Time (ms)")));
		}

		Object[] result = null;
		if (truth.getExample(0).getValueAsString(truth.getAttributes().get("Drift Type"))
				.equals(SettingsConstants.SUDDEN)) {
			result = evaluateSuddenDrift(driftList, detectedDrifts, interval);
		} else
			result = evaluateNonSuddenDrift(driftList, detectedDrifts, interval);

		if (result != null)
			fillResultsTable((int) result[0], (int) result[1], (int) result[2], (double) result[3]);

		logger.log(Level.INFO, "End: measuring concept drift (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();

		ParameterTypeDouble parameter1 = new ParameterTypeDouble(PARAMETER_1_KEY, PARAMETER_1_DESCR, 0,
				Double.MAX_VALUE, 1);

		ParameterTypeCategory parameter2 = new ParameterTypeCategory(PARAMETER_2_KEY, PARAMETER_2_DESCR,
				SettingsConstants.getTimeUnits().toArray(new String[SettingsConstants.getTimeUnits().size()]), 4);

		params.add(parameter1);
		params.add(parameter2);
		return params;
	}

	@SuppressWarnings("deprecation")
	private void fillResultsTable(int TP, int FP, int FN, double RMSE) {
		ExampleSet es = null;
		MemoryExampleTable table = null;
		List<Attribute> attributes = new LinkedList<Attribute>();
		attributes.add(AttributeFactory.createAttribute("Name", Ontology.STRING));
		attributes.add(AttributeFactory.createAttribute("Value", Ontology.NUMERICAL));
		table = new MemoryExampleTable(attributes);

		DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

		table.addDataRow(factory.create(new Object[] { "True Positives", TP },
				attributes.toArray(new Attribute[attributes.size()])));
		table.addDataRow(factory.create(new Object[] { "False Positives", FP },
				attributes.toArray(new Attribute[attributes.size()])));
		table.addDataRow(factory.create(new Object[] { "False Negatives", FN },
				attributes.toArray(new Attribute[attributes.size()])));

		double precision = ((double) TP) / ((double) TP + FP);
		if (Double.isNaN(precision))
			precision = 0;
		table.addDataRow(factory.create(new Object[] { "Precision", precision },
				attributes.toArray(new Attribute[attributes.size()])));

		double recall = ((double) TP) / ((double) TP + FN);
		if (Double.isNaN(recall))
			recall = 0;
		table.addDataRow(factory.create(new Object[] { "Recall", recall },
				attributes.toArray(new Attribute[attributes.size()])));

		double f1score = (2 * precision * recall) / (precision + recall);
		if (Double.isNaN(f1score))
			f1score = 0;
		table.addDataRow(factory.create(new Object[] { "F1-score", f1score },
				attributes.toArray(new Attribute[attributes.size()])));

		if (Double.isNaN(RMSE))
			RMSE = 0;
		table.addDataRow(factory.create(new Object[] { "Rooted Mean Square Error", RMSE },
				attributes.toArray(new Attribute[attributes.size()])));

		es = table.createExampleSet();
		output.deliver(es);
	}

	private long getTimeInMiliseconds(double input, String timeUnit) {
		switch (timeUnit) {
		case SettingsConstants.MILISECONDS:
			return Math.round(input);
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

	public class SuddenDrift {
		private long realdrift, interval;

		public SuddenDrift(long realdrift, long interval) {

			this.realdrift = realdrift;
			this.interval = interval;
		}

		public boolean doesFit(long time) {
			if (time >= realdrift - interval && time <= realdrift + interval)
				return true;
			else
				return false;
		}

		public long getDistance(long time) {
			return Math.abs(realdrift - time);
		}

		public int getClosest(List<Long> detected) {
			assert !detected.isEmpty();

			int index = 0;
			long minDistance = Long.MAX_VALUE;

			for (int i = 0; i < detected.size(); i++)
				if (getDistance(detected.get(i)) < minDistance) {
					minDistance = getDistance(detected.get(i));
					index = i;
				}
			return index;
		}

		public long getTime() {
			return realdrift;
		}
	}

	public Object[] evaluateSuddenDrift(List<Long> realDriftPoints, List<Long> detectedDrifts, long interval) {

		List<SuddenDrift> realDrifts = new ArrayList<SuddenDrift>();
		for (long l : realDriftPoints)
			realDrifts.add(new SuddenDrift(l, interval));

		List<Long> detectedDrifts_aux = new ArrayList<Long>();
		detectedDrifts_aux.addAll(detectedDrifts);

		// (real drift, detected drift)
		Map<SuddenDrift, Long> matches = new HashMap<SuddenDrift, Long>();

		for (SuddenDrift real : realDrifts) {
			List<Long> truePositives = new ArrayList<Long>();
			for (long l : detectedDrifts_aux) {
				if (real.doesFit(l))
					truePositives.add(l);
			}
			if (!truePositives.isEmpty()) {
				// we have a match
				matches.put(real, truePositives.get(real.getClosest(truePositives)));

				// not eligible anymore
				detectedDrifts_aux.remove(matches.get(real));
			}
		}

		int TP = matches.size();
		int FP = detectedDrifts.size() - matches.size();
		int FN = realDrifts.size() - matches.size();

		// RMSE
		double accum = 0;
		for (SuddenDrift drift : matches.keySet()) {
			long absDistance = drift.getDistance(matches.get(drift));
			accum = accum + Math.pow(absDistance, 2);
		}

		double rmse = Math.sqrt(accum);

		return new Object[] { TP, FP, FN, rmse };
	}

	/**
	 * This method evaluates drift in the case of gradual or momentary drifts
	 * (non-sudden) the particularity of them is that drift is not
	 * instantaneous, but has a start and a complete point.
	 * 
	 * even drifts (0, 2, 4,...) are starts, odd drifts (1, 3, 5....) are
	 * completes of the previous.
	 * 
	 * @param realDriftPoints
	 * @param detectedDrifts
	 * @param interval
	 * @return
	 */
	public Object[] evaluateNonSuddenDrift(List<Long> realDriftPoints, List<Long> detectedDrifts, long interval) {

		List<SuddenDrift> realDrifts = new ArrayList<SuddenDrift>();
		for (long l : realDriftPoints)
			realDrifts.add(new SuddenDrift(l, interval));

		List<Long> detectedDrifts_aux = new ArrayList<Long>();
		detectedDrifts_aux.addAll(detectedDrifts);

		// (real drift, detected drift)
		Map<SuddenDrift, Long> matches = new HashMap<SuddenDrift, Long>();

		for (SuddenDrift real : realDrifts) {
			List<Long> truePositives = new ArrayList<Long>();
			for (long l : detectedDrifts_aux) {
				if (real.doesFit(l))
					truePositives.add(l);
			}
			if (!truePositives.isEmpty()) {
				// we have a match
				matches.put(real, truePositives.get(real.getClosest(truePositives)));

				// not eligible anymore
				detectedDrifts_aux.remove(matches.get(real));
			}
		}

		int TP = matches.size();
		int FN = realDrifts.size() - matches.size();

		// false positives that are within a drift interval are not punished
		List<Long> unassignedDrifts = new ArrayList<Long>();
		for (long drift : detectedDrifts)
			if (!matches.containsValue(drift))
				unassignedDrifts.add(drift);

		// for each unassigned drift, we check if it is in a valid interval
		int inInterval = 0;
		for (long detected : unassignedDrifts)
			for (int i = 0; i < realDrifts.size() - 1; i = i + 2)
				// even drifts are starts, odd drifts are completes
				if (detected >= realDrifts.get(i).getTime() && detected <= realDrifts.get(i + 1).getTime())
					inInterval++;

		int FP = detectedDrifts.size() - matches.size() - inInterval;

		// RMSE
		double accum = 0;
		for (SuddenDrift drift : matches.keySet()) {
			long absDistance = drift.getDistance(matches.get(drift));
			accum = accum + Math.pow(absDistance, 2);
		}

		double rmse = Math.sqrt(accum);

		return new Object[] { TP, FP, FN, rmse };
	}

}
