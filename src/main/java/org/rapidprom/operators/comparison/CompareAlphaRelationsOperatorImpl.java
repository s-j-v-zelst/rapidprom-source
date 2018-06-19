package org.rapidprom.operators.comparison;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.classification.XEventClasses;
import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.classification.XEventNameClassifier;
import org.deckfour.xes.model.XLog;
import org.processmining.alphaminer.abstractions.AlphaAbstractionFactory;
import org.processmining.alphaminer.abstractions.AlphaClassicAbstraction;
import org.processmining.framework.util.Pair;
import org.processmining.logabstractions.models.DirectlyFollowsAbstraction;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.parameter.ParameterTypeXEventClassifierCategory;
import org.rapidprom.util.ExampleSetUtils;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.utils.ExampleSetBuilder;
import com.rapidminer.example.utils.ExampleSets;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.tools.Ontology;

public class CompareAlphaRelationsOperatorImpl extends Operator {

	private static XEventClassifier[] PARAMETER_DEFAULT_CLASSIFIERS = new XEventClassifier[] {
			new XEventNameClassifier() };
	private static final String PARAMETER_DESC_EVENT_CLASSIFIER = "Specifies how to identify events within the event log, as defined in http://www.xes-standard.org/";

	private static final String PARAMETER_KEY_EVENT_CLASSIFIER = "event_classifier";
	private static final String PARAMETER_KEY_EVENT_CLASSIFIER_REFERENCE = "event_classifier_reference";

	private static final String PARAMETER_KEY_INCLUDE_START_ACTIVITIES = "include start activities";
	private static final String PARAMETER_DESC_INCLUDE_START_ACTIVITIES = "Take start activities into consideration in comparison.";
	private static final boolean PARAMETER_DEF_INCLUDE_START_ACTIVITIES = true;

	private static final String PARAMETER_KEY_INCLUDE_END_ACTIVITIES = "include end activities";
	private static final String PARAMETER_DESC_INCLUDE_END_ACTIVITIES = "Take end activities into consideration in comparison.";
	private static final boolean PARAMETER_DEF_INCLUDE_END_ACTIVITIES = true;

	private static final int INDEX_TRUE_POSITIVE = 0;
	private static final int INDEX_FALSE_POSITIVE = 1;
	private static final int INDEX_TRUE_NEGATIVE = 2;
	private static final int INDEX_FALSE_NEGATIVE = 3;
	private static final int INDEX_RECALL = 4;
	private static final int INDEX_PRECISION = 5;
	private static final int INDEX_F1 = 6;

	private static final String COLUMN_FALSE_NEGATIVE = "false negative";
	private static final String COLUMN_FALSE_POSITIVE = "false positive";
	private static final String COLUMN_TRUE_NEGATIVE = "true negative";
	private static final String COLUMN_TRUE_POSITIVE = "true positive";
	private static final String COLUMN_RECALL = "recall";
	private static final String COLUMN_PRECISION = "precision";
	private static final String COLUMN_F1 = "f1";

	private static final MDInteger[] COLUMNS_MISSING = new MDInteger[] { new MDInteger(0), new MDInteger(0),
			new MDInteger(0), new MDInteger(0), new MDInteger(0), new MDInteger(0), new MDInteger(0) };
	private static final String[] COLUMNS_NAMES = new String[] { COLUMN_TRUE_POSITIVE, COLUMN_FALSE_POSITIVE,
			COLUMN_TRUE_NEGATIVE, COLUMN_FALSE_NEGATIVE, COLUMN_RECALL, COLUMN_PRECISION, COLUMN_F1 };

	private static final String[] COLUMNS_ROLES = new String[] { AttributeColumn.REGULAR, AttributeColumn.REGULAR,
			AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR, AttributeColumn.REGULAR,
			AttributeColumn.REGULAR };
	private static final int[] COLUMNS_TYPES = new int[] { Ontology.INTEGER, Ontology.INTEGER, Ontology.INTEGER,
			Ontology.INTEGER, Ontology.REAL, Ontology.REAL, Ontology.REAL };
	private final DataRowFactory dataRowFactory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

	private InputPort inputReferenceLog = getInputPorts().createPort("reference log", XLogIOObject.class);
	private InputPort inputLog = getInputPorts().createPort("event log", XLogIOObject.class);

	private ExampleSetMetaData outputPerformanceMetaData = new ExampleSetMetaData();
	private OutputPort outputPortPerformance = getOutputPorts().createPort("example set with memory performance");

	public CompareAlphaRelationsOperatorImpl(OperatorDescription description) {
		super(description);
		outputPerformanceMetaData = ExampleSetUtils.constructExampleSetMetaData(outputPerformanceMetaData,
				COLUMNS_NAMES, COLUMNS_TYPES, COLUMNS_ROLES, COLUMNS_MISSING);
		getTransformer().addRule(new GenerateNewMDRule(outputPortPerformance, outputPerformanceMetaData));
	}

	private ExampleSet constructPerformanceExampleSet(int[] classification) {
		Attribute[] attributes = new Attribute[COLUMNS_NAMES.length];
		for (int i = 0; i < COLUMNS_NAMES.length; i++) {
			attributes[i] = AttributeFactory.createAttribute(COLUMNS_NAMES[i], COLUMNS_TYPES[i]);
		}
		Object[] values = new Object[COLUMNS_NAMES.length];
		values[INDEX_TRUE_POSITIVE] = classification[INDEX_TRUE_POSITIVE];
		values[INDEX_FALSE_POSITIVE] = classification[INDEX_FALSE_POSITIVE];
		values[INDEX_TRUE_NEGATIVE] = classification[INDEX_TRUE_NEGATIVE];
		values[INDEX_FALSE_NEGATIVE] = classification[INDEX_FALSE_NEGATIVE];

		double recall = (double) classification[INDEX_TRUE_POSITIVE]
				/ ((double) classification[INDEX_TRUE_POSITIVE] + classification[INDEX_FALSE_NEGATIVE]);
		double precision = (double) classification[INDEX_TRUE_POSITIVE]
				/ ((double) classification[INDEX_TRUE_POSITIVE] + classification[INDEX_FALSE_POSITIVE]);
		double f1 = (2 * precision * recall) / (precision + recall);

		values[INDEX_RECALL] = recall;
		values[INDEX_PRECISION] = precision;
		values[INDEX_F1] = f1;

		ExampleSetBuilder table = ExampleSets.from(attributes);
		table.addDataRow(dataRowFactory.create(values, attributes));
		return table.build();
	}

	@Override
	public void doWork() throws UserError {
		XEventClassifier referenceClassifier = getXEventClassifierForKey(PARAMETER_KEY_EVENT_CLASSIFIER_REFERENCE);
		XEventClassifier testClassifier = getXEventClassifierForKey(PARAMETER_KEY_EVENT_CLASSIFIER);
		XLog reference = getXLogFromPort(inputReferenceLog);
		XLog log = getXLogFromPort(inputLog);
		XEventClasses refClasses = XEventClasses.deriveEventClasses(referenceClassifier, reference);
		XEventClasses testClasses = XEventClasses.deriveEventClasses(testClassifier, log);

		AlphaClassicAbstraction<XEventClass> refAlpha = AlphaAbstractionFactory.createAlphaClassicAbstraction(reference,
				referenceClassifier);
		AlphaClassicAbstraction<XEventClass> testAlpha = AlphaAbstractionFactory.createAlphaClassicAbstraction(log,
				getXEventClassifierForKey(PARAMETER_KEY_EVENT_CLASSIFIER));
		int[] classification = classifyDFR(refAlpha, testAlpha, refClasses, testClasses, new int[4]);
		if (getParameterAsBoolean(PARAMETER_KEY_INCLUDE_START_ACTIVITIES))
			classification = classifyStart(refAlpha, testAlpha, refClasses, testClasses, classification);
		if (getParameterAsBoolean(PARAMETER_KEY_INCLUDE_END_ACTIVITIES))
			classification = classifyEnd(refAlpha, testAlpha, refClasses, testClasses, classification);

		outputPortPerformance.deliver(constructPerformanceExampleSet(classification));
	}

	private int[] classifyEnd(AlphaClassicAbstraction<XEventClass> reference, AlphaClassicAbstraction<XEventClass> test,
			XEventClasses refClasses, XEventClasses testClasses, final int[] classification) {
		for (XEventClass end : refClasses.getClasses()) {
			if (reference.getEndActivityAbstraction().getAllGEQThreshold().contains(end)) {
				if (test.getEndActivityAbstraction().getAllGEQThreshold().contains(end)) {
					classification[INDEX_TRUE_POSITIVE]++;
				} else {
					classification[INDEX_FALSE_NEGATIVE]++;
				}
			} else {
				if (test.getEndActivityAbstraction().getAllGEQThreshold().contains(end)) {
					classification[INDEX_FALSE_POSITIVE]++;
				} else {
					classification[INDEX_TRUE_NEGATIVE]++;
				}
			}
		}
		Collection<XEventClass> remaining = new HashSet<>(testClasses.getClasses());
		remaining.removeAll(refClasses.getClasses());
		for (XEventClass end : remaining) {
			if (test.getEndActivityAbstraction().getAllGEQThreshold().contains(end)) {
				classification[INDEX_FALSE_POSITIVE]++;
			} else {
				classification[INDEX_TRUE_NEGATIVE]++;
			}
		}
		return classification;
	}

	private int[] classifyStart(AlphaClassicAbstraction<XEventClass> reference,
			AlphaClassicAbstraction<XEventClass> test, XEventClasses refClasses, XEventClasses testClasses,
			final int[] classification) {
		for (XEventClass start : refClasses.getClasses()) {
			if (reference.getStartActivityAbstraction().getAllGEQThreshold().contains(start)) {
				if (test.getStartActivityAbstraction().getAllGEQThreshold().contains(start)) {
					classification[INDEX_TRUE_POSITIVE]++;
				} else {
					classification[INDEX_FALSE_NEGATIVE]++;
				}
			} else {
				if (test.getStartActivityAbstraction().getAllGEQThreshold().contains(start)) {
					classification[INDEX_FALSE_POSITIVE]++;
				} else {
					classification[INDEX_TRUE_NEGATIVE]++;
				}
			}
		}
		Collection<XEventClass> remaining = new HashSet<>(testClasses.getClasses());
		remaining.removeAll(refClasses.getClasses());
		for (XEventClass start : remaining) {
			if (test.getStartActivityAbstraction().getAllGEQThreshold().contains(start)) {
				classification[INDEX_FALSE_POSITIVE]++;
			} else {
				classification[INDEX_TRUE_NEGATIVE]++;
			}
		}
		return classification;
	}

	private int[] classifyDFR(AlphaClassicAbstraction<XEventClass> reference, AlphaClassicAbstraction<XEventClass> test,
			XEventClasses refClasses, XEventClasses testClasses, int[] classification) {

		for (XEventClass ec1 : refClasses.getClasses()) {
			for (XEventClass ec2 : refClasses.getClasses()) {
				Pair<XEventClass, XEventClass> pair = new Pair<XEventClass, XEventClass>(ec1, ec2);
				if (reference.getDirectlyFollowsAbstraction().getAllGEQThreshold().contains(pair)) {
					// positive
					if (test.getDirectlyFollowsAbstraction().getAllGEQThreshold().contains(pair)) {
						classification[INDEX_TRUE_POSITIVE]++;
					} else {
						classification[INDEX_FALSE_NEGATIVE]++;
					}
				} else {
					// negative
					if (test.getDirectlyFollowsAbstraction().getAllGEQThreshold().contains(pair)) {
						classification[INDEX_FALSE_POSITIVE]++;
					} else {
						classification[INDEX_TRUE_NEGATIVE]++;
					}
				}
			}
		}
		Collection<XEventClass> remaining = new HashSet<>(testClasses.getClasses());
		remaining.removeAll(refClasses.getClasses());
		for (XEventClass ec1 : remaining) {
			for (XEventClass ec2 : remaining) {
				Pair<XEventClass, XEventClass> pair = new Pair<XEventClass, XEventClass>(ec1, ec2);
				if (test.getDirectlyFollowsAbstraction().getAllGEQThreshold().contains(pair)) {
					classification[INDEX_FALSE_POSITIVE]++;
				} else {
					classification[INDEX_TRUE_NEGATIVE]++;
				}

			}
		}
		return classification;
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeXEventClassifierCategory(PARAMETER_KEY_EVENT_CLASSIFIER_REFERENCE,
				PARAMETER_DESC_EVENT_CLASSIFIER, new String[] { PARAMETER_DEFAULT_CLASSIFIERS[0].toString() },
				PARAMETER_DEFAULT_CLASSIFIERS, 0, false, inputReferenceLog));

		params.add(new ParameterTypeXEventClassifierCategory(PARAMETER_KEY_EVENT_CLASSIFIER,
				PARAMETER_DESC_EVENT_CLASSIFIER, new String[] { PARAMETER_DEFAULT_CLASSIFIERS[0].toString() },
				PARAMETER_DEFAULT_CLASSIFIERS, 0, false, inputLog));

		params.add(new ParameterTypeBoolean(PARAMETER_KEY_INCLUDE_START_ACTIVITIES,
				PARAMETER_DESC_INCLUDE_START_ACTIVITIES, PARAMETER_DEF_INCLUDE_START_ACTIVITIES, false));

		params.add(new ParameterTypeBoolean(PARAMETER_KEY_INCLUDE_END_ACTIVITIES, PARAMETER_DESC_INCLUDE_END_ACTIVITIES,
				PARAMETER_DEF_INCLUDE_END_ACTIVITIES, false));
		return params;
	}

	protected XEventClassifier getXEventClassifierForKey(String key) throws UndefinedParameterError {
		ParameterTypeXEventClassifierCategory eClassParam = (ParameterTypeXEventClassifierCategory) getParameterType(
				key);
		try {
			return eClassParam.valueOf(getParameterAsInt(key));
		} catch (IndexOutOfBoundsException e) {
			throw new UndefinedParameterError("The index chosen is no longer available");
		}

	}

	protected XLog getXLogFromPort(InputPort port) throws UserError {
		return ((XLogIOObject) port.getData(XLogIOObject.class)).getArtifact();
	}

}
