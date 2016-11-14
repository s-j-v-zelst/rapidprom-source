package org.rapidprom.operators.conformance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.deckfour.xes.classification.XEventAndClassifier;
import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.classification.XEventClasses;
import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.classification.XEventLifeTransClassifier;
import org.deckfour.xes.classification.XEventNameClassifier;
import org.deckfour.xes.model.XLog;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.log.utils.XUtils;
import org.processmining.models.graphbased.directed.petrinet.PetrinetGraph;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.plugins.connectionfactories.logpetrinet.TransEvClassMapping;
import org.processmining.plugins.utils.sequencedistance.GenericLevenshteinDistance;
import org.processmining.plugins.utils.sequencedistance.SequenceDistance;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.DataPetriNetIOObject;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.ioobjects.TransEvMappingIOObject;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.parameter.ParameterTypeXEventClassifierCategory;
import org.rapidprom.util.ObjectUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.Port;
import com.rapidminer.operator.ports.PortOwner;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.operator.ports.metadata.MetaDataError;
import com.rapidminer.operator.ports.metadata.SimplePrecondition;
import com.rapidminer.operator.ports.quickfix.QuickFix;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.UndefinedParameterError;

/**
 * Creates an {@link TransEvClassMapping} from an {@link Example}. To be used as
 * input for an alignment.
 * 
 * @author F. Mannhardt
 *
 */
public class CreateTransEvMappingOperator extends Operator {

	private static final String EVENTCLASS_COLUMN = "eventclass";
	public static final String TRANSITION_COLUMN = "transition";

	private interface Matcher {

		int distance(String id, String label);

	}

	private static final String NO_MATCHING = "No Matching";
	private static final String APPROXIMATE_MATCHING = "Approximate Matching";
	private static final String EXACT_MATCHING = "Exact Matching";
	private static final String[] MATCHING_TYPES = new String[] { EXACT_MATCHING, APPROXIMATE_MATCHING, NO_MATCHING };

	private final class MissingMappingError implements MetaDataError {

		private final String message;
		private final Port port;

		public MissingMappingError(Port port, String message) {
			this.port = port;
			this.message = message;
		}

		@Override
		public String getMessage() {
			return message;
		}

		@Override
		public PortOwner getOwner() {
			return port.getPorts().getOwner();
		}

		@Override
		public Port getPort() {
			return port;
		}

		@Override
		public List<? extends QuickFix> getQuickFixes() {
			return ImmutableList.of();
		}

		@Override
		public Severity getSeverity() {
			return Severity.WARNING;
		}
	}

	// dummy event class (for unmapped transitions)
	private final static XEventClass DUMMY = new XEventClass("DUMMY", -1) {

		@Override
		public boolean equals(Object o) {
			return this == o;
		}

		@Override
		public int hashCode() {
			return System.identityHashCode(this);
		}

	};

	private static final String AUTOMATIC_MAPPING_KEY = "Automatic Label Matching",
			AUTOMATIC_MAPPING_DESCR = "Automatically map matching transition labels to their corresponding event class labels.",
			APPROXIMATE_MATCHING_DISTANCE_KEY = "Approximate Matching Distance",
			APPROXIMATE_MATCHING_DISTANCE_DEC = "How much difference (string edit distance) between the transition label and the event class label is allowed.",
			PARAMETER_KEY_EVENT_CLASSIFIER = "event_classifier",
			PARAMETER_DESC_EVENT_CLASSIFIER = "Specifies how to identify events within the event log, as defined in http://www.xes-standard.org/";

	private static XEventClassifier[] PARAMETER_DEFAULT_CLASSIFIERS = new XEventClassifier[] {
			new XEventNameClassifier(),
			new XEventAndClassifier(new XEventNameClassifier(), new XEventLifeTransClassifier()) };

	private TransEvClassMapping createDefaultMappingTransitionsToEventClasses(PetrinetGraph net,
			XEventClassifier classifier, XEventClasses eventClasses, Matcher matcher) {
		TransEvClassMapping activityMapping = new TransEvClassMapping(classifier, DUMMY);
		for (Transition t : net.getTransitions()) {
			if (!t.isInvisible()) {
				int bestDistance = Integer.MAX_VALUE;
				XEventClass bestClass = null;
				for (XEventClass eventClass : eventClasses.getClasses()) {
					int newDistance = matcher.distance(eventClass.getId(), t.getLabel());
					if (newDistance < bestDistance) {
						bestDistance = newDistance;
						bestClass = eventClass;
					}
				}
				if (bestClass != null && bestDistance < getApproximateMatchingDistance()) {
					activityMapping.put(t, bestClass);
				}
			}
		}
		for (Transition t : net.getTransitions()) {
			if (!activityMapping.containsKey(t)) {
				activityMapping.put(t, activityMapping.getDummyEventClass());
			}
		}
		return activityMapping;
	}

	private InputPort inputLog = getInputPorts().createPort("event log (ProM Event Log)", XLogIOObject.class);
	private InputPort inputModel = getInputPorts().createPort("model (ProM Petri net / Data Petri net)",
			IOObject.class);

	private InputPort inputMapping = getInputPorts().createPort("mapping (Example set)");

	private OutputPort outputMappingProM = getOutputPorts().createPort("mapping (ProM Transition/Event Class Mapping)");
	private OutputPort outputMappingExampleSet = getOutputPorts().createPort("mapping (Example set)");

	public CreateTransEvMappingOperator(OperatorDescription description) {
		super(description);
		inputMapping.addPrecondition(new SimplePrecondition(inputMapping, new MetaData(ExampleSet.class), false));
		getTransformer().addRule(new GenerateNewMDRule(outputMappingProM, TransEvMappingIOObject.class));
		getTransformer().addRule(new GenerateNewMDRule(outputMappingExampleSet, ExampleSet.class));
	}

	@Override
	public void doWork() throws OperatorException {

		XLog log = getXLog();		
		XEventClasses eventClasses = XUtils.createEventClasses(getClassifier(), log);

		String matchingType = getParameterAsString(AUTOMATIC_MAPPING_KEY);

		PetrinetGraph model = getModel();
		if (model == null) {
			throw new OperatorException("Missing model!");
		}

		TransEvClassMapping activityMapping = automaticMatching(eventClasses, matchingType, model);

		ExampleSet mapping = getMapping();
		if (mapping != null) {
			applyExampleSetMapping(mapping, eventClasses, model, activityMapping);
		}

		TransEvMappingIOObject mappingIOObject = new TransEvMappingIOObject(activityMapping, getContext());
		outputMappingProM.deliver(mappingIOObject);
		ExampleSet asExampleSet = mappingIOObject.getAsExampleSet();
		outputMappingExampleSet.deliver(asExampleSet);
	}

	private TransEvClassMapping automaticMatching(XEventClasses eventClasses, String matchingType, PetrinetGraph model)
			throws UndefinedParameterError {
		TransEvClassMapping activityMapping;
		if (EXACT_MATCHING.equals(matchingType)) {
			activityMapping = createDefaultMappingTransitionsToEventClasses(model, getClassifier(), eventClasses,
					new Matcher() {

						@Override
						public int distance(String eventClass, String transition) {
							return eventClass.equalsIgnoreCase(transition) ? 0 : Integer.MAX_VALUE;
						}
					});
		} else if (APPROXIMATE_MATCHING.equals(matchingType)) {
			final GenericLevenshteinDistance<Character> distance = new GenericLevenshteinDistance<Character>();
			activityMapping = createDefaultMappingTransitionsToEventClasses(model, getClassifier(), eventClasses,
					new Matcher() {

						@Override
						public int distance(String eventClass, String transition) {
							return distance.computeDistance(Lists.charactersOf(eventClass),
									Lists.charactersOf(transition), new SequenceDistance.Equivalence<Character>() {

										@Override
										public boolean equals(Character a, Character b) {
											return a.equals(b);
										}
									});
						}

					});
		} else {
			activityMapping = new TransEvClassMapping(getClassifier(), DUMMY);
		}
		return activityMapping;
	}

	private void applyExampleSetMapping(ExampleSet mapping, XEventClasses eventClasses, PetrinetGraph model,
			TransEvClassMapping activityMapping) throws OperatorException {
		Attribute transAttr = mapping.getAttributes().get(TRANSITION_COLUMN, false);
		if (transAttr == null) {
			throw new OperatorException("Expected column '" + TRANSITION_COLUMN + "'!");
		}
		Attribute eventAttr = mapping.getAttributes().get(EVENTCLASS_COLUMN, false);
		if (eventAttr == null) {
			throw new OperatorException("Expected column '" + EVENTCLASS_COLUMN + "'!");
		}

		Map<String, Transition> transitions = getTransitions(model);

		for (Example element : mapping) {
			String transitionLabel = element.getValueAsString(transAttr);
			String eventClassLabel = element.getValueAsString(eventAttr);
			if (transitionLabel == null && eventClassLabel == null) {
				inputMapping.addError(new MissingMappingError(inputMapping, "Invalid entry " + element));
			} else if (transitionLabel == null) {
				inputMapping.addError(
						new MissingMappingError(inputMapping, "Missing transition for eventclass " + eventClassLabel));
			} else {
				Transition transition = transitions.get(transitionLabel);
				if (transition != null) {
					if (eventClassLabel.equals(Attribute.MISSING_NOMINAL_VALUE)) {
						activityMapping.put(transition, DUMMY);
					} else {
						XEventClass eventClass = eventClasses.getByIdentity(eventClassLabel);
						if (eventClass != null) {
							activityMapping.put(transition, eventClass);
						} else {
							inputMapping.addError(
									new MissingMappingError(inputMapping, "Unknown event class " + eventClassLabel));
						}
					}
				} else {
					inputMapping
							.addError(new MissingMappingError(inputMapping, "Unknown transition " + transitionLabel));
				}
			}
		}
	}

	private Map<String, Transition> getTransitions(PetrinetGraph model) {
		Map<String, Transition> transitions = new HashMap<>();
		for (Transition t : model.getTransitions()) {
			transitions.put(t.getLabel(), t);
		}
		return transitions;
	}

	private XEventClassifier getClassifier() throws UndefinedParameterError {
		ParameterTypeXEventClassifierCategory eClassParam = (ParameterTypeXEventClassifierCategory) getParameterType(
				PARAMETER_KEY_EVENT_CLASSIFIER);
		try {
			return eClassParam.valueOf(getParameterAsInt(PARAMETER_KEY_EVENT_CLASSIFIER));
		} catch (IndexOutOfBoundsException e) {
			throw new UndefinedParameterError("The index chosen is no longer available");
		}
	}

	private int getApproximateMatchingDistance() {
		try {
			return getParameterAsInt(APPROXIMATE_MATCHING_DISTANCE_KEY);
		} catch (UndefinedParameterError e) {
			throw new RuntimeException(e);
		}
	}

	private PluginContext getContext() throws UserError {
		return RapidProMGlobalContext.instance().getPluginContext();
	}

	private ExampleSet getMapping() throws UserError {
		return inputMapping.getDataOrNull(ExampleSet.class);
	}

	private XLog getXLog() throws UserError {
		return inputLog.getData(XLogIOObject.class).getArtifact();
	}

	private PetrinetGraph getModel() throws OperatorException {
		IOObject pnObject = inputModel.getData(IOObject.class);
		if (pnObject != null) {
			if (pnObject instanceof PetriNetIOObject) {
				return ((PetriNetIOObject) pnObject).getArtifact();
			} else if (pnObject instanceof DataPetriNetIOObject) {
				return ((DataPetriNetIOObject) pnObject).getArtifact();
			} else {
				throw new OperatorException("Wrong input model " + pnObject);
			}
		} else {
			return null;
		}
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();

		params.add(new ParameterTypeXEventClassifierCategory(PARAMETER_KEY_EVENT_CLASSIFIER,
				PARAMETER_DESC_EVENT_CLASSIFIER, ObjectUtils.toString(PARAMETER_DEFAULT_CLASSIFIERS),
				PARAMETER_DEFAULT_CLASSIFIERS, 0, false, inputLog));

		params.add(new ParameterTypeCategory(AUTOMATIC_MAPPING_KEY, AUTOMATIC_MAPPING_DESCR, MATCHING_TYPES, 0));

		params.add(new ParameterTypeInt(APPROXIMATE_MATCHING_DISTANCE_KEY, APPROXIMATE_MATCHING_DISTANCE_DEC, 0, 100, 5,
				true));

		return params;
	}
}