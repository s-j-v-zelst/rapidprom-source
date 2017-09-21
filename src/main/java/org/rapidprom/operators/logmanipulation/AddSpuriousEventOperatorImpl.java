package org.rapidprom.operators.logmanipulation;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.classification.XEventClasses;
import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryNaiveImpl;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.models.semantics.IllegalTransitionException;
import org.processmining.models.semantics.petrinet.Marking;
import org.processmining.models.semantics.petrinet.PetrinetSemantics;
import org.processmining.models.semantics.petrinet.impl.EfficientPetrinetSemanticsImpl;
import org.processmining.plugins.petrinet.replayresult.PNRepResult;
import org.processmining.plugins.replayer.replayresult.SyncReplayResult;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.PNRepResultIOObject;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMEventLogBasedOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.ParameterTypeLong;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.parameter.conditions.BooleanParameterCondition;

import javassist.tools.rmi.ObjectNotFoundException;

/**
 * adds spurious events to an event log. A spurious event is defined as an event
 * that, at its position, could not happen according to the originating process.
 * assumptions: 1. input process model is a sound workflow net 2. labels in
 * event log match with the net 3. log is clean, i.e. it was generated from the
 * given model.
 * 
 * @author svzelst
 *
 */
public class AddSpuriousEventOperatorImpl extends AbstractRapidProMEventLogBasedOperator {

	private final String PARAM_KEY_NOISE_PROBABILITY = "insertion_probability";
	private final String PARAM_DESC_NOISE_PROBABILITY = "Probability of adding a spurious event";
	private final double PARAM_DEFAULT_NOISE_PROBABILITY = 0.2;

	private final String PARAM_KEY_LABEL_PERCENTAGE = "percentage_of_labels";
	private final String PARAM_DESC_LABEL_PERCENTAGE = "Percentage of labels to use for spurious event generation";
	private final int PARAM_DEFAULT_LABEL_PERCENTAGE = 100;

	private final String PARAM_KEY_NOISE_LABEL = "noise_label";
	private final String PARAM_DESC_NOISE_LABEL = "Indicates which \"key\" needs to be used as a (boolean) event attribute to indicate noise, e.g. (noise=true indicates the event is noisy).";
	private final String PARAM_DEFAULT_NOISE_LABEL = "noise";

	private final String PARAM_KEY_SEED = "seed";
	private final String PARAM_DESC_SEED = "Indicates whether we should use a user-provided seed within the experiment.";
	private final boolean PARAM_DEFAULT_SEED = true;

	private final String PARAM_KEY_SEED_VAL = "seed_value";
	private final String PARAM_DESC_SEED_VAL = "Value of the seed to use (conditional to the boolean \"use seed\" parameter)";
	private final long PARAM_DEFAULT_SEED_VAL = 1337l;

	private final String ARTIFICIAL_LABEL = "__ARTIFICIAL_LABEL__" + System.currentTimeMillis();

	private InputPort inputModel = getInputPorts().createPort("model", PetriNetIOObject.class);
	private InputPort inputAlignments = getInputPorts().createPort("alignments", PNRepResultIOObject.class);
	private OutputPort outputNoisyEventLog = getOutputPorts().createPort("noisy_log");

	private Petrinet pn = null;

	public AddSpuriousEventOperatorImpl(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputNoisyEventLog, XLogIOObject.class));
	}

	@Override
	public void doWork() throws UserError {
		XLog copy = (XLog) getXLog().clone();
		PNRepResult alignments = ((PNRepResultIOObject) inputAlignments.getAnyDataOrNull()).getArtifact();
		final long seed = getSeed();
		Random random = new Random(seed);
		Collection<XEventClass> spuriousCandidates = getSpuriousEventCandidateLabels(copy, random);
		XEventClass[] spurArr = spuriousCandidates.toArray(new XEventClass[spuriousCandidates.size()]);
		PetriNetIOObject pnio = inputModel.getDataOrNull(PetriNetIOObject.class);
		pn = pnio.getArtifact();
		PetrinetSemantics semantics = new EfficientPetrinetSemanticsImpl(pn);
		XFactory factory = new XFactoryNaiveImpl();
		Marking initialMarking = null;
		try {
			initialMarking = pnio.getInitialMarking();
		} catch (ObjectNotFoundException e) {
			e.printStackTrace();
		}

		for (int i = 0; i < copy.size(); i++) {
			int alignmentIndex = 0;
			XTrace t = copy.get(i);
			SyncReplayResult alignment = getAlignment(i, alignments);
			semantics.setCurrentState(initialMarking);
			// add initial noisy event
			int index = 0;
			int v = random.nextInt(99) + 1;
			if (v <= getParameterAsDouble(PARAM_KEY_NOISE_PROBABILITY) * 100) {
				t.add(index, createNoisyEvent(factory, semantics, spurArr, random));
				index++;
			}
			while (index < t.size()) {
				v = random.nextInt(99) + 1;
				if (v <= getParameterAsDouble(PARAM_KEY_NOISE_PROBABILITY) * 100) {
					t.add(index + 1, createNoisyEvent(factory, semantics, spurArr, random));
					index++;
				}
				alignmentIndex = execute(semantics, alignment, alignmentIndex);
				index++;
			}
		}
		outputNoisyEventLog.deliver(new XLogIOObject(copy, RapidProMGlobalContext.instance().getPluginContext()));
	}

	private int execute(final PetrinetSemantics semantics, final SyncReplayResult alignment, int alignmentIndex) {
		Transition exec = (Transition) alignment.getNodeInstance().get(alignmentIndex);
		try {
			semantics.executeExecutableTransition(exec);
		} catch (IllegalTransitionException e) {
			e.printStackTrace();
		}
		alignmentIndex++;
		while (exec.isInvisible() && alignmentIndex < alignment.getNodeInstance().size()) {
			exec = (Transition) alignment.getNodeInstance().get(alignmentIndex);
			try {
				semantics.executeExecutableTransition(exec);
			} catch (IllegalTransitionException e) {
				e.printStackTrace();
			}
			alignmentIndex++;
		}
		return alignmentIndex;
	}

	private SyncReplayResult getAlignment(final int index, final PNRepResult repRes) {
		for (SyncReplayResult alignment : repRes) {
			if (alignment.getTraceIndex().contains(index)) {
				return alignment;
			}
		}
		return null;
	}

	// private PetrinetSemantics execute(PetrinetSemantics semantics, String
	// label) throws IllegalTransitionException {
	// boolean fired = false;
	// for (Transition t : semantics.getExecutableTransitions()) {
	// if (t.getLabel().equals(label)) {
	// semantics.executeExecutableTransition(t);
	// fired = true;
	// break;
	// }
	// }
	// if (!fired) {
	// for (Transition t : semantics.getExecutableTransitions()) {
	// if (t.isInvisible()) {
	// semantics.executeExecutableTransition(t);
	// return execute(semantics, label);
	// }
	// }
	// }
	// return semantics;
	// }

	private XEvent createNoisyEvent(final XFactory factory, final PetrinetSemantics semantics,
			final XEventClass[] spurArr, final Random random) throws UndefinedParameterError {
		XAttributeMap map = factory.createAttributeMap();
		int[] spuriousIndices = getNoiseCandidates(semantics, spurArr);
		String eventName;
		if (spuriousIndices.length > 0) {
			eventName = spurArr[spuriousIndices[random.nextInt(spuriousIndices.length)]].toString();
		} else {
			eventName = ARTIFICIAL_LABEL;
		}
		map.put(XConceptExtension.KEY_NAME,
				new XAttributeLiteralImpl(XConceptExtension.KEY_NAME, eventName, XConceptExtension.instance()));
		map.put(getParameterAsString(PARAM_KEY_NOISE_LABEL),
				new XAttributeLiteralImpl(getParameterAsString(PARAM_KEY_NOISE_LABEL), "true"));
		return factory.createEvent(map);
	}

	/**
	 * returns an array of indices of event classes that may be noise at the
	 * given marking
	 * 
	 * @param net
	 * @param m
	 * @param noiseCandidates
	 * @return
	 */
	private int[] getNoiseCandidates(final PetrinetSemantics semantics, final XEventClass[] spurArr) {
		int[] res = new int[0];
		Collection<String> enabled = computeDirectlyObservableLabels(semantics.getCurrentState(), new HashSet<String>(),
				new HashSet<Transition>());
		for (int i = 0; i < spurArr.length; i++) {
			if (!enabled.contains(spurArr[i].toString())) {
				res = Arrays.copyOf(res, res.length + 1);
				res[res.length - 1] = i;
			}
		}
		return res;
	}

	private Collection<String> computeDirectlyObservableLabels(final Marking marking, Collection<String> labels,
			Collection<Transition> considered) {
		PetrinetSemantics sem = new EfficientPetrinetSemanticsImpl(pn, marking);
		for (Transition t : sem.getExecutableTransitions()) {
			if (!t.isInvisible()) {
				labels.add(t.getLabel());
			} else {
				if (!considered.contains(t)) {
					PetrinetSemantics sem2 = new EfficientPetrinetSemanticsImpl(pn, marking);
					try {
						sem2.executeExecutableTransition(t);
					} catch (IllegalTransitionException e) {
						e.printStackTrace();
					}
					considered.add(t);
					labels = computeDirectlyObservableLabels(sem2.getCurrentState(), labels, considered);
				}
			}
		}
		return labels;
	}

	private long getSeed() throws UndefinedParameterError {
		return getParameterAsBoolean(PARAM_KEY_SEED) ? getParameterAsLong(PARAM_KEY_SEED_VAL)
				: System.currentTimeMillis();
	}

	private Collection<XEventClass> getSpuriousEventCandidateLabels(final XLog log, final Random random)
			throws UndefinedParameterError {
		Collection<XEventClass> res = new HashSet<>();
		XEventClasses eventClasses = XEventClasses.deriveEventClasses(getXEventClassifier(), log);

		int numDraws = (int) Math
				.round(((double) getParameterAsInt(PARAM_KEY_LABEL_PERCENTAGE) / 100d) * eventClasses.size());

		if (numDraws >= eventClasses.size())
			res.addAll(eventClasses.getClasses());
		else {
			XEventClass[] clz = eventClasses.getClasses().toArray(new XEventClass[eventClasses.size()]);
			boolean[] included = new boolean[eventClasses.size()];
			Arrays.fill(included, false);
			while (res.size() < numDraws) {
				int index = -1;
				while (index == -1 || included[index]) {
					index = random.nextInt(eventClasses.size());
				}
				res.add(clz[index]);
				included[index] = true;
			}
		}
		return res;
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();

		ParameterTypeDouble insertProb = new ParameterTypeDouble(PARAM_KEY_NOISE_PROBABILITY,
				PARAM_DESC_NOISE_PROBABILITY, 0, 1, PARAM_DEFAULT_NOISE_PROBABILITY, false);
		params.add(insertProb);

		ParameterTypeInt spurPercentage = new ParameterTypeInt(PARAM_KEY_LABEL_PERCENTAGE, PARAM_DESC_LABEL_PERCENTAGE,
				0, 100, PARAM_DEFAULT_LABEL_PERCENTAGE, false);
		params.add(spurPercentage);

		ParameterTypeString noiseLabel = new ParameterTypeString(PARAM_KEY_NOISE_LABEL, PARAM_DESC_NOISE_LABEL,
				PARAM_DEFAULT_NOISE_LABEL, false);
		params.add(noiseLabel);

		ParameterTypeBoolean seed = new ParameterTypeBoolean(PARAM_KEY_SEED, PARAM_DESC_SEED, PARAM_DEFAULT_SEED, true);

		params.add(seed);

		ParameterTypeLong seedVal = new ParameterTypeLong(PARAM_KEY_SEED_VAL, PARAM_DESC_SEED_VAL, Long.MIN_VALUE,
				Long.MAX_VALUE, PARAM_DEFAULT_SEED_VAL, true);

		seedVal.registerDependencyCondition(new BooleanParameterCondition(this, PARAM_KEY_SEED, true, true));
		params.add(seedVal);

		return params;
	}

}
