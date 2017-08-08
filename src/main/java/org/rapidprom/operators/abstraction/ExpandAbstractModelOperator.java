package org.rapidprom.operators.abstraction;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.processmining.dataawareexplorer.utils.PetrinetUtils;
import org.processmining.datapetrinets.DataPetriNet;
import org.processmining.datapetrinets.DataPetriNet.PetrinetWithMarkings;
import org.processmining.datapetrinets.DataPetriNetsWithMarkings;
import org.processmining.datapetrinets.exception.NonExistingVariableException;
import org.processmining.datapetrinets.expression.GuardExpression;
import org.processmining.framework.connections.ConnectionCannotBeObtained;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.logenhancement.abstraction.model.AbstractionModel;
import org.processmining.logenhancement.abstraction.model.AbstractionPattern;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.graphbased.directed.petrinet.PetrinetEdge;
import org.processmining.models.graphbased.directed.petrinet.PetrinetGraph;
import org.processmining.models.graphbased.directed.petrinet.PetrinetNode;
import org.processmining.models.graphbased.directed.petrinet.elements.Place;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.models.graphbased.directed.petrinetwithdata.newImpl.DataElement;
import org.processmining.models.graphbased.directed.petrinetwithdata.newImpl.PNWDTransition;
import org.processmining.models.graphbased.directed.petrinetwithdata.newImpl.PetriNetWithData;
import org.processmining.models.graphbased.directed.petrinetwithdata.newImpl.VariableAccess;
import org.processmining.models.semantics.petrinet.Marking;
import org.processmining.plugins.petrinet.reduction.Murata;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.AbstractionModelIOObject;
import org.rapidprom.ioobjects.PetriNetIOObject;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;

import javassist.tools.rmi.ObjectNotFoundException;

/**
 * Expands a model using the pattern from the abstraction model.
 * 
 * @author F. Mannhardt
 *
 */
public class ExpandAbstractModelOperator extends Operator {

	private static final class LabelGenerator {

		int tCount = 0;

		public String getPlaceName(PetrinetGraph parentModel, Place place) {
			return place.getLabel();
		}

		public String getTransitionName(PetrinetGraph parentModel, Transition transition) {
			return transition.getLabel();
		}

		public String getAnonymousTransitionName() {
			return "t".concat(String.valueOf(tCount++));
		}

	}

	private InputPort inputModel = getInputPorts().createPort("model (ProM Petri net)", PetriNetIOObject.class);
	private InputPort inputAbstractionModel = getInputPorts().createPort("abstraction model (ProM Abstraction Model)",
			AbstractionModelIOObject.class);

	private OutputPort output = getOutputPorts().createPort("model (ProM Petri net)");

	public ExpandAbstractModelOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, PetriNetIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {

		PetriNetIOObject modelIO = inputModel.getData(PetriNetIOObject.class);
		Petrinet model = modelIO.getArtifact();
		AbstractionModel abstractionModel = inputAbstractionModel.getData(AbstractionModelIOObject.class).getArtifact();

		Map<Transition, PetrinetGraph> defaultValues = new HashMap<>();
		for (Transition t : model.getTransitions()) {
			for (AbstractionPattern pattern : abstractionModel.getPatterns()) {
				DataPetriNet dpnPattern = pattern.getDPN();
				if (dpnPattern.getLabel().equals(t.getLabel())) {
					defaultValues.put(t, dpnPattern);
				}
			}
		}

		try {
			DataPetriNet dpn = doTransformModelBasedOnAbstractionPatterns(model, modelIO.getInitialMarking(),
					modelIO.getFinalMarking(), defaultValues);
			if (!(dpn instanceof DataPetriNetsWithMarkings)) {
				throw new OperatorException("DPN is not a DataPetriNetsWithMarkings");
			}

			DataPetriNetsWithMarkings expandedModel = (DataPetriNetsWithMarkings) dpn;
			PetrinetWithMarkings pnWithMarkings = DataPetriNet.Factory.toPetrinetWithMarkings(expandedModel);

			PluginContext pluginContext = RapidProMGlobalContext.instance()
					.getFutureResultAwarePluginContext(Murata.class);
			Murata reducer = new Murata();
			try {
				Object[] murateResult = reducer.runPreserveBehavior(pluginContext, pnWithMarkings.getNet(),
						pnWithMarkings.getInitialMarking());
				Petrinet net = (Petrinet) murateResult[0];
				Marking initialMarking = (Marking) murateResult[1];

				// TODO convert original final marking
				Marking finalMarking = PetrinetUtils.guessFinalMarking(net);

				output.deliver(new PetriNetIOObject(net, initialMarking, finalMarking, getContext()));
			} catch (ConnectionCannotBeObtained e) {
				// could not reduce
				output.deliver(new PetriNetIOObject(pnWithMarkings.getNet(), pnWithMarkings.getInitialMarking(),
						pnWithMarkings.getFinalMarkings() != null ? pnWithMarkings.getFinalMarkings()[0] : null,
						getContext()));
			}
		} catch (ObjectNotFoundException e) {
			throw new OperatorException("Missing initial or final marking", e);
		}

	}

	private PluginContext getContext() throws UserError {
		return RapidProMGlobalContext.instance().getPluginContext();
	}

	public DataPetriNet doTransformModelBasedOnAbstractionPatterns(final PetrinetGraph model, Marking initialMarking,
			Marking finalMarking, Map<Transition, PetrinetGraph> transitionToPattern) {

		LabelGenerator labelGenerator = new LabelGenerator();

		DataPetriNetsWithMarkings transformedNet = new PetriNetWithData(
				model.getLabel() + " - Replaced Transitions with Patterns");

		Map<PetrinetNode, PetrinetNode> old2NewNodes = addDPNStructure(transformedNet, model, labelGenerator);

		for (Entry<Transition, PetrinetGraph> entry : transitionToPattern.entrySet()) {

			Transition transition = entry.getKey();
			PetrinetGraph patternNet = entry.getValue();

			Place patternSource = findSource(patternNet);
			Place patternSink = findSink(patternNet);

			if (patternSource == null || patternSink == null) {
				throw new UnsupportedOperationException("Patterns must be SESE with unique source and sink places!");
			}

			PetrinetNode transitionToReplace = old2NewNodes.get(transition);

			Map<PetrinetNode, PetrinetNode> patternOld2New = addDPNStructure(transformedNet, patternNet,
					labelGenerator);

			for (PetrinetEdge<? extends PetrinetNode, ? extends PetrinetNode> edge : transformedNet
					.getInEdges(transitionToReplace)) {
				if (edge instanceof VariableAccess) {
					throw new UnsupportedOperationException("Cannot handle Transitions with write operations!");
				}

				// Add new edge from our source to source of pattern
				Transition tau = transformedNet.addTransition(labelGenerator.getAnonymousTransitionName());
				tau.setInvisible(true);
				transformedNet.addArc((Place) edge.getSource(), tau);
				transformedNet.addArc(tau, (Place) patternOld2New.get(patternSource));
			}

			for (PetrinetEdge<? extends PetrinetNode, ? extends PetrinetNode> edge : transformedNet
					.getOutEdges(transitionToReplace)) {
				if (edge instanceof VariableAccess) {
					throw new UnsupportedOperationException("Cannot handle Transitions with write operations!");
				}

				// Add new edge from sink of pattern to our sink
				Transition tau = transformedNet.addTransition(labelGenerator.getAnonymousTransitionName());
				tau.setInvisible(true);
				transformedNet.addArc((Place) patternOld2New.get(patternSink), tau);
				transformedNet.addArc(tau, (Place) edge.getTarget());
			}

			transformedNet.removeTransition((Transition) transitionToReplace);
		}

		if (initialMarking != null) {
			for (Place place : initialMarking.baseSet()) {
				transformedNet.setInitialMarking(new Marking());
				transformedNet.getInitialMarking().add((Place) old2NewNodes.get(place),
						initialMarking.occurrences(place));
			}
		} else {
			// try with structure
			Place oldSource = findSource(model);
			if (oldSource != null) {
				transformedNet.setInitialMarking(new Marking());
				transformedNet.getInitialMarking().add((Place) old2NewNodes.get(oldSource));
			} else {
				throw new UnsupportedOperationException("Cannot handle models without initial marking");
			}
		}

		if (finalMarking != null) {
			for (Place place : finalMarking.baseSet()) {
				transformedNet.setFinalMarkings(new Marking[] { new Marking() });
				transformedNet.getFinalMarkings()[0].add((Place) old2NewNodes.get(place),
						finalMarking.occurrences(place));
			}
		} else {
			// try with structure
			Place oldSink = findSink(model);
			if (oldSink != null) {
				transformedNet.setFinalMarkings(new Marking[] { new Marking() });
				transformedNet.getFinalMarkings()[0].add((Place) old2NewNodes.get(oldSink));
			} else {
				throw new UnsupportedOperationException("Cannot handle models without final marking");
			}
		}

		return transformedNet;
	}

	private static Map<PetrinetNode, PetrinetNode> addDPNStructure(DataPetriNet targetModel, PetrinetGraph sourceModel,
			LabelGenerator labelGenerator) {
		Map<PetrinetNode, PetrinetNode> old2New = new HashMap<PetrinetNode, PetrinetNode>();
		if (sourceModel instanceof DataPetriNet) {
			// First add all variable, we keep the same naming so callers need
			// to be make sure variables do not overlap if not desired
			for (DataElement variable : ((DataPetriNet) sourceModel).getVariables()) {
				DataElement newVariable = targetModel.addVariable(variable.getVarName(), variable.getType(),
						variable.getMinValue(), variable.getMaxValue());
				old2New.put(variable, newVariable);
			}
		}

		for (PetrinetEdge<? extends PetrinetNode, ? extends PetrinetNode> edge : sourceModel.getEdges()) {
			PetrinetNode newSourceNode = old2New.get(edge.getSource());
			if (newSourceNode == null) {
				newSourceNode = addNode(targetModel, edge.getSource(), labelGenerator);
				old2New.put(edge.getSource(), newSourceNode);
			}
			PetrinetNode newTargetNode = old2New.get(edge.getTarget());
			if (newTargetNode == null) {
				newTargetNode = addNode(targetModel, edge.getTarget(), labelGenerator);
				old2New.put(edge.getTarget(), newTargetNode);
			}
			addEdge(targetModel, newSourceNode, newTargetNode);
		}
		return old2New;
	}

	private static void addEdge(DataPetriNet model, PetrinetNode source, PetrinetNode target) {
		if (source instanceof Transition && target instanceof Place) {
			model.addArc((Transition) source, (Place) target);
		} else if (source instanceof Place && target instanceof Transition) {
			model.addArc((Place) source, (Transition) target);
		} else if (target instanceof DataElement) {
			model.assignWriteOperation((Transition) source, (DataElement) target);
		} else if (source instanceof DataElement) {
			model.assignReadOperation((Transition) source, (DataElement) target);
		} else {
			throw new IllegalArgumentException(String.format("Cannot add an edge between %s and %s", source, target));
		}
	}

	private static PetrinetNode addNode(DataPetriNet model, PetrinetNode node, LabelGenerator labelGenerator) {
		if (node instanceof Transition) {
			Transition transition = (Transition) node;
			Transition newTransition = model.addTransition(labelGenerator.getTransitionName(model, transition));
			newTransition.setInvisible(transition.isInvisible());
			if (transition instanceof PNWDTransition) {
				GuardExpression expression = ((PNWDTransition) transition).getGuardExpression();
				if (expression != null) {
					try {
						((PNWDTransition) newTransition).setGuard(model, expression);
					} catch (NonExistingVariableException e) {
						throw new RuntimeException(e);
					}
				}
			}
			return newTransition;
		} else if (node instanceof Place) {
			Place place = (Place) node;
			return model.addPlace(labelGenerator.getPlaceName(model, place));
		} else if (node instanceof DataElement) {
			throw new IllegalArgumentException(String.format("Variable %s should have been added beforehand!", node));
		} else {
			throw new IllegalArgumentException(String.format("Unkown node %s", node));
		}
	}

	private static Place findSink(PetrinetGraph net) {
		for (Place p : net.getPlaces()) {
			if (net.getOutEdges(p).isEmpty()) {
				return p;
			}
		}
		return null;
	}

	private static Place findSource(PetrinetGraph net) {
		for (Place p : net.getPlaces()) {
			if (net.getInEdges(p).isEmpty()) {
				return p;
			}
		}
		return null;
	}

}