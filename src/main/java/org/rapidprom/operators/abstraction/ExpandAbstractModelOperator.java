package org.rapidprom.operators.abstraction;

import java.util.HashMap;
import java.util.Map;
import org.processmining.datapetrinets.DataPetriNet;
import org.processmining.datapetrinets.DataPetriNet.PetrinetWithMarkings;
import org.processmining.datapetrinets.DataPetriNetsWithMarkings;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.logenhancement.abstraction.PatternBasedModelTransformation;
import org.processmining.logenhancement.abstraction.model.AbstractionModel;
import org.processmining.logenhancement.abstraction.model.AbstractionPattern;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.graphbased.directed.petrinet.PetrinetGraph;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
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

/**
 * Expands a model using the pattern from the abstraction model.
 * 
 * @author F. Mannhardt
 *
 */
public class ExpandAbstractModelOperator extends Operator {

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

		Petrinet model = inputModel.getData(PetriNetIOObject.class).getArtifact();
		AbstractionModel abstractionModel = inputAbstractionModel.getData(AbstractionModelIOObject.class).getArtifact();

		Map<PetrinetGraph, Transition> defaultValues = new HashMap<>();
		for (AbstractionPattern pattern : abstractionModel.getPatterns()) {
			DataPetriNet dpnPattern = pattern.getDPN();
			Transition transition = getTransition(dpnPattern.getLabel(), model);
			if (transition != null) {
				defaultValues.put(dpnPattern, transition);
			}
		}

		// TODO change in LogEnhancement
		DataPetriNetsWithMarkings expandedModel = (DataPetriNetsWithMarkings) new PatternBasedModelTransformation()
				.doTransformModelBasedOnAbstractionPatterns(model, defaultValues);

		PetrinetWithMarkings pnWithMarkings = DataPetriNet.Factory.toPetrinetWithMarkings(expandedModel);

		output.deliver(new PetriNetIOObject(pnWithMarkings.getNet(), pnWithMarkings.getInitialMarking(),
				pnWithMarkings.getFinalMarkings() != null ? pnWithMarkings.getFinalMarkings()[0] : null, getContext()));
	}

	private Transition getTransition(String label, PetrinetGraph graph) {
		for (Transition t : graph.getTransitions()) {
			if (label.equals(t.getLabel())) {
				return t;
			}
		}
		return null;
	}

	private PluginContext getContext() throws UserError {
		return RapidProMGlobalContext.instance().getPluginContext();
	}

}