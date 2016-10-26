package org.rapidprom.operators.analysis;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.semantics.petrinet.Marking;
import org.processmining.plugins.petrinet.reduction.Murata;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.PetriNetIOObject;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.tools.LogService;

public class MurataReductionOperator extends Operator {

	private InputPort inputPetrinet = getInputPorts().createPort("model (ProM Petri Net)", PetriNetIOObject.class);
	private OutputPort outputPetrinet = getOutputPorts().createPort("model (ProM Petri Net)");

	private static final String VARIATION = "Reduction Method:",
			VARIATION_DESCR = "This parameter defines which method of reduction is used to reduce the petri net.";
	private static final String SIMPLIFY = "Simplify For Replay", REDUCE_ALL = "Reduce All Transitions",
			REDUCE_ALL_RETAIN = "Reduce All Transitions, Retain Sink/Source Places";

	public MurataReductionOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputPetrinet, PetriNetIOObject.class));
	}

	// TO-DO : add parameters
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: reduce petri net");
		long time = System.currentTimeMillis();
		PluginContext pluginContext = RapidProMGlobalContext.instance().getFutureResultAwarePluginContext(Murata.class);
		Murata reducer = new Murata();
		Object[] result = new Object[2];
		try {
			if (getParameterAsString(VARIATION).equals(SIMPLIFY))
				result = reducer.simplify(pluginContext,
						inputPetrinet.getData(PetriNetIOObject.class).getArtifact(),
						inputPetrinet.getData(PetriNetIOObject.class).getInitialMarking());
			else if (getParameterAsString(VARIATION).equals(REDUCE_ALL))
				result = reducer.run(pluginContext,
						inputPetrinet.getData(PetriNetIOObject.class).getArtifact(),
						inputPetrinet.getData(PetriNetIOObject.class).getInitialMarking());
			else {
				result[0] = reducer.runWF(pluginContext, inputPetrinet.getData(PetriNetIOObject.class).getArtifact());
				result[1] = inputPetrinet.getData(PetriNetIOObject.class).getInitialMarking();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		PetriNetIOObject pn = new PetriNetIOObject((Petrinet) result[0], (Marking) result[1], null, pluginContext);
		outputPetrinet.deliver(pn);

		logger.log(Level.INFO,
				"End: reduce petri net (" + (System.currentTimeMillis() - time) / 1000 + " sec)");

	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		String[] options = new String[] { SIMPLIFY, REDUCE_ALL, REDUCE_ALL_RETAIN };

		ParameterTypeCategory variation = new ParameterTypeCategory(VARIATION, VARIATION_DESCR, options, 0);
		parameterTypes.add(variation);

		return parameterTypes;
	}

}