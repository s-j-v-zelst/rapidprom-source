package org.rapidprom.operators.decomposition;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.rapidprom.ioobjects.PetriNetRPSTIOObject;
import org.rapidprom.operators.decomposition.rpst.PetriNetRPST;
import org.rapidprom.operators.decomposition.rpst.PetriNetRPSTNode;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.UndefinedParameterError;

public class FilterRPSTOperator extends Operator {

	private static final String MIN_SIZE_PARAMETER_KEY = "Minimum size";
	private static final String MIN_SIZE_PARAMETER_DESCR = "Minimum size for the SESE fragments in the decomposition.";	
	
	private final InputPort input = getInputPorts().createPort("rpst (ProM Petri Net RPST)",
			PetriNetRPSTIOObject.class);
	private final OutputPort output = getOutputPorts().createPort("rpst (ProM Petri Net RPST)");

	public FilterRPSTOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, PetriNetRPSTIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {
		PetriNetRPSTIOObject rpstIO = input.getData(PetriNetRPSTIOObject.class);
		PetriNetRPST rpstFiltered = filter(rpstIO.getArtifact());
		output.deliver(new PetriNetRPSTIOObject(rpstFiltered, rpstIO.getPluginContext()));
	}

	private PetriNetRPST filter(PetriNetRPST rpst) throws UndefinedParameterError {
		PetriNetRPST rpstFiltered = rpst.clone();
		filterMinSize(rpstFiltered, getParameterAsInt(MIN_SIZE_PARAMETER_KEY));
		return rpstFiltered;
	}

	private void filterMinSize(PetriNetRPST rpst, int minSize) {
		Set<PetriNetRPSTNode> nodesToRemove = new HashSet<>();
		for (PetriNetRPSTNode node : rpst.getNodes()) {
			if (determineSize(node) < minSize) {
				nodesToRemove.add(node);
			}
		}
		nodesToRemove.remove(rpst.getRoot()); // Never Remove the Root

		for (PetriNetRPSTNode nodeToRemove : nodesToRemove) {
			rpst.removeNodeAndCollapse(nodeToRemove);
		}
	}

	private int determineSize(PetriNetRPSTNode node) {
		int num = 0;
		for (Transition trans: node.getTrans()) {
			if (!trans.isInvisible()) {
				num++;
			}
		}
		return num;
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeInt(MIN_SIZE_PARAMETER_KEY, MIN_SIZE_PARAMETER_DESCR, 1, Integer.MAX_VALUE, 10));
		return params;
	}

}