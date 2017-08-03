package org.rapidprom.operators.io;

import java.io.File;
import java.util.List;

import org.processmining.dataawareexplorer.utils.PetrinetUtils;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.models.graphbased.AttributeMap;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.semantics.petrinet.Marking;
import org.processmining.plugins.pnml.importing.PnmlImportNet;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMImportOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeFile;
import com.rapidminer.parameter.ParameterTypeString;

public class ImportPetriNetOperator extends AbstractRapidProMImportOperator<PetriNetIOObject> {

	private final static String[] SUPPORTED_FILE_FORMATS = new String[] { "pnml" };
	
	private static final String PARAMETER_KEY_LABEL = "Label";
	private static final String PARAMETER_DESC_LABEL = "Label that should be assigned to the imported Data Petri net.";
	
	private static final String PARAMETER_KEY_GUESS_MARKINGS = "Guess Markings";
	private static final String PARAMETER_DESC_GUESS_MARKINGS = "Whether to guess the initial and final marking based on the structure of the Petri net if the marking is not defined in the PNML file.";

	public ImportPetriNetOperator(OperatorDescription description) {
		super(description, PetriNetIOObject.class, SUPPORTED_FILE_FORMATS);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		types.add(new ParameterTypeFile(PARAMETER_KEY_FILE, PARAMETER_DESC_FILE, false, SUPPORTED_FILE_FORMATS));
		types.add(new ParameterTypeString(PARAMETER_KEY_LABEL, PARAMETER_DESC_LABEL, true, true));
		types.add(new ParameterTypeBoolean(PARAMETER_KEY_GUESS_MARKINGS, PARAMETER_DESC_GUESS_MARKINGS, true, true));		
		return types;
	}

	@Override
	protected PetriNetIOObject read(File file) throws Exception {
		PluginContext context = RapidProMGlobalContext.instance()
				.getFutureResultAwarePluginContext(PnmlImportNet.class);
		PnmlImportNet importer = new PnmlImportNet();

		Object[] result = (Object[]) importer.importFile(context, getParameterAsFile(PARAMETER_KEY_FILE));
		Petrinet pn = (Petrinet) result[0];
		Marking initialMarking = (Marking) result[1];
		Marking guessedFinalMarking = null; // standard PNML is missing final marking 
		
		String label = getParameter(PARAMETER_KEY_LABEL);
		if (label != null && !label.isEmpty()) {
			pn.getAttributeMap().put(AttributeMap.LABEL, label);
		}
		
		if (getParameterAsBoolean(PARAMETER_KEY_GUESS_MARKINGS)) {
			if (initialMarking == null) {
				initialMarking = PetrinetUtils.guessInitialMarking(pn);
			}
			guessedFinalMarking = PetrinetUtils.guessFinalMarking(pn);
		}
		
		PetriNetIOObject pnResult = new PetriNetIOObject(pn, initialMarking, guessedFinalMarking, context);
		return pnResult;
	}
	
}