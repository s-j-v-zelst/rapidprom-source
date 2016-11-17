package org.rapidprom.operators.io;

import java.io.File;
import java.util.List;

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
import com.rapidminer.parameter.ParameterTypeFile;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.UndefinedParameterError;

public class ImportPetriNetOperator extends AbstractRapidProMImportOperator<PetriNetIOObject> {

	private final static String[] SUPPORTED_FILE_FORMATS = new String[] { "pnml" };
	private static final String PARAMETER_KEY_LABEL = "Label";
	private static final String PARAMETER_DESC_LABEL = "Label that should be assigned to the imported Data Petri net.";

	public ImportPetriNetOperator(OperatorDescription description) {
		super(description, PetriNetIOObject.class, SUPPORTED_FILE_FORMATS);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		types.add(new ParameterTypeFile(PARAMETER_KEY_FILE, PARAMETER_DESC_FILE, false, SUPPORTED_FILE_FORMATS));
		types.add(new ParameterTypeString(PARAMETER_KEY_LABEL, PARAMETER_DESC_LABEL, true, true));		
		return types;
	}

	@Override
	protected PetriNetIOObject read(File file) throws Exception {
		PluginContext context = RapidProMGlobalContext.instance()
				.getFutureResultAwarePluginContext(PnmlImportNet.class);
		PnmlImportNet importer = new PnmlImportNet();
		Object[] result = null;
		try {
			result = (Object[]) importer.importFile(context, getParameterAsFile(PARAMETER_KEY_FILE));
			assignLabel((Petrinet) result[0]);
		} catch (Exception e) {
			e.printStackTrace();
		}
		PetriNetIOObject pnResult = new PetriNetIOObject((Petrinet) result[0], (Marking) result[1], null, context);
		return pnResult;
	}
	
	private void assignLabel(Petrinet pn) throws UndefinedParameterError {
		String label = getParameter(PARAMETER_KEY_LABEL);
		if (label != null && !label.isEmpty()) {
			pn.getAttributeMap().put(AttributeMap.LABEL, label);
		}
	}
	
}