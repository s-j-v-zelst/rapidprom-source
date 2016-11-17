package org.rapidprom.operators.io;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;

import org.processmining.datapetrinets.DataPetriNetsWithMarkings;
import org.processmining.datapetrinets.io.DataPetriNetImporter;
import org.processmining.datapetrinets.io.DataPetriNetImporter.DPNWithLayout;
import org.processmining.models.graphbased.AttributeMap;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.DataPetriNetIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMImportOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeFile;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.UndefinedParameterError;

public class ImportDataPetriNetOperator extends AbstractRapidProMImportOperator<DataPetriNetIOObject> {

	private final static String[] SUPPORTED_FILE_FORMATS = new String[] { "pnml" };
	private static final String PARAMETER_KEY_LABEL = "Label";
	private static final String PARAMETER_DESC_LABEL = "Label that should be assigned to the imported Data Petri net.";

	public ImportDataPetriNetOperator(OperatorDescription description) {
		super(description, DataPetriNetIOObject.class, SUPPORTED_FILE_FORMATS);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		types.add(new ParameterTypeFile(PARAMETER_KEY_FILE, PARAMETER_DESC_FILE, false, SUPPORTED_FILE_FORMATS));
		types.add(new ParameterTypeString(PARAMETER_KEY_LABEL, PARAMETER_DESC_LABEL, true, true));
		return types;
	}

	@Override
	protected DataPetriNetIOObject read(File file) throws Exception {
		DPNWithLayout dpnLayout = new DataPetriNetImporter().importFromStream(new FileInputStream(file));
		DataPetriNetsWithMarkings dpn = dpnLayout.getDPN();
		assignLabel(dpn);
		return new DataPetriNetIOObject(dpn, dpn.getInitialMarking(),
				dpn.getFinalMarkings() != null ? dpn.getFinalMarkings()[0] : null,
				RapidProMGlobalContext.instance().getPluginContext());
	}

	private void assignLabel(DataPetriNetsWithMarkings dpn) throws UndefinedParameterError {
		String label = getParameter(PARAMETER_KEY_LABEL);
		if (label != null && !label.isEmpty()) {
			dpn.getAttributeMap().put(AttributeMap.LABEL, label);
		}
	}

}