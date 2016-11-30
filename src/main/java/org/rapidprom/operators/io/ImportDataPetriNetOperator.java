package org.rapidprom.operators.io;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;

import org.processmining.dataawareexplorer.utils.PetrinetUtils;
import org.processmining.datapetrinets.DataPetriNetsWithMarkings;
import org.processmining.datapetrinets.io.DataPetriNetImporter;
import org.processmining.datapetrinets.io.DataPetriNetImporter.DPNWithLayout;
import org.processmining.models.graphbased.AttributeMap;
import org.processmining.models.semantics.petrinet.Marking;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.DataPetriNetIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMImportOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeFile;
import com.rapidminer.parameter.ParameterTypeString;

public class ImportDataPetriNetOperator extends AbstractRapidProMImportOperator<DataPetriNetIOObject> {

	private final static String[] SUPPORTED_FILE_FORMATS = new String[] { "pnml" };
	
	private static final String PARAMETER_KEY_LABEL = "Label";
	private static final String PARAMETER_DESC_LABEL = "Label that should be assigned to the imported Data Petri net.";
	
	private static final String PARAMETER_KEY_GUESS_MARKINGS = "Guess Markings";
	private static final String PARAMETER_DESC_GUESS_MARKINGS = "Whether to guess the initial and final marking based on the structure of the Petri net if the marking is not defined in the PNML file.";

	public ImportDataPetriNetOperator(OperatorDescription description) {
		super(description, DataPetriNetIOObject.class, SUPPORTED_FILE_FORMATS);
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
	protected DataPetriNetIOObject read(File file) throws Exception {
		DPNWithLayout dpnLayout = new DataPetriNetImporter().importFromStream(new FileInputStream(file));
		
		DataPetriNetsWithMarkings dpn = dpnLayout.getDPN();
		
		String label = getParameter(PARAMETER_KEY_LABEL);
		if (label != null && !label.isEmpty()) {
			dpn.getAttributeMap().put(AttributeMap.LABEL, label);
		}
		
		if (getParameterAsBoolean(PARAMETER_KEY_GUESS_MARKINGS)) {
			if (dpn.getInitialMarking() == null) {
				dpn.setInitialMarking(PetrinetUtils.guessInitialMarking(dpn));
			}
			if (dpn.getFinalMarkings() == null) {
				Marking guessFinalMarking = PetrinetUtils.guessFinalMarking(dpn);
				if (guessFinalMarking != null) {
					dpn.setFinalMarkings(new Marking[] {guessFinalMarking});	
				}				
			}
		}
		
		return new DataPetriNetIOObject(dpn, dpn.getInitialMarking(),
				dpn.getFinalMarkings() != null ? dpn.getFinalMarkings()[0] : null,
				RapidProMGlobalContext.instance().getPluginContext());
	}

}