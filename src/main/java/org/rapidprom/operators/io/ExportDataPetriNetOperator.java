package org.rapidprom.operators.io;

import java.io.File;
import java.io.IOException;

import org.processmining.datapetrinets.DataPetriNet;
import org.processmining.datapetrinets.io.DataPetriNetExporter;
import org.rapidprom.ioobjects.DataPetriNetIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMExporterOperator;
import org.rapidprom.util.IOUtils;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;

public class ExportDataPetriNetOperator
		extends AbstractRapidProMExporterOperator<DataPetriNetIOObject, DataPetriNet, String> {

	private static final String PNML = "pnml";

	public ExportDataPetriNetOperator(OperatorDescription description) {
		super(description, DataPetriNetIOObject.class, new String[] { PNML }, PNML);
	}

	@Override
	public DataPetriNetIOObject write(DataPetriNetIOObject netIO) throws OperatorException {
		try {
			String format = PARAMETER_VALUES_FILE_FORMAT[getParameterAsInt(PARAMETER_KEY_FILE_FORMAT)];
			File target = IOUtils.prepareTargetFile(getParameterAsFile(PARAMETER_KEY_FOLDER).getCanonicalPath(),
					getParameterAsString(PARAMETER_KEY_FILE_NAME), format);
			if (target.exists()) {
				target.delete();
			}
			new DataPetriNetExporter().export(netIO.getArtifact(), target);
			return netIO;
		} catch (Exception e) {
			throw new OperatorException("Failed to save DPN to file. ", e);
		}
	}

	@Override
	protected void writeToFile(File file, DataPetriNet net, String format) throws IOException {
		// Not used here
	}

}
