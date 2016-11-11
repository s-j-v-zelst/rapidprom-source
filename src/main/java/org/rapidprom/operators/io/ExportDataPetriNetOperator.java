package org.rapidprom.operators.io;

import java.io.File;
import java.io.IOException;
import org.processmining.datapetrinets.DataPetriNet;
import org.processmining.datapetrinets.io.DataPetriNetExporter;
import org.rapidprom.ioobjects.DataPetriNetIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMExporterOperator;

import com.rapidminer.operator.OperatorDescription;

public class ExportDataPetriNetOperator
		extends AbstractRapidProMExporterOperator<DataPetriNetIOObject, DataPetriNet, String> {

	private static final String PNML = "pnml";

	public ExportDataPetriNetOperator(OperatorDescription description) {
		super(description, DataPetriNetIOObject.class, new String[] { PNML }, PNML);
	}

	@Override
	protected void writeToFile(File file, DataPetriNet net, String format) throws IOException {
		try {
			new DataPetriNetExporter().export(net, file);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

}
