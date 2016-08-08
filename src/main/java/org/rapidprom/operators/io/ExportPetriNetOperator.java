package org.rapidprom.operators.io;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;

import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.petrinets.PetriNetFileFormat;
import org.processmining.plugins.pnml.exporting.PnmlExportNetToEPNML;
import org.processmining.plugins.pnml.exporting.PnmlExportNetToPNML;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMExporterOperator;

import com.rapidminer.operator.OperatorDescription;

public class ExportPetriNetOperator
		extends AbstractRapidProMExporterOperator<PetriNetIOObject, Petrinet, PetriNetFileFormat> {

	public ExportPetriNetOperator(OperatorDescription description) {
		super(description, PetriNetIOObject.class,
				EnumSet.allOf(PetriNetFileFormat.class)
						.toArray(new PetriNetFileFormat[EnumSet.allOf(PetriNetFileFormat.class).size()]),
				PetriNetFileFormat.PNML);
	}

	@Override
	protected void writeToFile(File file, Petrinet object, PetriNetFileFormat format) throws IOException {
		switch (format) {
		case EPNML:
			PnmlExportNetToEPNML exporterEPNML = new PnmlExportNetToEPNML();
			exporterEPNML.exportPetriNetToEPNMLFile(
					RapidProMGlobalContext.instance().getFutureResultAwarePluginContext(PnmlExportNetToEPNML.class),
					object, file);
			break;
		case PNML:
		default:
			PnmlExportNetToPNML exporterPNML = new PnmlExportNetToPNML();
			exporterPNML.exportPetriNetToPNMLFile(
					RapidProMGlobalContext.instance().getFutureResultAwarePluginContext(PnmlExportNetToPNML.class),
					object, file);
			break;
		}
	}

}
