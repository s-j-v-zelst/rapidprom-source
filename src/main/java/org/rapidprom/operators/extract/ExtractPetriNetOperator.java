package org.rapidprom.operators.extract;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.semantics.petrinet.Marking;
import org.processmining.plugins.pnml.importing.PnmlImportNet;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMExtractorOperator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.nio.file.FileObject;
import com.rapidminer.tools.LogService;

public class ExtractPetriNetOperator extends AbstractRapidProMExtractorOperator<PetriNetIOObject> {

	private File currentFile = null;
	
	public ExtractPetriNetOperator(OperatorDescription description) {
		super(description, PetriNetIOObject.class);
	}

	protected PetriNetIOObject read(File file) throws Exception {
		PluginContext context = RapidProMGlobalContext.instance()
				.getFutureResultAwarePluginContext(PnmlImportNet.class);
		PnmlImportNet importer = new PnmlImportNet();
		Object[] result = null;
		try {
			result = (Object[]) importer.importFile(context, file.getAbsolutePath());
		} catch (Exception e) {
			e.printStackTrace();
		}
		PetriNetIOObject pnResult = new PetriNetIOObject((Petrinet) result[0], (Marking) result[1], null, context);
		return pnResult;
	}

	@Override
	public PetriNetIOObject read() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: importing petri net");
		long time = System.currentTimeMillis();
		
		PetriNetIOObject petrinet = null;
		try {
			petrinet = read(getFile());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		logger.log(Level.INFO, "End: importing petri net (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
		return petrinet;
	}
	
	protected File getFile() throws UserError {
		try {
			File file = inputfile.getData(FileObject.class).getFile();
			this.currentFile = file;
		} catch (OperatorException e) {
			// Do nothing
		}
		return currentFile;
	}

}
