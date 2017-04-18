package org.rapidprom.operators.io;

import java.io.File;
import java.util.List;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.models.cnet.CausalNet;
import org.processmining.models.cnet.importing.ImportCNet;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.CausalNetIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMImportOperator;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeFile;

public class ImportCausalNetOperator extends AbstractRapidProMImportOperator<CausalNetIOObject> {

	private final static String[] SUPPORTED_FILE_FORMATS = new String[] { "cnet" };

	public ImportCausalNetOperator(OperatorDescription description) {
		super(description, CausalNetIOObject.class, SUPPORTED_FILE_FORMATS);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		types.add(new ParameterTypeFile(PARAMETER_KEY_FILE, PARAMETER_DESC_FILE, false, SUPPORTED_FILE_FORMATS));
		return types;
	}

	@Override
	protected CausalNetIOObject read(File file) throws Exception {
		PluginContext context = RapidProMGlobalContext.instance().getFutureResultAwarePluginContext(ImportCNet.class);
		ImportCNet importer = new ImportCNet();

		Object[] result = (Object[]) importer.importFile(context, getParameterAsFile(PARAMETER_KEY_FILE));
		CausalNet cnet = (CausalNet) result[0];

		CausalNetIOObject cNetIO = new CausalNetIOObject(cnet, context);
		return cNetIO;
	}

}