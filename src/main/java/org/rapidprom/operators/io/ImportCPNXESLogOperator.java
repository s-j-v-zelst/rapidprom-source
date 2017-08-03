package org.rapidprom.operators.io;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.deckfour.xes.model.XLog;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.logenhancement.cpn.LoadCPNXESFileWithData;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMImportOperator;
import org.rapidprom.operators.ports.metadata.XLogIOObjectMetaData;
import org.rapidprom.operators.util.RapidProMProgress;

import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeFile;

/**
 * Import event log in cpnxes format created by CPN Tools
 * 
 */
public class ImportCPNXESLogOperator extends AbstractRapidProMImportOperator<XLogIOObject> {

	private final static String[] SUPPORTED_FILE_FORMATS = new String[] { "cpnxes", "cpnxes.gz" };

	public ImportCPNXESLogOperator(OperatorDescription description) {
		super(description, XLogIOObject.class, SUPPORTED_FILE_FORMATS);
	}

	@Override
	public MetaData getGeneratedMetaData() throws OperatorException {
		return new XLogIOObjectMetaData();
	}

	@Override
	protected XLogIOObject read(File file) throws Exception {		
		PluginContext context = RapidProMGlobalContext.instance().getFutureResultAwarePluginContext(LoadCPNXESFileWithData.class, new RapidProMProgress(getProgress()));		
		LoadCPNXESFileWithData loadCPNXES = new LoadCPNXESFileWithData();
		if (file.getName().endsWith("cpnxes.gz")) {
			XLog log = loadCPNXES.importCPNFile(context, new GZIPInputStream(new FileInputStream(file), 65536),
					file.getName());
			return new XLogIOObject(log, context);
		} else {
			XLog log = loadCPNXES.importCPNFile(context, new BufferedInputStream(new FileInputStream(file), 65536),
					file.getName());
			return new XLogIOObject(log, context);	
		}
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		types.add(new ParameterTypeFile(PARAMETER_KEY_FILE, PARAMETER_DESC_FILE, false, SUPPORTED_FILE_FORMATS));
		return types;
	}

}