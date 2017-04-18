package org.rapidprom.operators.util;

import java.util.EnumSet;
import java.util.List;

import org.deckfour.xes.factory.XFactoryNaiveImpl;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.rapidprom.operators.extract.ExtractXLogOperator.ImplementingPlugin;
import org.xeslite.external.XFactoryExternalStore;
import org.xeslite.lite.factory.XFactoryLiteImpl;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;

/**
 * Choose XFactory implementation
 * 
 * @author F. Mannhardt
 *
 */
public class XLogFactorySelectionOperator extends Operator {

	private final static String PARAMETER_KEY_IMPORTER = "importer";
	private final static String PARAMETER_DESC_IMPORTER = "Select the implementing importer, importers differ in terms of performance: "
			+ "The \"Naive\" importer loads the Log completely in memory (faster, but more memory usage). "
			+ "The \"Buffered by MAPDB\" importer loads only log, trace and event ids, "
			+ "and the rest of the data (mainly attribute values) are stored in disk by MapDB "
			+ "(slower, but less memory usage). "
			+ "The \"Lightweight & Sequential IDs\" importer is a balance between the \"Naive\" and the \"Buffered by MapDB\" importers";

	private final static ImplementingPlugin[] PARAMETER_OPTIONS_IMPORTER = EnumSet.allOf(ImplementingPlugin.class)
			.toArray(new ImplementingPlugin[EnumSet.allOf(ImplementingPlugin.class).size()]);

	public XLogFactorySelectionOperator(OperatorDescription description) {
		super(description);
	}

	@Override
	public void doWork() throws OperatorException {

		ImplementingPlugin importPlugin = PARAMETER_OPTIONS_IMPORTER[getParameterAsInt(PARAMETER_KEY_IMPORTER)];

		switch (importPlugin) {
		case LIGHT_WEIGHT_SEQ_ID:
			XFactoryRegistry.instance().setCurrentDefault(new XFactoryLiteImpl());
			break;
		case XESLITE_MAP_DB:
			XFactoryRegistry.instance().setCurrentDefault(new XFactoryExternalStore.MapDBDiskWithoutCacheImpl());
			break;
		case XESLITE_MAP_DB_SEQUENTIAL:
			XFactoryRegistry.instance()
					.setCurrentDefault(new XFactoryExternalStore.MapDBDiskSequentialAccessWithoutCacheImpl());
			break;
		case XESLITE_IN_MEMORY:
			XFactoryRegistry.instance().setCurrentDefault(new XFactoryExternalStore.InMemoryStoreImpl());
			break;
		case NAIVE:
		default:
			XFactoryRegistry.instance().setCurrentDefault(new XFactoryNaiveImpl());
			break;
		}

	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		types.add(createImporterParameterTypeCategory(PARAMETER_KEY_IMPORTER, PARAMETER_DESC_IMPORTER,
				PARAMETER_OPTIONS_IMPORTER));
		return types;
	}

	private ParameterType createImporterParameterTypeCategory(String key, String desc, ImplementingPlugin[] importers) {
		String[] importersStr = new String[importers.length];
		for (int i = 0; i < importersStr.length; i++) {
			importersStr[i] = importers[i].toString();
		}
		return new ParameterTypeCategory(key, desc, importersStr, 0, true);
	}
}
