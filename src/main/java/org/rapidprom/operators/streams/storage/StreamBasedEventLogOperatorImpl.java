package org.rapidprom.operators.streams.storage;

import java.util.EnumSet;
import java.util.List;

import org.processmining.eventstream.core.interfaces.XSStaticXSEventStream;
import org.processmining.streambasedeventlog.algorithms.CaseLevelReservoirSamplingBasedEventLogImpl;
import org.processmining.streambasedeventlog.algorithms.EventLevelReservoirSamplingBasedEventLogImpl;
import org.processmining.streambasedeventlog.algorithms.SlidingWindowBasedEventLogImpl;
import org.processmining.streambasedeventlog.algorithms.TrieBasedEventCollectorImpl;
import org.processmining.streambasedeventlog.models.EventPayload;
import org.processmining.streambasedeventlog.models.XSEventStreamToXLogReader;
import org.processmining.streambasedeventlog.parameters.StreamBasedEventStorageParametersImpl;
import org.processmining.streambasedeventstorage.algorithms.XSEventStoreSlidingWindowImpl;
import org.processmining.streambasedeventstorage.parameters.XSEventStoreReservoirCaseLevelSamplingParametersImpl;
import org.processmining.streambasedeventstorage.parameters.XSEventStoreReservoirEventLevelSamplingParametersImpl;
import org.processmining.streambasedeventstorage.parameters.XSEventStoreSlidingWindowParametersImpl;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.ioobjects.streams.event.XSStaticXSEventStreamIOObject;
import org.rapidprom.operators.meta.RPAbstractIteratingOperatorChain;
import org.rapidprom.util.ExampleSetUtils;
import org.rapidprom.util.ObjectUtils;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.utils.ExampleSetBuilder;
import com.rapidminer.example.utils.ExampleSets;
import com.rapidminer.operator.IOContainer;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.io.AbstractDataReader.AttributeColumn;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.Port;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MDInteger;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.conditions.EqualStringCondition;
import com.rapidminer.tools.Ontology;

public class StreamBasedEventLogOperatorImpl extends RPAbstractIteratingOperatorChain {

	private static enum StorageTechnology {
		SLIDING_WINDOW("Sliding Window"), RESERVOIR_SAMPLING_CASE_LEVEL(
				"Reservoir Sampling Case Level"), RESERVOIR_SAMPLING_EVENT_LEVEL(
						"Reservoir Sampling EVENT Level"), TRIE("Prefix-Tree");

		private final String humanReadable;

		private StorageTechnology(final String toStr) {
			this.humanReadable = toStr;
		}

		@Override
		public String toString() {
			return humanReadable;
		}
	}

	private final static String PARAMETER_KEY_STORAGE_TECHNOLOGY = "Storage Technology";
	private final static String PARAMETER_DESC_STORAGE_TECHNOLOGY = "Describes the underlying storage technique to use for event storage.";
	private final static StorageTechnology[] PARAMETER_VALUES_STORAGE_TECHNOLOGY = EnumSet
			.allOf(StorageTechnology.class)
			.toArray(new StorageTechnology[EnumSet.allOf(StorageTechnology.class).size()]);

	private final static String PARAMETER_KEY_SW_RES_SIZE = "Size";
	private final static String PARAMETER_DESC_SW_RES_SIZE = "Size of the sliding window / reservoir";
	private final static int PARAMETER_DEFAULT_VAL_SW_RES_SIZE = 1000;

	private final static String PARAMETER_KEY_RES_CASE_LEVEL_INTERNAL_SIZE = "Internal Size";
	private final static String PARAMETER_DESC_RES_CASE_LEVEL_INTERNAL_SIZE = "Internal size of the individual cells in the (case-level) reservoir";
	private final static int PARAMETER_DEFAULT_VAL_RES_CASE_LEVEL_INTERNAL_SIZE = 25;

	private final static String PARAMETER_KEY_STOP_CRITERION = "Stop Criterion";
	private final static String PARAMETER_DESC_STOP_CRITERION = "Indicates after which number of received events the internal workflow needs to stop";
	private final static int PARAMETER_DEFAULT_STOP_CRITERION = 1000;

	private int eventIndex = -1;
	private XSEventStreamToXLogReader<StreamBasedEventStorageParametersImpl> coll = null;

	// input port for stream
	private final InputPort inputEventStream = getInputPorts().createPort("static event stream",
			XSStaticXSEventStreamIOObject.class);

	// input port with generated XLog/memory statistics
	private final OutputPort internalInputEventLog = getSubprocess(0).getInnerSources().createPort("xlog", true);
	private final OutputPort internalInputMemoryPerformance = getSubprocess(0).getInnerSources().createPort("memory",
			true);
	private final DataRowFactory dataRowFactory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

	private final String COLUMN_EVENT = "event";
	private final String COLUMN_MEM_REAL = "memory_real";
	private final String COLUMN_MEM_VIRTUAL = "memory_virtual";

	private final MDInteger[] COLUMNS_MEM_PERF_MISSING = new MDInteger[] { new MDInteger(0), new MDInteger(0),
			new MDInteger(0) };
	private final String[] COLUMNS_MEM_PERF_NAMES = new String[] { COLUMN_EVENT, COLUMN_MEM_REAL, COLUMN_MEM_VIRTUAL };
	private final String[] COLUMNS_MEM_PERF_ROLES = new String[] { AttributeColumn.REGULAR, AttributeColumn.REGULAR,
			AttributeColumn.REGULAR };
	private final int[] COLUMNS_MEM_PERF_TYPES = new int[] { Ontology.INTEGER, Ontology.INTEGER, Ontology.INTEGER };

	private ExampleSetMetaData outputMemoryPerformanceMD = new ExampleSetMetaData();
	private OutputPort outputPortMemoryPerformance = getOutputPorts().createPort("example set with memory performance");

	public StreamBasedEventLogOperatorImpl(OperatorDescription description) {
		super(description);
		outputMemoryPerformanceMD = ExampleSetUtils.constructExampleSetMetaData(outputMemoryPerformanceMD,
				COLUMNS_MEM_PERF_NAMES, COLUMNS_MEM_PERF_TYPES, COLUMNS_MEM_PERF_ROLES, COLUMNS_MEM_PERF_MISSING);
		getTransformer().addRule(new GenerateNewMDRule(outputPortMemoryPerformance, outputMemoryPerformanceMD));
	}

	@Override
	public void doWork() throws OperatorException {
		if (coll == null) {
			eventIndex = 0;
			StreamBasedEventStorageParametersImpl params = new StreamBasedEventStorageParametersImpl();
			params.setSlidingWindowSize(getParameterAsInt(PARAMETER_KEY_SW_RES_SIZE));
			int size = getParameterAsInt(PARAMETER_KEY_SW_RES_SIZE);
			switch (PARAMETER_VALUES_STORAGE_TECHNOLOGY[getParameterAsInt(PARAMETER_KEY_STORAGE_TECHNOLOGY)]) {
			case RESERVOIR_SAMPLING_EVENT_LEVEL:
				XSEventStoreReservoirEventLevelSamplingParametersImpl resParams = new XSEventStoreReservoirEventLevelSamplingParametersImpl();
				resParams.setSize(size);
				resParams.setSeeded(false);
				coll = new EventLevelReservoirSamplingBasedEventLogImpl<StreamBasedEventStorageParametersImpl>(params,
						resParams);
				break;
			case RESERVOIR_SAMPLING_CASE_LEVEL:
				XSEventStoreReservoirCaseLevelSamplingParametersImpl resParamsCl = new XSEventStoreReservoirCaseLevelSamplingParametersImpl();
				resParamsCl.setSize(size);
				resParamsCl.setSeeded(false);
				int internalSize = getParameterAsInt(PARAMETER_KEY_RES_CASE_LEVEL_INTERNAL_SIZE);
				resParamsCl.setMaxEntrySize(internalSize);
				coll = new CaseLevelReservoirSamplingBasedEventLogImpl<StreamBasedEventStorageParametersImpl>(params,
						resParamsCl);
				break;
			case SLIDING_WINDOW:
				XSEventStoreSlidingWindowParametersImpl swParams = new XSEventStoreSlidingWindowParametersImpl();
				swParams.setSize(size);
				coll = new SlidingWindowBasedEventLogImpl<StreamBasedEventStorageParametersImpl>(params, swParams);
				break;
			case TRIE:
				XSEventStoreSlidingWindowParametersImpl swParamsTr = new XSEventStoreSlidingWindowParametersImpl();
				swParamsTr.setSize(size);
				XSEventStoreSlidingWindowImpl slidingWindow = new XSEventStoreSlidingWindowImpl(swParamsTr);
				coll = new TrieBasedEventCollectorImpl<EventPayload, StreamBasedEventStorageParametersImpl>(params,
						new EventPayload.FactoryNaiveImpl(), slidingWindow);
				break;
			default:
				break;
			}
			assert (coll != null);
		}
		super.doWork();
	}

	@Override
	protected boolean shouldStop(IOContainer iterationResults) throws OperatorException {
		XSStaticXSEventStream stream = inputEventStream.getData(XSStaticXSEventStreamIOObject.class).getArtifact();
		if (eventIndex < stream.size() && eventIndex < getParameterAsInt(PARAMETER_KEY_STOP_CRITERION)) {
			coll.triggerPacketHandle(stream.get(eventIndex));
			eventIndex++;
			internalInputEventLog.clear(Port.CLEAR_DATA);
			internalInputEventLog.deliver(
					new XLogIOObject(coll.getCurrentResult(), RapidProMGlobalContext.instance().getPluginContext()));
			internalInputMemoryPerformance.clear(Port.CLEAR_DATA);
			internalInputMemoryPerformance.deliver(constructMemoryPerformanceExampleSet());
			return false;
		} else {
			resetOperator();
			return true;
		}
	}

	private ExampleSet constructMemoryPerformanceExampleSet() {
		Attribute[] attributes = new Attribute[COLUMNS_MEM_PERF_NAMES.length];
		for (int i = 0; i < COLUMNS_MEM_PERF_NAMES.length; i++) {
			attributes[i] = AttributeFactory.createAttribute(COLUMNS_MEM_PERF_NAMES[i], COLUMNS_MEM_PERF_TYPES[i]);
		}
		Object[] values = new Object[COLUMNS_MEM_PERF_NAMES.length];
		values[0] = coll.getTotalNumberOfEventsDescribedByMemory();
		values[1] = coll.getTotalPayloadMemoryOccupation();
		values[2] = coll.getNumberOfMemoryEntriesRepresentingEvents();
		ExampleSetBuilder table = ExampleSets.from(attributes);
		table.addDataRow(dataRowFactory.create(values, attributes));
		return table.build();
	}

	private void resetOperator() {
		eventIndex = -1;
		coll = null;
	}

	@Override
	public void processFinished() throws OperatorException {
		super.processFinished();
		resetOperator();
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();

		ParameterTypeCategory storageStrat = new ParameterTypeCategory(PARAMETER_KEY_STORAGE_TECHNOLOGY,
				PARAMETER_DESC_STORAGE_TECHNOLOGY, ObjectUtils.toString(PARAMETER_VALUES_STORAGE_TECHNOLOGY), 0, false);
		params.add(storageStrat);

		ParameterTypeInt swResSize = new ParameterTypeInt(PARAMETER_KEY_SW_RES_SIZE, PARAMETER_DESC_SW_RES_SIZE, 0,
				Integer.MAX_VALUE, PARAMETER_DEFAULT_VAL_SW_RES_SIZE, false);
		params.add(swResSize);

		ParameterTypeInt resIntsize = new ParameterTypeInt(PARAMETER_KEY_RES_CASE_LEVEL_INTERNAL_SIZE,
				PARAMETER_DESC_RES_CASE_LEVEL_INTERNAL_SIZE, 0, Integer.MAX_VALUE,
				PARAMETER_DEFAULT_VAL_RES_CASE_LEVEL_INTERNAL_SIZE, false);
		resIntsize.registerDependencyCondition(new EqualStringCondition(this, PARAMETER_KEY_STORAGE_TECHNOLOGY, true,
				StorageTechnology.RESERVOIR_SAMPLING_CASE_LEVEL.toString()));
		params.add(resIntsize);

		ParameterTypeInt stopCriterion = new ParameterTypeInt(PARAMETER_KEY_STOP_CRITERION,
				PARAMETER_DESC_STOP_CRITERION, 0, Integer.MAX_VALUE, PARAMETER_DEFAULT_STOP_CRITERION, false);
		params.add(stopCriterion);

		return params;
	}

}
