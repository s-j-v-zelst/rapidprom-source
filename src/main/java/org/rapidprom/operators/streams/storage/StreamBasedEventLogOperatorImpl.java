package org.rapidprom.operators.streams.storage;

import java.util.EnumSet;
import java.util.List;

import org.processmining.eventstream.core.interfaces.XSStaticXSEventStream;
import org.processmining.streambasedeventlog.algorithms.ReservoirSamplingBasedEventCollectorImpl;
import org.processmining.streambasedeventlog.algorithms.SlidingWindowBasedEventCollectorImpl;
import org.processmining.streambasedeventlog.algorithms.TrieBasedEventCollectorImpl;
import org.processmining.streambasedeventlog.models.EventPayload;
import org.processmining.streambasedeventlog.models.XSEventStreamToXLogReader;
import org.processmining.streambasedeventlog.parameters.StreamBasedEventStorageParametersImpl;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.ioobjects.streams.event.XSStaticXSEventStreamIOObject;
import org.rapidprom.operators.meta.RPAbstractIteratingOperatorChain;
import org.rapidprom.util.ObjectUtils;

import com.rapidminer.operator.IOContainer;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.Port;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeInt;

public class StreamBasedEventLogOperatorImpl extends RPAbstractIteratingOperatorChain {

	private static enum StorageTechnology {
		SLIDING_WINDOW("Sliding Window"), RESERVOIR_SAMPLING("Reservoir Sampling"), TRIE("Prefix-Tree");

		private final String humanReadable;

		private StorageTechnology(final String toStr) {
			this.humanReadable = toStr;
		}

		@Override
		public String toString() {
			return humanReadable;
		}
	}

	private final static String PARAM_KEY_STORAGE_TECHNOLOGY = "Storage Technology";
	private final static String PARAM_DESC_STORAGE_TECHNOLOGY = "Describes the underlying storage technique to use for event storage.";
	private final static StorageTechnology[] PARAM_VALUES_STORAGE_TECHNOLOGY = EnumSet.allOf(StorageTechnology.class)
			.toArray(new StorageTechnology[EnumSet.allOf(StorageTechnology.class).size()]);

	private final static String PARAM_KEY_SW_RES_SIZE = "Size";
	private final static String PARAM_DESC_SW_RES_SIZE = "Size of the sliding window / reservoir";
	private final static int PARAM_DEFAULT_VAL_SW_RES_SIZE = 1000;

	private final static String PARAM_KEY_IGNORE_WARMUP_EVENTS = "Ignore Warm-up Events";
	private final static String PARAM_DESC_IGNORE_WARMUP_EVENTS = "Ignores the event logs that are generated during warm-up";
	private final static boolean PARAM_DEFAULT_IGNORE_WARMUP_EVENTS = true;

	private int eventIndex = -1;
	private XSEventStreamToXLogReader<StreamBasedEventStorageParametersImpl> coll = null;
	private boolean start = false;

	// input port for stream
	private final InputPort inputEventStream = getInputPorts().createPort("static event stream",
			XSStaticXSEventStreamIOObject.class);

	// input port with generated XLog
	private final OutputPort internalInputEventLog = getSubprocess(0).getInnerSources().createPort("xlog", true);

	public StreamBasedEventLogOperatorImpl(OperatorDescription description) {
		super(description);
	}

	@Override
	public void doWork() throws OperatorException {
		if (coll == null) {
			eventIndex = 0;
			StreamBasedEventStorageParametersImpl params = new StreamBasedEventStorageParametersImpl();
			params.setSlidingWindowSize(getParameterAsInt(PARAM_KEY_SW_RES_SIZE));
			switch (PARAM_VALUES_STORAGE_TECHNOLOGY[getParameterAsInt(PARAM_KEY_STORAGE_TECHNOLOGY)]) {
			case RESERVOIR_SAMPLING:
				coll = new ReservoirSamplingBasedEventCollectorImpl<StreamBasedEventStorageParametersImpl>(params);
				break;
			case SLIDING_WINDOW:
				coll = new SlidingWindowBasedEventCollectorImpl<StreamBasedEventStorageParametersImpl>(params);
				break;
			case TRIE:
				coll = new TrieBasedEventCollectorImpl<EventPayload, StreamBasedEventStorageParametersImpl>(params,
						new EventPayload.FactoryNaiveImpl());
				break;
			default:
				break;
			}
			assert (coll != null);
			int w = getParameterAsInt(PARAM_KEY_SW_RES_SIZE);
			XSStaticXSEventStream stream = inputEventStream.getData(XSStaticXSEventStreamIOObject.class).getArtifact();
			if (getParameterAsBoolean(PARAM_KEY_IGNORE_WARMUP_EVENTS)) {
				while (eventIndex < w) {
					coll.triggerPacketHandle(stream.get(eventIndex));
					eventIndex++;
				}
			}
		}
		super.doWork();
	}

	@Override
	protected boolean shouldStop(IOContainer iterationResults) throws OperatorException {
		XSStaticXSEventStream stream = inputEventStream.getData(XSStaticXSEventStreamIOObject.class).getArtifact();
		if (eventIndex < stream.size()) {
			if (!start) {
				if (getParameterAsBoolean(RPAbstractIteratingOperatorChain.PARAMETER_SET_MACRO)) {
					super.setCurrentIteration(super.getCurrentIteration() + getParameterAsInt(PARAM_KEY_SW_RES_SIZE));
				}
				start = true;
			}
			coll.triggerPacketHandle(stream.get(eventIndex));
			eventIndex++;
			internalInputEventLog.clear(Port.CLEAR_DATA);
			internalInputEventLog.deliver(
					new XLogIOObject(coll.getCurrentResult(), RapidProMGlobalContext.instance().getPluginContext()));
			return false;
		} else {
			resetOperator();
			return true;
		}
	}

	private void resetOperator() {
		eventIndex = -1;
		coll = null;
		start = false;
	}

	@Override
	public void processFinished() throws OperatorException {
		super.processFinished();
		resetOperator();
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();

		ParameterTypeCategory storageStrat = new ParameterTypeCategory(PARAM_KEY_STORAGE_TECHNOLOGY,
				PARAM_DESC_STORAGE_TECHNOLOGY, ObjectUtils.toString(PARAM_VALUES_STORAGE_TECHNOLOGY), 0, false);
		params.add(storageStrat);

		ParameterTypeInt swResSize = new ParameterTypeInt(PARAM_KEY_SW_RES_SIZE, PARAM_DESC_SW_RES_SIZE, 0,
				Integer.MAX_VALUE, PARAM_DEFAULT_VAL_SW_RES_SIZE, false);
		params.add(swResSize);

		ParameterTypeBoolean ignoreTrainingCases = new ParameterTypeBoolean(PARAM_KEY_IGNORE_WARMUP_EVENTS,
				PARAM_DESC_IGNORE_WARMUP_EVENTS, PARAM_DEFAULT_IGNORE_WARMUP_EVENTS, true);
		params.add(ignoreTrainingCases);

		return params;
	}

}
