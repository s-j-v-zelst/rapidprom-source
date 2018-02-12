package org.rapidprom.operators.logmanipulation;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.extension.std.XTimeExtension;
import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryNaiveImpl;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.rapidprom.ioobjects.XLogIOObject;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.tools.LogService;

public class AddNoiseOperator extends Operator {

	private static final String PARAMETER_1_KEY = "Noise Percentage",
			PARAMETER_1_DESCR = "The probabilitiy that, for any given trace, noise will be added to it.",
			PARAMETER_2_KEY = "Noise Type",
			PARAMETER_2_DESCR = "There are 5 possible noise types: remove head, remove body, swap tasks, "
					+ "remove task, and add task. The noise types \"remove head\" and \"remove body\" "
					+ "respectively remove at most the first or second 1/3 of a trace. "
					+ "The noise type swap randomly swaps two event in a trace. "
					+ "The noise type remove randomly removes event from a trace. "
					+ "The add type randomly adds an event to a trace. "
					+ "Also a random noise option is provided. This option randomly chooses between add, remove or swap task.",

			PARAMETER_3_KEY = "Seed", PARAMETER_3_DESCR = "This parameter defines the seed used to evaluate noise "
					+ "probability and apply the noise type.",

			PARAMETER_4_KEY = "Event Percentage",
			PARAMETER_4_DESCR = "The percentage of events in a trace that noise will be applied to. Negative values are used as a special value, which indicates that just one event is changed.";

	private static final String HEAD = "Remove Head", BODY = "Remove Body", EXTRA = "Add Event", SWAP = "Swap Tasks",
			SWAP_TIME = "Swap Tasks & Time", REMOVE = "Remove Task", RANDOM = "Random (add/remove/swap task)";

	private InputPort inputXLog = getInputPorts().createPort("event log (ProM Event Log)", XLogIOObject.class);
	private OutputPort outputEventLog = getOutputPorts().createPort("event log (ProM Event Log)");

	public AddNoiseOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputEventLog, XLogIOObject.class));
	}

	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: add noise");
		long time = System.currentTimeMillis();

		MetaData md = inputXLog.getMetaData();

		XLogIOObject xLogIOObject = inputXLog.getData(XLogIOObject.class);
		XLog logOriginal = xLogIOObject.getArtifact();
		XLogIOObject result = new XLogIOObject(filterLog(logOriginal), xLogIOObject.getPluginContext());

		outputEventLog.deliverMD(md);
		outputEventLog.deliver(result);
		logger.log(Level.INFO, "End: add noise (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	private XLog filterLog(XLog log) throws UndefinedParameterError {

		XFactory factory = XFactoryRegistry.instance().currentDefault();
		XLog result = factory.createLog(log.getAttributes());
		result.getClassifiers().addAll(log.getClassifiers());

		int traceCounter = 0;
		Random rOverall = new Random(getParameterAsInt(PARAMETER_3_KEY));
		for (XTrace t : log) {
			XTrace copy = factory.createTrace(t.getAttributes());
			Random noiseRandom = new Random(getParameterAsInt(PARAMETER_3_KEY) + new Integer(traceCounter).hashCode());
			double nextDouble = rOverall.nextDouble();
			double eventPercentage = getParameterAsDouble(PARAMETER_4_KEY);
			// System.out.println("nextDouble:" + nextDouble);
			if (nextDouble < getParameterAsDouble(PARAMETER_1_KEY) && eventPercentage != 0) {				
				if (eventPercentage > 0) {
					int eventsToModify = (int) Math.ceil(t.size() * eventPercentage);
					for (int k = 0; k < eventsToModify; k++) {
						applyNoise(log, factory, t, copy, noiseRandom, getParameterAsString(PARAMETER_2_KEY));
						t = copy;
						if (k < eventsToModify - 1) {
							copy = factory.createTrace(t.getAttributes());
						}
					}
				} else {
					applyNoise(log, factory, t, copy, noiseRandom, getParameterAsString(PARAMETER_2_KEY));
				}
			} else {
				for (XEvent e : t) {
					XEvent copyEvent = factory.createEvent(e.getAttributes());
					copy.add(copyEvent);
				}
			}
			traceCounter++;
			result.add(copy);
		}
		return result;
	}

	private void applyNoise(XLog log, XFactory factory, XTrace t, XTrace copy, Random noiseRandom, String noiseType)
			throws UndefinedParameterError {
		double oneThird = t.size() / 3.0;

		if(noiseType.equals(RANDOM)){
			switch(safeNextInt(noiseRandom, 3)){
			case 0: 
				noiseType = EXTRA;
				break;
			case 1: 
				noiseType = SWAP;
				break;
			case 2:
			default:
				noiseType = REMOVE;
				break;
			}
		}
		
		if (noiseType.equals(HEAD)) {
			int start = safeNextInt(noiseRandom, (int) oneThird);
			for (int i = start; i < t.size(); i++) {
				XEvent e = t.get(i);
				XEvent copyEvent = factory.createEvent(e.getAttributes());
				copy.add(copyEvent);
			}
		} else if (noiseType.equals(BODY)) {
			int stopFirst = safeNextInt(noiseRandom, (int) oneThird);
			for (int i = 0; i < stopFirst; i++) {
				XEvent e = t.get(i);
				XEvent copyEvent = factory.createEvent(e.getAttributes());
				copy.add(copyEvent);
			}

			int startLast = t.size() - safeNextInt(noiseRandom, (int) oneThird);
			for (int i = startLast; i < t.size(); i++) {
				XEvent e = t.get(i);
				XEvent copyEvent = factory.createEvent(e.getAttributes());
				copy.add(copyEvent);
			}
		} else if (noiseType.equals(EXTRA)) {
			for (XEvent e : t) {
				XEvent copyEvent = factory.createEvent(e.getAttributes());
				copy.add(copyEvent);
			}
			// add event
			int pos = safeNextInt(noiseRandom, t.size());

			// System.out.println("Pos: " + pos);

			// get the previous event to check for timestamp
			Date lowb = (pos != 0) ? XTimeExtension.instance().extractTimestamp(copy.get(pos - 1)) : null;
			Date upb = (pos != t.size()) ? XTimeExtension.instance().extractTimestamp(copy.get(pos)) : null;

			// if(lowb!= null)
			// System.out.println("Low: " + lowb.toString());
			//
			// if(upb!=null)
			// System.out.println("Up: " + upb.toString());

			if ((lowb != null) && (upb != null)) {
				// the new event has timestamp in between
				copy.add(pos, createEvent(log, log.size(), noiseRandom, new Date((upb.getTime() + lowb.getTime()) / 2),
						XTimeExtension.instance()));
			} else if (lowb != null) {
				// there is a lower bound
				copy.add(pos, createEvent(log, log.size(), noiseRandom, new Date(lowb.getTime() + 1),
						XTimeExtension.instance()));
			} else if (upb != null) {
				// there is an upper bound
				copy.add(pos, createEvent(log, log.size(), noiseRandom, new Date(upb.getTime() - 1),
						XTimeExtension.instance()));
			} else {
				// there is neither a lower or an upper bound
				copy.add(pos, createEvent(log, log.size(), noiseRandom, null, XTimeExtension.instance()));
			}
		} else if (noiseType.equals(SWAP)) {
			int indexFirstTaskToSwap = safeNextInt(noiseRandom, t.size());
			int indexSecondTaskToSwap = safeNextInt(noiseRandom, t.size());
			XEvent firstTaskToSwap = null;
			XEvent secondTaskToSwap = null;
			XEvent event = null;
			if (indexFirstTaskToSwap != indexSecondTaskToSwap) {
				// it makes sense to swap
				firstTaskToSwap = t.get(indexSecondTaskToSwap);
				secondTaskToSwap = t.get(indexFirstTaskToSwap);
				// swap also the timestamps
				// Date firstTimestamp = XTimeExtension.instance()
				// .extractTimestamp(firstTaskToSwap);
				// Date secondTimestamp = XTimeExtension.instance()
				// .extractTimestamp(secondTaskToSwap);

				for (int i = 0; i < t.size(); i++) {
					if (i == indexFirstTaskToSwap) {
						event = (XEvent) firstTaskToSwap.clone();
						// XTimeExtension.instance().assignTimestamp(event,
						// secondTimestamp);
					} else if (i == indexSecondTaskToSwap) {
						event = (XEvent) secondTaskToSwap.clone();
						// XTimeExtension.instance().assignTimestamp(event,
						// firstTimestamp);
					} else {
						event = t.get(i);
					}
					XEvent copyEvent = factory.createEvent((XAttributeMap) event.getAttributes().clone());
					copy.add(copyEvent);

				}
			} else {
				// we still need to copy
				for (XEvent e : t) {
					XEvent copyEvent = factory.createEvent((XAttributeMap) e.getAttributes().clone());
					copy.add(copyEvent);
				}
			}
		} else {
			// remove an event
			int pos = Math.abs(noiseRandom.nextInt()) % (t.size() + 1);
			for (int i = 0; i < t.size(); i++) {
				if (i != pos) {
					XEvent event = t.get(i);
					XEvent copyEvent = factory.createEvent(event.getAttributes());
					copy.add(copyEvent);
				}
			}
		}
	}

	private int safeNextInt(Random r, int maxInt) {
		return r.nextInt(maxInt > 0 ? maxInt : 1);
	}

	protected XEvent createEvent(XLog log, int logSize, Random rand, Date date, XTimeExtension xTime) {
		// both date are null
		XTrace tr = log.get(Math.abs(rand.nextInt()) % logSize);
		int pos = safeNextInt(rand, tr.size());

		if (pos == 0 && pos < tr.size() - 1) // so it does not create "start"
												// events
			pos++;

		XFactory factory = new XFactoryNaiveImpl();
		XEvent newEvt = factory.createEvent((XAttributeMap) tr.get(pos).getAttributes().clone());
		if (date != null) {
			xTime.assignTimestamp(newEvt, date);
		}
		return newEvt;
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterTypeDouble parameterType1 = new ParameterTypeDouble(PARAMETER_1_KEY, PARAMETER_1_DESCR, 0, 1, 0.05);
		parameterTypes.add(parameterType1);

		String[] par2categories = new String[] { REMOVE, HEAD, BODY, EXTRA, SWAP, SWAP_TIME, RANDOM };
		ParameterTypeCategory parameterType2 = new ParameterTypeCategory(PARAMETER_2_KEY, PARAMETER_2_DESCR,
				par2categories, 0);
		parameterTypes.add(parameterType2);

		ParameterTypeInt parameterType3 = new ParameterTypeInt(PARAMETER_3_KEY, PARAMETER_3_DESCR, 0, Integer.MAX_VALUE,
				1);
		parameterTypes.add(parameterType3);

		ParameterTypeDouble parameterType4 = new ParameterTypeDouble(PARAMETER_4_KEY, PARAMETER_4_DESCR, -1, 1, -1);
		parameterTypes.add(parameterType4);
		return parameterTypes;
	}

}
