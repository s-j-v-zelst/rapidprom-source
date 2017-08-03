package org.rapidprom.operators.logmanipulation;

import java.util.Collections;
import java.util.List;
import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.factory.XFactoryRegistry;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeLiteral;
import org.deckfour.xes.model.XAttributeMap;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.util.XAttributeUtils;
import org.processmining.xeslite.query.AttributeTypeResolver;
import org.processmining.xeslite.query.XIndex;
import org.processmining.xeslite.query.XIndexedEvents;
import org.processmining.xeslite.query.syntax.ParseException;
import org.rapidprom.ioobjects.XLogIOObject;

import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.QueryFactory;
import com.googlecode.cqengine.query.option.DeduplicationStrategy;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.SimpleProcessSetupError;
import com.rapidminer.operator.ProcessSetupError.Severity;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeString;

public class FilterEventsOperator extends Operator {

	public static final class AttributeTypeResolverNaiveImpl implements AttributeTypeResolver {

		private final Iterable<XTrace> traces;

		private AttributeTypeResolverNaiveImpl(Iterable<XTrace> traces) {
			this.traces = traces;
		}

		@Override
		public Class<?> getAttributeType(String attributeName) {
			// simply return the first class found
			for (XTrace t : traces) {
				for (XEvent e : t) {
					XAttribute attribute = e.getAttributes().get(attributeName);
					if (attribute != null) {
						return XAttributeUtils.getType(attribute);
					}
				}
			}
			return XAttributeLiteral.class;
		}

	}

	private static final String PARAMETER_FILTER_QUERY_KEY = "Filter query",
			PARAMETER_FILTER_QUERY_DESCR = "<HTML>Query query that determined which events are kept."
					+ "<h1>Query Syntax</h1>"
					+ "SQL-like filtering by event names (event occurrence) or by attributes (attribute with specified value present in event)"
					+ "<h2>Examples</h2>" + "<ul>"
					+ "<li>'A' - searches for events that contain event with exact name 'A'</li>"
					+ "<li>'\"event A\"' - searches for events with exact name 'event A'</li>"
					+ "<li>'%A' - searches for events whose name contains 'A'</li>"
					+ "<li>'~.*A.*' - searches for events whose name matches the regex '.*A.*'</li>"
					+ "<li>'amount>50' - searches for events with numeric attribute 'amount' which is greater than 50</li>"
					+ "<li>'name%joe' - searches for events with literal attribute 'name' which contains the value 'joe'</li>"
					+ "</ul>" + "<h2>Details</h2>" + "<ul>"
					+ "<li>Either searches for 'concept:name' attributes of traces and events (start with '~' for use a java regular expression, start with '%' to use a 'contains' query)</li>"
					+ "<li>Supports filtering by event attributes in form of 'eventName'.'attributeName OP attributeValue'.</li>"
					+ "<li>Supported operators (OP) are (=, >, <, !=, >=, <=, % (contains), ~ (java regex), some operators only work with numeric/date attributes.</li>"
					+ "<li>Terms can be connected with (AND, OR) and nested with parens.</li>" + "</ul></HTML>",
			PARAMETER_FILTER_KEEP_EVENTS_KEY = "Keep events",
			PARAMETER_FILTER_KEEP_EVENTS_DESC = "Keep events that match the filter query.",
			PARAMETER_FILTER_KEEP_EMPTY_TRACES_KEY = "Keep empty traces",
			PARAMETER_FILTER_KEEP_EMPTY_TRACES_DESC = "Keep traces without events.";

	private InputPort inputLog = getInputPorts().createPort("event log (ProM Event Log)", XLogIOObject.class);
	private OutputPort outputLog = getOutputPorts().createPort("event log (ProM Event Log)");

	public FilterEventsOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputLog, XLogIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {
		MetaData md = inputLog.getMetaData();
		XLogIOObject logIO = inputLog.getData(XLogIOObject.class);
		XLog log = logIO.getArtifact();

		try {
			XLog filteredLog = filterEvents(log, getParameterAsString(PARAMETER_FILTER_QUERY_KEY));
			XLogIOObject xLogIOObject = new XLogIOObject(filteredLog, logIO.getPluginContext());
			outputLog.deliverMD(md);
			outputLog.deliver(xLogIOObject);
		} catch (ParseException e) {
			addError(new SimpleProcessSetupError(Severity.ERROR, getPortOwner(), "Could not parse filter query",
					e.getMessage()));
		}
	}

	private XLog filterEvents(final XLog log, String searchQuery) throws ParseException {
		XIndexedEvents indexedEvents = XIndex.newEvents(Collections.<XEvent> emptyList());
		Query<XEvent> eventQuery = indexedEvents.parseQuery(searchQuery, new AttributeTypeResolverNaiveImpl(log));

		XFactory factory = XFactoryRegistry.instance().currentDefault();
		XLog filteredLog = factory.createLog((XAttributeMap) log.getAttributes().clone());

		boolean keepFiltered = getParameterAsBoolean(PARAMETER_FILTER_KEEP_EVENTS_KEY);
		boolean keepEmptyTraces = getParameterAsBoolean(PARAMETER_FILTER_KEEP_EMPTY_TRACES_KEY);

		for (XTrace trace : log) {
			XTrace filteredTrace = factory.createTrace((XAttributeMap) trace.getAttributes().clone());
			for (XEvent event : trace) {
				boolean matches = eventQuery.matches(event,
						QueryFactory.queryOptions(QueryFactory.deduplicate(DeduplicationStrategy.LOGICAL_ELIMINATION)));
				if ((keepFiltered && matches) || (!keepFiltered && !matches)) {
					filteredTrace.add((XEvent) event.clone());
				}
			}
			if (!filteredTrace.isEmpty() || keepEmptyTraces) {
				filteredLog.add(filteredTrace);
			}
		}
		return filteredLog;
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		parameterTypes.add(new ParameterTypeString(PARAMETER_FILTER_QUERY_KEY, PARAMETER_FILTER_QUERY_DESCR, ""));
		parameterTypes.add(
				new ParameterTypeBoolean(PARAMETER_FILTER_KEEP_EVENTS_KEY, PARAMETER_FILTER_KEEP_EVENTS_DESC, true));
		parameterTypes.add(new ParameterTypeBoolean(PARAMETER_FILTER_KEEP_EMPTY_TRACES_KEY,
				PARAMETER_FILTER_KEEP_EMPTY_TRACES_DESC, true));

		return parameterTypes;
	}

}