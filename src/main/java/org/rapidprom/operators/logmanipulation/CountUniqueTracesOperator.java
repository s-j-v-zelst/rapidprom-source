package org.rapidprom.operators.logmanipulation;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.rapidprom.operators.abstr.AbstractRapidProMDiscoveryOperator;

import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.ExampleSetFactory;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.tools.LogService;

public class CountUniqueTracesOperator extends AbstractRapidProMDiscoveryOperator {

	private OutputPort output = getOutputPorts().createPort("example set");

	public CountUniqueTracesOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, ExampleSet.class));
	}

	public void doWork() throws OperatorException {

		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: counting unique traces");
		long time = System.currentTimeMillis();

		XLog log = getXLog();

		Object[][] outputString = new Object[1][2];
		outputString[0][0] = "Number of Unique Traces (CAses) in the Event Log";

		Set<XTrace> traceSet = new TreeSet<XTrace>(new TraceComparator(getXEventClassifier()));
		traceSet.addAll(log);
		
		outputString[0][1] = Integer.toString(traceSet.size());

		ExampleSet es = ExampleSetFactory.createExampleSet(outputString);

		output.deliver(es);

		logger.log(Level.INFO, "End: counting unique traces (" + (System.currentTimeMillis() - time) / 1000 + " sec)");

	}

	private class TraceComparator implements Comparator<XTrace> {

		XEventClassifier classifier;

		private TraceComparator(XEventClassifier classifier) {
			this.classifier = classifier;
		}

		@Override
		public int compare(XTrace o1, XTrace o2) {
			Iterator<XEvent> it1 = o1.iterator();
			Iterator<XEvent> it2 = o2.iterator();

			while (it1.hasNext() && it2.hasNext()) {
				String ev1Name = classifier.getClassIdentity(it1.next());
				String ev2Name = classifier.getClassIdentity(it2.next());
				if (ev1Name != ev2Name)
					return ev1Name.compareTo(ev2Name);
			}
			if (it1.hasNext())
				return 1;
			if (it2.hasNext())
				return -1;
			return 0;
		}
	}
}
