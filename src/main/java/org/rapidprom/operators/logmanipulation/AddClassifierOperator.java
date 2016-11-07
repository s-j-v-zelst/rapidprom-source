package org.rapidprom.operators.logmanipulation;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.deckfour.xes.classification.XEventAndClassifier;
import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.classification.XEventLifeTransClassifier;
import org.deckfour.xes.classification.XEventNameClassifier;
import org.deckfour.xes.classification.XEventResourceClassifier;
import org.deckfour.xes.model.XLog;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.XLogIOObject;
import org.rapidprom.operators.ports.metadata.XLogIOObjectMetaData;

import com.rapidminer.operator.IOObject;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.tools.LogService;

public class AddClassifierOperator extends Operator {

	public static final String PARAMETER_1_KEY = "Classifier",
			PARAMETER_1_DESCR = "Classifier to be added to the event log";

	public static final String NONE = "None (do not add classifier)", EN = "Event name",
			EN_LT = "Event name + Lifecycle transition", EN_LT_RE = "Event name + Lifecycle transition + Resource";

	private InputPort inputXLog = getInputPorts().createPort("event log (ProM Event Log)", XLogIOObject.class);
	private OutputPort outputEventLog = getOutputPorts().createPort("event log (ProM Event Log)");

	public AddClassifierOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new XLogMetaData(outputEventLog, XLogIOObject.class));

	}

	public void doWork() throws OperatorException {

		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: add classifier");
		long time = System.currentTimeMillis();

		XLogIOObject logObject = inputXLog.getData(XLogIOObject.class);

		XLog newLog = (XLog) logObject.getArtifact().clone();

		switch (getParameterAsString(PARAMETER_1_KEY)) {
		case NONE:
			break;
		case EN:
			newLog.getClassifiers().add(new XEventNameClassifier());

			break;
		case EN_LT:
			newLog.getClassifiers()
					.add(new XEventAndClassifier(new XEventNameClassifier(), new XEventLifeTransClassifier()));
			break;
		case EN_LT_RE:
			newLog.getClassifiers().add(new XEventAndClassifier(new XEventNameClassifier(),
					new XEventLifeTransClassifier(), new XEventResourceClassifier()));
			break;
		}

		XLogIOObject result = new XLogIOObject(newLog, RapidProMGlobalContext.instance().getPluginContext());
		outputEventLog.deliver(result);
		logger.log(Level.INFO, "End: add classifier (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	public MetaData getGeneratedMetaData() throws OperatorException {
		getLogger().fine("Generating meta data for " + this.getName());
		List<XEventClassifier> classifiers = new ArrayList<XEventClassifier>();

		try {
			XLog newLog = inputXLog.getData(XLogIOObject.class).getArtifact();

			if (newLog != null && newLog.getClassifiers() != null)
				classifiers.addAll(newLog.getClassifiers());
		} catch (Exception e) {
			
		}
		try {

			switch (getParameterAsString(PARAMETER_1_KEY)) {
			case NONE:
				break;
			case EN:
				classifiers.add(new XEventNameClassifier());

				break;
			case EN_LT:
				classifiers.add(new XEventAndClassifier(new XEventNameClassifier(), new XEventLifeTransClassifier()));
				break;
			case EN_LT_RE:
				classifiers.add(new XEventAndClassifier(new XEventNameClassifier(), new XEventLifeTransClassifier(),
						new XEventResourceClassifier()));
				break;
			}

		} catch (Exception e) {
			return new XLogIOObjectMetaData();
		}
		if (!classifiers.isEmpty())
			return new XLogIOObjectMetaData(classifiers);
		else
			return new XLogIOObjectMetaData();
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		String[] par2categories = new String[] { NONE, EN, EN_LT, EN_LT_RE };
		ParameterTypeCategory parameterType2 = new ParameterTypeCategory(PARAMETER_1_KEY, PARAMETER_1_DESCR,
				par2categories, 0);
		parameterTypes.add(parameterType2);

		return parameterTypes;
	}

	class XLogMetaData extends GenerateNewMDRule {

		public XLogMetaData(OutputPort outputPort, Class<? extends IOObject> clazz) {
			super(outputPort, clazz);
		}

		@Override
		public void transformMD() {
			try {
				outputEventLog.deliverMD(getGeneratedMetaData());
			} catch (OperatorException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}
