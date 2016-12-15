package org.rapidprom.operators.analysis;

import java.awt.Dialog.ModalityType;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.swing.JComponent;

import org.deckfour.xes.classification.XEventNameClassifier;
import org.deckfour.xes.factory.XFactory;
import org.deckfour.xes.model.XLog;
import org.processmining.dataawareexplorer.explorer.ExplorerContext;
import org.processmining.dataawareexplorer.explorer.ExplorerInterface;
import org.processmining.dataawareexplorer.explorer.ExplorerUpdater;
import org.processmining.dataawareexplorer.explorer.NetVisualizationPanel;
import org.processmining.dataawareexplorer.explorer.events.ExplorerEvent;
import org.processmining.dataawareexplorer.explorer.exception.NetVisualizationException;
import org.processmining.dataawareexplorer.explorer.model.ExplorerModel;
import org.processmining.dataawareexplorer.explorer.model.FilterConfiguration;
import org.processmining.dataawareexplorer.explorer.netview.NetView;
import org.processmining.dataawareexplorer.explorer.netview.impl.ViewMode;
import org.processmining.datapetrinets.DataPetriNetsWithMarkings;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.logenhancement.view.LogViewContext;
import org.processmining.logenhancement.view.LogViewContextAbstract;
import org.processmining.plugins.balancedconformance.config.BalancedProcessorConfiguration;
import org.processmining.plugins.connectionfactories.logpetrinet.TransEvClassMapping;
import org.processmining.plugins.graphviz.dot.Dot;
import org.processmining.xesalignmentextension.XAlignmentExtension.XAlignedLog;
import org.processmining.xeslite.lite.factory.XFactoryLiteImpl;
import org.rapidprom.ioobjects.DataPetriNetIOObject;
import org.rapidprom.ioobjects.DotIOObject;
import org.rapidprom.ioobjects.TransEvMappingIOObject;
import org.rapidprom.ioobjects.XAlignedLogIOObject;
import org.rapidprom.ioobjects.XLogIOObject;

import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.operator.ports.metadata.SimplePrecondition;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;

import javassist.tools.rmi.ObjectNotFoundException;

/**
 * Projects an XAlignedLog on a Data Petri net
 * 
 * @author F. Mannhardt
 *
 */
public class ProjectAlignedLogOnDPNOperator extends Operator {

	private static final String PROJECTION_TYPE = "Projection type";
	private static final String PRECISION = "Precision";
	private static final String FITNESS = "Fitness";
	private static final String PERFORMANCE = "Performance";

	private static final class ExplorerContextHeadlessImpl implements ExplorerContext {

		private final ExplorerInterface userQuery;
		private final PluginContext context;

		private ExplorerContextHeadlessImpl(PluginContext context, ExplorerInterface userQuery) {
			this.context = context;
			this.userQuery = userQuery;
		}

		@Override
		public XFactory getFactory() {
			return new XFactoryLiteImpl();
		}

		@Override
		public ExecutorService getExecutor() {
			return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		}

		@Override
		public ExplorerInterface getUserQuery() {
			return userQuery;
		}

		@Override
		public LogViewContext getLogViewContext() {
			return new LogViewContextAbstract() {

				@Override
				public void exportLog(XLog log) {
				}
			};
		}

		@Override
		public PluginContext getContext() {
			return context;
		}

	}

	private static final class ExplorerInterfaceHeadlessImpl implements ExplorerInterface {
		@Override
		public void showError(String errorMessage, Exception e) {
			System.out.println(errorMessage);
			e.printStackTrace();
		}

		@Override
		public QueryResult queryYesNo(String queryTitle, JComponent queryComponent) {
			return new StandardQueryResult(ResultOption.NO);
		}

		@Override
		public QueryResult queryOkCancel(String queryTitle, JComponent queryComponent) {
			return new StandardQueryResult(ResultOption.OK);
		}

		@Override
		public QueryResult queryCustom(String queryTitle, JComponent queryComponent, String[] options) {
			return new CustomQueryResult(0); // first option is confirm
		}
		
		@Override
		public void showCustom(JComponent component, String dialogTitle, ModalityType modalityType) {
			//noop
		}

		@Override
		public void showWarning(String warningMessage, String warningTitle) {
			System.out.println(warningMessage);
		}

		@Override
		public void showMessage(String message, String title) {
			System.out.println(message);
		}

		@Override
		public void showError(String errorMessage, String errorTitle, Exception e) {
			System.out.println(errorMessage);
			e.printStackTrace();
		}

		@Override
		public String queryString(String query, String initialLabel) {
			return "";
		}

	}

	private static final class ExplorerUpdaterNoOpImpl implements ExplorerUpdater {

		private EventBus eventBus = new EventBus("explorer");

		@Override
		public EventBus getEventBus() {
			return eventBus;
		}

		@Override
		public void post(ExplorerEvent event) {
			eventBus.post(event);
		}

	}

	private InputPort inputLog = getInputPorts().createPort("event log (ProM Event Log)", XLogIOObject.class);
	private InputPort inputAlignedLog = getInputPorts().createPort("aligned log (ProM Aligned Log)",
			XAlignedLogIOObject.class);
	private InputPort inputModel = getInputPorts().createPort("model (ProM Data petri net)",
			DataPetriNetIOObject.class);

	private InputPort inputTransitionMapping = getInputPorts()
			.createPort("mapping (ProM Transition/Event Class Mapping)");

	private OutputPort outputProjection = getOutputPorts().createPort("projection (ProM Dot Graph)");

	public ProjectAlignedLogOnDPNOperator(OperatorDescription description) {
		super(description);
		inputTransitionMapping.addPrecondition(
				new SimplePrecondition(inputTransitionMapping, new MetaData(TransEvMappingIOObject.class), false));
		getTransformer().addRule(new GenerateNewMDRule(outputProjection, DotIOObject.class));
	}

	@Override
	public void doWork() throws OperatorException {

		XLogIOObject logIO = inputLog.getData(XLogIOObject.class);
		XAlignedLogIOObject alignedLogIO = inputAlignedLog.getData(XAlignedLogIOObject.class);
		DataPetriNetIOObject dpnIO = inputModel.getData(DataPetriNetIOObject.class);

		try {

			XLog log = logIO.getArtifact();
			XAlignedLog alignedLog = alignedLogIO.getAsAlignedLog();
			
			if (!(dpnIO.getArtifact() instanceof DataPetriNetsWithMarkings)) {
				throw new OperatorException("Missing markings of the Data Petri net");
			}
			
			DataPetriNetsWithMarkings dpn = (DataPetriNetsWithMarkings) dpnIO.getArtifact();
			try {
				dpn.setInitialMarking(dpnIO.getInitialMarking());
				dpn.setFinalMarkings(dpnIO.getFinalMarkingAsArray());
			} catch (ObjectNotFoundException e) {
				throw new OperatorException("Missing marking", e);
			}

			PluginContext pluginContext = dpnIO.getPluginContext();

			ExplorerUpdater updatableExplorer = new ExplorerUpdaterNoOpImpl();
			ExplorerInterface userQuery = new ExplorerInterfaceHeadlessImpl();
			ExplorerContext explorerContext = new ExplorerContextHeadlessImpl(pluginContext, userQuery);

			ViewMode mode = ViewMode.PERFORMANCE;
			String type = getParameter(PROJECTION_TYPE);
			switch (type) {
				case PERFORMANCE:
					mode = ViewMode.PERFORMANCE;
					break;
				case FITNESS:
					mode = ViewMode.FITNESS;
					break;
				case PRECISION:
					mode = ViewMode.PRECISION;
					break;
				default:
					break;
			}

			final ExplorerModel explorerModel = new ExplorerModel(log, dpn);
			BalancedProcessorConfiguration fakeConfig = new BalancedProcessorConfiguration();
			if (inputTransitionMapping.isConnected()) {
				TransEvClassMapping mapping = inputTransitionMapping.getData(TransEvMappingIOObject.class)
						.getArtifact();
				explorerModel.setEventClassifier(mapping.getEventClassifier());
				fakeConfig.setActivityMapping(mapping);
			} else {
				// TODO read from aligned log??
				explorerModel.setEventClassifier(new XEventNameClassifier());
			}
			fakeConfig.setInitialMarking(dpn.getInitialMarking());
			fakeConfig.setFinalMarkings(dpn.getFinalMarkings());
			//TODO read from aligned log
			fakeConfig.setVariableMapping(ImmutableMap.<String, String> of());
			explorerModel.setAlignmentConfiguration(fakeConfig);
			explorerModel.setAlignment(alignedLog);
			explorerModel.setFilterConfiguration(new FilterConfiguration());
			explorerModel.filter();

			NetView netView = mode.getViewFactory().newInstance(explorerContext, updatableExplorer, explorerModel);
			netView.updateData();
			// no need to call netView.updateUI();

			NetVisualizationPanel visualization = new NetVisualizationPanel(updatableExplorer, explorerModel);
			visualization.updateData(netView.getModelDecorationData());
			Dot dot = visualization.getDpnAsDot().getDot();

			outputProjection.deliver(new DotIOObject(dot, dpnIO.getPluginContext()));

		} catch (NetVisualizationException e) {
			throw new OperatorException("Failed to create visualization " + e.getMessage(), e);
		}

	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeCategory(PROJECTION_TYPE, "", new String[] { PERFORMANCE, FITNESS, PRECISION }, 0));
		return params;
	}

}