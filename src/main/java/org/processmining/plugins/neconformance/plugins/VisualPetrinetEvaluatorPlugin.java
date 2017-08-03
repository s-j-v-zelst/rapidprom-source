package org.processmining.plugins.neconformance.plugins;


import java.util.ArrayList;
import java.util.List;

import javax.swing.JComponent;

import org.deckfour.uitopia.api.event.TaskListener.InteractionResult;
import org.deckfour.xes.classification.XEventClass;
import org.deckfour.xes.classification.XEventClasses;
import org.deckfour.xes.info.impl.XLogInfoImpl;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.processmining.contexts.uitopia.UIPluginContext;
import org.processmining.contexts.uitopia.annotations.UITopiaVariant;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.models.semantics.petrinet.Marking;
import org.processmining.plugins.kutoolbox.logmappers.PetrinetLogMapper;
import org.processmining.plugins.kutoolbox.logmappers.PetrinetLogMapperPanel;
import org.processmining.plugins.neconformance.models.ProcessReplayModel;
import org.processmining.plugins.neconformance.models.impl.PetrinetReplayModel;
import org.processmining.plugins.neconformance.negativeevents.AbstractNegativeEventInducer;
import org.processmining.plugins.neconformance.negativeevents.impl.LogTreeWeightedNegativeEventInducer;
import org.processmining.plugins.neconformance.trees.LogTree;
import org.processmining.plugins.neconformance.trees.ukkonen.UkkonenLogTree;
import org.processmining.plugins.neconformance.ui.EvaluationVisualizator;

public class VisualPetrinetEvaluatorPlugin {
	@Plugin(name = "Evaluate Behavioral (Weighted) Conformance Log on Petri Net", 
			parameterLabels = { "Log", "Petri net", "Marking" }, 
			returnLabels = { "Evaluation Result" }, 
			returnTypes = { JComponent.class }, 
			help = "Evaluate a log on petri net (fitness, conformance)")
	@UITopiaVariant(affiliation = "KU Leuven", 
		author = "Seppe vanden Broucke", 
		email = "seppe.vandenbroucke@econ.kuleuven.be", 
		website = "http://econ.kuleuven.be")
	
	public static JComponent main(UIPluginContext context, XLog log, Petrinet onet, Marking marking) {
		PetrinetLogMapper mapper;
		
		PetrinetLogMapperPanel mapperPanel = new PetrinetLogMapperPanel(log, onet);
		InteractionResult ir = context.showWizard("Mapping", true, true, mapperPanel);
		if (!ir.equals(InteractionResult.FINISHED)) {
			context.getFutureResult(0).cancel(true);
			return null;
		}
		mapper = mapperPanel.getMap();
		mapper.applyMappingOnTransitions();
		
		context.log("Making log tree and negative event inducer");
		
		LogTree logTree = new UkkonenLogTree(log);
		XEventClasses eventClasses = XEventClasses.deriveEventClasses(XLogInfoImpl.STANDARD_CLASSIFIER, log);
		LogTreeWeightedNegativeEventInducer inducer = new LogTreeWeightedNegativeEventInducer(
				eventClasses,
				AbstractNegativeEventInducer.deriveStartingClasses(eventClasses, log),
				logTree);
		inducer.setReturnZeroEvents(false);
		inducer.setUseBothWindowRatios(false);
		inducer.setUseWeighted(true);
		
		context.log("Making replay models...");
		List<ProcessReplayModel<Transition, XEventClass, Marking>> replayModels = 
				new ArrayList<ProcessReplayModel<Transition, XEventClass, Marking>>();
		for (int t = 0; t < log.size(); t++) {
			if (t % 10 == 0)
				context.log(" "+t+" / "+log.size());
			XTrace trace = log.get(t);
			ProcessReplayModel<Transition, XEventClass, Marking> replayModel = new PetrinetReplayModel(onet, marking, mapper);
			replayModel.reset();
			List<XEventClass> classSequence = getTraceAsClassSequence(trace, inducer);
			replayModel.replay(classSequence);
			replayModels.add(replayModel);
			
		}	
		
		return new EvaluationVisualizator(replayModels, inducer, log, onet, marking, mapper);
		
	}
	
	private static List<XEventClass> getTraceAsClassSequence(XTrace trace, AbstractNegativeEventInducer inducer) {
		List<XEventClass> sequence = new ArrayList<XEventClass>();
		for (XEvent event : trace)
			sequence.add(inducer.getClassAlphabet().getClassOf(event));
		return sequence;
	}

}
