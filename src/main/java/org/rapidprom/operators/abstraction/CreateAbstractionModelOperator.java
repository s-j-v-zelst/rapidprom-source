package org.rapidprom.operators.abstraction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.lang.model.SourceVersion;

import org.processmining.datapetrinets.DataPetriNet;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.logenhancement.abstraction.PatternBasedLogAbstractionPlugin;
import org.processmining.logenhancement.abstraction.model.AbstractionModel;
import org.processmining.logenhancement.abstraction.model.AbstractionPattern;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionVisitorException;
import org.processmining.logenhancement.abstraction.model.syntax.ParseException;
import org.processmining.models.graphbased.directed.petrinet.PetrinetGraph;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.AbstractionModelIOObject;
import org.rapidprom.ioobjects.DataPetriNetIOObject;

import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ResultObjectAdapter;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPortExtender;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.UndefinedParameterError;

/**
 * Create an abstraction model (DPN-based) from multiple process patterns.
 * 
 * @author F. Mannhardt
 *
 */
public class CreateAbstractionModelOperator extends Operator {

	private static final String INTERLEAVING = "Interleaving (%)";
	private static final String PARALLEL = "Parallel (#)";

	private final static class AbstractionCodeResult extends ResultObjectAdapter {
		private static final long serialVersionUID = 5985457282363620702L;

		private final String abstractionModelCode;

		public AbstractionCodeResult(String abstractionModelCode) {
			this.abstractionModelCode = abstractionModelCode;
		}

		@Override
		public String toString() {
			return abstractionModelCode;
		}
	}

	private static final String DEFAULT_COMPOSITION_KEY = "Default Composition",
			DEFAULT_COMPOSITION_DESCR = "Speciy how activity patterns are composed by default.",
			ABSTRACTION_MODEL_KEY = "Abstraction Model",
			ABSTRACTION_MODEL_DESCR = "<HTML>Specify how activity patterns are composed to an integrated abstraction model. "
					+ "If left empty all patterns are composed with the default composition (parallel or interleaving), e.g, for patterns A,B,C, the abstraction model is A*#B*#C*."
					+ "<h1>Model Syntax</h1>" + "Initially all abstraction patterns are composed in parallel. "
					+ "The composition can be changed by manipulating the formula or by using the context menu (right-click) of the model."
					+ "Operators can be nested, patterns can appear multiple times and parentheses can be used to indicate preceedence if necessary."
					+ "<h2>Examples for patterns A and B</h2>" + "<ul>"
					+ "<li>'A#B' - A and B can happen in parallel</li>" 
					+ "<li>'A|B' - Either A or B can happen</li>"
					+ "<li>'A<B' - A must happen before B</li>" 
					+ "<li>'%[A,B]' - A and B can interleave</li>"
					+ "<li>'A?' - A can happen 1 or 0 times</li>" + "<li>'A+' - A can happen 1 or multiple times</li>"
					+ "<li>'A*' - A can happen 0 or multiple times</li>"
					+ "<li>'A{n,m}' - A can happen n to m times</li>" + "</ul>" + "<h2>Full Syntax</h2>" + "<pre>"
					+ "parse		:=	expression <EOF>\r\n" + "expression	:=	parallel ( ( \"#\" parallel ) )*\r\n"
					+ "parallel	:=	choice ( ( \"|\" choice ) )*\r\n"
					+ "choice		:=	sequence ( ( \"<\" sequence ) )*\r\n"
					+ "sequence	:=	( \"%[\" nary ( \",\" nary )+ \"]\" )\r\n"
					+ "		|	( \"|[\" nary ( \",\" nary )+ \"]\" )\r\n"
					+ "		|	( \"#[\" nary ( \",\" nary )+ \"]\" )\r\n" + "		|	nary\r\n"
					+ "nary		:=	basic ( ( \"*\" ) | ( \"+\" ) | ( \"?\" ) | ( \"{\" <INTEGER> \",\" ( <INTEGER> | \"*\" ) \"}\" ) )?\r\n"
					+ "basic		:=	( \"(\" expression \")\" )\r\n" + "		|	identifier\r\n"
					+ "identifier	:=	( <IDENTIFIER> | <STRING_LITERAL> )" + "</pre></HTML>";

	private final InputPortExtender inExtender = new InputPortExtender("patterns (Data Petri nets)", getInputPorts(),
			new MetaData(DataPetriNetIOObject.class), true);

	private OutputPort outputModel = getOutputPorts().createPort("abstraction model (ProM Abstraction Model)");
	private OutputPort outputCode = getOutputPorts().createPort("abstraction code (Abstraction Code)");

	public CreateAbstractionModelOperator(OperatorDescription description) {
		super(description);
		inExtender.start();
		getTransformer().addRule(new GenerateNewMDRule(outputModel, AbstractionModelIOObject.class));
		getTransformer().addRule(new GenerateNewMDRule(outputCode, ResultObjectAdapter.class));
	}

	@Override
	public void doWork() throws OperatorException {

		DataPetriNet[] patterns = getPatterns();
		Map<String, AbstractionPattern> patternMap = createPatternMap(patterns);

		try {
			String abstractionModelCode = getAbstractionModelCode(patternMap);

			AbstractionModel abstractionModel = PatternBasedLogAbstractionPlugin.composePatterns(abstractionModelCode,
					patternMap, true);

			outputModel.deliver(new AbstractionModelIOObject(abstractionModel, getContext()));
			outputCode.deliver(new AbstractionCodeResult(abstractionModelCode));

		} catch (final ParseException | CompositionVisitorException e) {
			throw new OperatorException("Failed to build abstaction model!", e);			
		}
	}

	private Map<String, AbstractionPattern> createPatternMap(DataPetriNet[] patterns) throws OperatorException {
		Map<DataPetriNet, String> patternNames = new HashMap<>();
		for (DataPetriNet net : patterns) {
			if (!patternNames.containsKey(net)) {
				patternNames.put(net, net.getLabel());
			} else {
				throw new OperatorException("Invalid input, two identical patterns with the name " + net.getLabel());
			}
		}
		return PatternBasedLogAbstractionPlugin.buildPatterns(patternNames);
	}

	private String getAbstractionModelCode(Map<String, AbstractionPattern> abstractionPatterns)
			throws UndefinedParameterError {
		String modelAsString = getParameterAsString(ABSTRACTION_MODEL_KEY);
		if (modelAsString != null && !modelAsString.isEmpty()) {
			return modelAsString;
		} else {
			if (abstractionPatterns.size() == 1) {
				String id = abstractionPatterns.keySet().iterator().next();
				if (!SourceVersion.isIdentifier(id)) {
					id = "\"" + id + "\"";
				}
				return id + "*";
			} else {
				String defaultComposition = getParameter(DEFAULT_COMPOSITION_KEY);
				if (PARALLEL.equals(defaultComposition)) {
					return PatternBasedLogAbstractionPlugin.buildInitialModel(abstractionPatterns);				
				} else if (INTERLEAVING.equals(defaultComposition)) {
					return buildInitialModelInInterleaving(abstractionPatterns);
				} else {
					throw new IllegalArgumentException("Unknown default composition "+ defaultComposition);
				}
			}
		}
	}
	
	//TODO update in LogEnhancement
	private static String buildInitialModelInInterleaving(Map<String, AbstractionPattern> abstractionPatterns) {		
		StringBuilder sb = new StringBuilder();
		sb.append("(%[");
		for (Iterator<String> iter = abstractionPatterns.keySet().iterator(); iter.hasNext();) {
			String id = iter.next();
			if (!SourceVersion.isIdentifier(id)) {
				id = "\"" + id + "\"";
			}
			sb.append(id);
			sb.append("*");
			if (iter.hasNext()) {
				sb.append(",");
			}
		}
		sb.append("])*");
		return sb.toString();
	}

	private DataPetriNet[] getPatterns() throws UserError, OperatorException {
		Collection<PetrinetGraph> patterns = new ArrayList<>();
		@SuppressWarnings("rawtypes")
		List<IOObjectCollection> data = inExtender.getData(IOObjectCollection.class, false);
		for (IOObjectCollection<?> list : data) {
			for (Object obj : list.getObjectsRecursive()) {
				if (obj instanceof DataPetriNetIOObject) {
					patterns.add(((DataPetriNetIOObject) obj).getArtifact());
				} else {
					throw new OperatorException("Unknown pattern type " + obj);
				}
			}
		}
		return PatternBasedLogAbstractionPlugin.transformNets(patterns.toArray(new PetrinetGraph[patterns.size()]));
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> params = super.getParameterTypes();
		params.add(new ParameterTypeString(ABSTRACTION_MODEL_KEY, ABSTRACTION_MODEL_DESCR, true, false));
		params.add(new ParameterTypeCategory(DEFAULT_COMPOSITION_KEY, DEFAULT_COMPOSITION_DESCR,
				new String[] { PARALLEL, INTERLEAVING }, 0));
		return params;
	}

	private PluginContext getContext() throws UserError {
		return RapidProMGlobalContext.instance().getPluginContext();
	}

}