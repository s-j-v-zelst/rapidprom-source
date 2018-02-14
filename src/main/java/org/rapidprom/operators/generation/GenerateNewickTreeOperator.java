package org.rapidprom.operators.generation;

import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.ptandloggenerator.algorithms.TreeFactory;
import org.processmining.ptandloggenerator.interfaces.randomTree;
import org.processmining.ptandloggenerator.models.NewickTree;
import org.processmining.ptandloggenerator.parameters.TreeParameters;
import org.rapidprom.external.connectors.prom.RapidProMGlobalContext;
import org.rapidprom.ioobjects.NewickTreeIOObject;

import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.parameter.conditions.EqualStringCondition;
import com.rapidminer.tools.LogService;

public class GenerateNewickTreeOperator extends Operator {

	private OutputPort output = getOutputPorts().createPort("newick tree collection");

	private final static String PARAM_KEY_MIN_ACTIVITIES = "minimal_number_of_activities";
	private final static String PARAM_DESC_MIN_ACTIVITIES = "Minimal number of activities within process tree";
	private final static int PARAM_DEFAULT_MIN_ACTIVITIES = 10;

	private final static String PARAM_KEY_MODE_ACTIVITIES = "mode_of_number_of_activities";
	private final static String PARAM_DESC_MODE_ACTIVITIES = "Mode of triangular distribution of activities present in the process tree";
	private final static int PARAM_DEFAULT_MODE_ACTIVITIES = 20;

	private final static String PARAM_KEY_MAX_ACTIVITIES = "maximal_number_of_activities";
	private final static String PARAM_DESC_MAX_ACTIVITIES = "Maximal number of activities within process tree";
	private final static int PARAM_DEFAULT_MAX_ACTIVITIES = 30;

	private final static String PARAM_KEY_COLLECTION_SIZE = "number_of_trees";
	private final static String PARAM_DESC_COLLECTION_SIZE = "Number of trees to generate";
	private final static int PARAM_DEFAULT_COLLECTION_SIZE = 10;

	private final static String PARAM_KEY_PROBABILITY_SILENT = "silent_probability";
	private final static String PARAM_DESC_PROBABILITY_SILENT = "Probability of adding silent activities.";
	private final static double PARAM_DEFAULT_PROBABILITY_SILENT = 0.2;

	private final static String PARAM_KEY_LONG_TERM = "long_term_dependency";
	private final static String PARAM_DESC_LONG_TERM = "Probability of adding long-term dependencies.";
	private final static double PARAM_DEFAULT_LONG_TERM = 0.2;

	private final static String PARAM_KEY_DUPLICATE_ACTIVITIES = "duplicate_activities";
	private final static String PARAM_DESC_DUPLICATE_ACTIVITIES = "Probability of adding duplicate activities.";
	private final static double PARAM_DEFAULT_DUPLICATE_ACTIVITIES = 0;

	private final static String PARAM_KEY_INFREQUENT_PATHS = "include_infrequent_paths";
	private final static String PARAM_DESC_INFREQUENT_PATHS = "Indicates whether or not the process tree should contain infrequent paths.";
	private final static boolean PARAM_DEFAULT_INFREQUENT_PATHS = false;

	private final static String PARAM_DEFAULT_VALUE_CONFIG_MANUAL = "Manual";
	private final static String PARAM_DEFAULT_VALUE_CONFIG_REPEATED = "Repeated Experiment";

	private final static String[] PARAM_VAL_CONFIG = new String[] { PARAM_DEFAULT_VALUE_CONFIG_MANUAL,
			PARAM_DEFAULT_VALUE_CONFIG_REPEATED };
	private final static String PARAM_KEY_CONFIG = "config";
	private final static String PARAM_DESC_CONFIG = "Indicates how the operator is used. When selecting manual the user is allowed to specify the operator distribution manually, when repeated experiment is selected only one operator probability can be changed, the other operators are updated accordingly";
	private final static int PARAM_DEFAULT_CONFIG = 0;

	private final static String PARAM_KEY_CONFIG_MANUAL_SEQ_PROB = "sequence_probability";
	private final static String PARAM_DESC_CONFIG_MANUAL_SEQ_PROB = "Probability of inserting a sequence operator.";
	private final static double PARAM_DEFAULT_CONFIG_MANUAL_SEQ_PROB = 0.45;

	private final static String PARAM_KEY_CONFIG_MANUAL_PAR_PROB = "parallel_probability";
	private final static String PARAM_DESC_CONFIG_MANUAL_PAR_PROB = "Probability of inserting a parallel operator.";
	private final static double PARAM_DEFAULT_CONFIG_MANUAL_PAR_PROB = 0.2;

	private final static String PARAM_KEY_CONFIG_MANUAL_XOR_PROB = "xor_probability";
	private final static String PARAM_DESC_CONFIG_MANUAL_XOR_PROB = "Probability of inserting a choice operator.";
	private final static double PARAM_DEFAULT_CONFIG_MANUAL_XOR_PROB = 0.2;

	private final static String PARAM_KEY_CONFIG_MANUAL_OR_PROB = "or_probability";
	private final static String PARAM_DESC_CONFIG_MANUAL_OR_PROB = "Probability of inserting an or operator.";
	private final static double PARAM_DEFAULT_CONFIG_MANUAL_OR_PROB = 0.05;

	private final static String PARAM_KEY_CONFIG_MANUAL_LOOP_PROB = "loop_probability";
	private final static String PARAM_DESC_CONFIG_MANUAL_LOOP_PROB = "Probability of inserting a loop operator.";
	private final static double PARAM_DEFAULT_CONFIG_MANUAL_LOOP_PROB = 0.1;

	private final static String PARAM_DEFAULT_VALUE_CONFIG_EXP_SEQ = "Sequence";
	private final static String PARAM_DEFAULT_VALUE_CONFIG_EXP_PAR = "Parallel";
	private final static String PARAM_DEFAULT_VALUE_CONFIG_EXP_XOR = "XOR";
	private final static String PARAM_DEFAULT_VALUE_CONFIG_EXP_OR = "OR";
	private final static String PARAM_DEFAULT_VALUE_CONFIG_EXP_LOOP = "Loop";

	private final static String PARAM_KEY_CONFIG_EXP_OPERATORS = "select_repeated_operator";
	private final static String[] PARAM_VAL_CONFIG_EXP_OPERATORS = new String[] { PARAM_DEFAULT_VALUE_CONFIG_EXP_SEQ,
			PARAM_DEFAULT_VALUE_CONFIG_EXP_PAR, PARAM_DEFAULT_VALUE_CONFIG_EXP_XOR, PARAM_DEFAULT_VALUE_CONFIG_EXP_OR,
			PARAM_DEFAULT_VALUE_CONFIG_EXP_LOOP };
	private final static String PARAM_DESC_CONFIG_EXP_OPERATORS = "Indicates which operator needs to be specified in terms of occurrence probability (only in repeated experiment mode)";
	private final static int PARAM_DEFAULT_CONFIG_EXP_OPERATORS = 0;

	private final static String PARAM_KEY_CONFIG_EXP_PROBABILITY = "selected_operator_probability";
	private final static String PARAM_DESC_CONFIG_EXP_PROBABILITY = "Probability of insertion of the specified operator. Probabilities for the other operators are updated automatically and accordingly";
	private final static double PARAM_DEFAULT_CONFIG_EXP_PROBABILITY = 0.2;

	private final static String PARAM_KEY_USE_MANUAL_DISTRIBUTION_FOR_SCALING = "use_manual_distribution_for_experimental_distribution_scaling";
	private final static String PARAM_DESC_USE_MANUAL_DISTRIBUTION_FOR_SCALING = "If selected, the manual distribution specified will be used for scaling the probability distribution used in the repeated experiment(s)";
	private final static boolean PARAM_DEFAULT_USE_MANUAL_DISTRIBUTION_FOR_SCALING = true;

	public GenerateNewickTreeOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(output, IOObjectCollection.class));
	}

	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "start: generating newick trees");
		long time = System.currentTimeMillis();

		PluginContext pluginContext = RapidProMGlobalContext.instance().getPluginContext();

		TreeFactory treeFactory = new TreeFactory();
		IOObjectCollection<NewickTreeIOObject> result = new IOObjectCollection<NewickTreeIOObject>();
		Random r = new Random(fetchSeed());
		int numTrees= getParameterAsInt(PARAM_KEY_COLLECTION_SIZE);
		for (int i = 0; i < numTrees; i++) {
			TreeParameters newickTreeParams = constructNewickTreeParameters(r.nextLong());
			NewickTree tree = new NewickTree(treeFactory.createTree(newickTreeParams.getPatternProbabilities()));
			result.add(new NewickTreeIOObject(tree, pluginContext));
			treeFactory.cleanupInterpreter();
		}

		// NewickTreeCollection collection = iterator.getNewickTrees();
		// for (NewickTree tree : collection.getNewickTreeList())
		// result.add(new NewickTreeIOObject(tree, pluginContext));

		output.deliver(result);

		logger.log(Level.INFO, "end: generating newick trees (" + (System.currentTimeMillis() - time) / 1000 + " sec)");
	}

	private TreeParameters constructNewickTreeParameters(final long seed) throws OperatorException {
		String experimentConfig = getParameterAsString(PARAM_KEY_CONFIG);
		String newickArgs = String.valueOf(getParameterAsInt(PARAM_KEY_MODE_ACTIVITIES)) + ";"
				+ String.valueOf(getParameterAsInt(PARAM_KEY_MIN_ACTIVITIES)) + ";"
				+ String.valueOf(getParameterAsInt(PARAM_KEY_MAX_ACTIVITIES)) + ";";
		if (experimentConfig != null && experimentConfig.equals(PARAM_DEFAULT_VALUE_CONFIG_MANUAL)) {
			// start parsing
			newickArgs += String.valueOf(getParameterAsDouble(PARAM_KEY_CONFIG_MANUAL_SEQ_PROB)) + ";"
					+ String.valueOf(getParameterAsDouble(PARAM_KEY_CONFIG_MANUAL_XOR_PROB)) + ";"
					+ String.valueOf(getParameterAsDouble(PARAM_KEY_CONFIG_MANUAL_PAR_PROB)) + ";"
					+ String.valueOf(getParameterAsDouble(PARAM_KEY_CONFIG_MANUAL_LOOP_PROB)) + ";"
					+ String.valueOf(getParameterAsDouble(PARAM_KEY_CONFIG_MANUAL_OR_PROB)) + ";";
		} else if (experimentConfig != null && experimentConfig.equals(PARAM_DEFAULT_VALUE_CONFIG_REPEATED)) {
			String operator = getParameterAsString(PARAM_KEY_CONFIG_EXP_OPERATORS);
			double mainOperatorShare = getParameterAsDouble(PARAM_KEY_CONFIG_EXP_PROBABILITY);
			double remains = 1d - mainOperatorShare;
			boolean useManualForDistribution = getParameterAsBoolean(PARAM_KEY_USE_MANUAL_DISTRIBUTION_FOR_SCALING);
			double seqShare = useManualForDistribution ? getParameterAsDouble(PARAM_KEY_CONFIG_MANUAL_SEQ_PROB)
					: PARAM_DEFAULT_CONFIG_MANUAL_SEQ_PROB;
			double xorShare = useManualForDistribution ? getParameterAsDouble(PARAM_KEY_CONFIG_MANUAL_XOR_PROB)
					: PARAM_DEFAULT_CONFIG_MANUAL_XOR_PROB;
			double parShare = useManualForDistribution ? getParameterAsDouble(PARAM_KEY_CONFIG_MANUAL_PAR_PROB)
					: PARAM_DEFAULT_CONFIG_MANUAL_PAR_PROB;
			double loopShare = useManualForDistribution ? getParameterAsDouble(PARAM_KEY_CONFIG_MANUAL_LOOP_PROB)
					: PARAM_DEFAULT_CONFIG_MANUAL_LOOP_PROB;
			double orShare = useManualForDistribution ? getParameterAsDouble(PARAM_KEY_CONFIG_MANUAL_OR_PROB)
					: PARAM_DEFAULT_CONFIG_MANUAL_OR_PROB;

			if (operator != null && operator.equals(PARAM_DEFAULT_VALUE_CONFIG_EXP_SEQ)) {
				newickArgs += operatorDistributionSeqFixed(mainOperatorShare, remains, xorShare, parShare, loopShare,
						orShare);
			} else if (operator != null && operator.equals(PARAM_DEFAULT_VALUE_CONFIG_EXP_XOR)) {
				newickArgs += operatorDistributionXorFixed(mainOperatorShare, remains, seqShare, parShare, loopShare,
						orShare);
			} else if (operator != null && operator.equals(PARAM_DEFAULT_VALUE_CONFIG_EXP_PAR)) {
				newickArgs += operatorDistributionParFixed(mainOperatorShare, remains, seqShare, xorShare, loopShare,
						orShare);
			} else if (operator != null && operator.equals(PARAM_DEFAULT_VALUE_CONFIG_EXP_LOOP)) {
				newickArgs += operatorDistributionLoopFixed(mainOperatorShare, remains, seqShare, xorShare, parShare,
						orShare);
			} else if (operator != null && operator.equals(PARAM_DEFAULT_VALUE_CONFIG_EXP_OR)) {
				newickArgs += operatorDistributionOrFixed(mainOperatorShare, remains, seqShare, xorShare, parShare,
						loopShare);
			}
		} else {
			throw new UndefinedParameterError("Could not parse operator probabilities.");
		}
		newickArgs += String.valueOf(getParameterAsDouble(PARAM_KEY_PROBABILITY_SILENT)) + ";";
		newickArgs += String.valueOf(getParameterAsDouble(PARAM_KEY_DUPLICATE_ACTIVITIES)) + ";";
		newickArgs += String.valueOf(getParameterAsDouble(PARAM_KEY_LONG_TERM)) + ";";
		newickArgs += (getParameterAsBoolean(PARAM_KEY_INFREQUENT_PATHS) ? "1.0" : "0.0") + ";";
		newickArgs += String.valueOf(getParameterAsInt(PARAM_KEY_COLLECTION_SIZE)) + ";";

		// TODO: expose these variables to users
		// note: these are simulation related variables!
		newickArgs += "0;"; // relates to use loop unfolding, true => 1, false
							// => 0
		newickArgs += "0;"; // relates to number of iterations of loops if loop
							// unfolding is used.

		newickArgs += String.valueOf(seed);
		return new TreeParameters(newickArgs);
	}

	// i know the distribution functions are "a bit ugly", however, I prefer
	// readability over code efficiency here.
	private String operatorDistributionSeqFixed(final double seqShareFixed, final double remains, final double xorShare,
			final double parShare, final double loopShare, final double orShare) throws OperatorException {
		double totalDist = xorShare + parShare + loopShare + orShare;
		double xorShareRelative = (xorShare / totalDist) * remains;
		double parShareRelative = (parShare / totalDist) * remains;
		double loopShareRelative = (loopShare / totalDist) * remains;
		double orShareRelative = (orShare / totalDist) * remains;
		double total = seqShareFixed + xorShareRelative + parShareRelative + loopShareRelative + orShareRelative;
		if (total < 0.999 || total > 1.001) {
			throw new OperatorException("Incorrect probability distribution");
		} else {
			return String.valueOf(seqShareFixed) + ";" + String.valueOf(xorShareRelative) + ";"
					+ String.valueOf(parShareRelative) + ";" + String.valueOf(loopShareRelative) + ";"
					+ String.valueOf(String.valueOf(orShareRelative)) + ";";
		}
	}

	private String operatorDistributionXorFixed(final double xorShareFixed, final double remains, final double seqShare,
			final double parShare, final double loopShare, final double orShare) throws OperatorException {
		double totalDist = seqShare + parShare + loopShare + orShare;
		double seqShareRelative = (seqShare / totalDist) * remains;
		double parShareRelative = (parShare / totalDist) * remains;
		double loopShareRelative = (loopShare / totalDist) * remains;
		double orShareRelative = (orShare / totalDist) * remains;
		double total = xorShareFixed + seqShareRelative + parShareRelative + loopShareRelative + orShareRelative;
		if (total < 0.999 || total > 1.001) {
			throw new OperatorException("Incorrect probability distribution");
		} else {
			return String.valueOf(seqShareRelative) + ";" + String.valueOf(xorShareFixed) + ";"
					+ String.valueOf(parShareRelative) + ";" + String.valueOf(loopShareRelative) + ";"
					+ String.valueOf(orShareRelative) + ";";
		}
	}

	private String operatorDistributionParFixed(final double parShareFixed, final double remains, final double seqShare,
			final double xorShare, final double loopShare, final double orShare) throws OperatorException {
		double totalDist = xorShare + seqShare + loopShare + orShare;
		double xorShareRelative = (xorShare / totalDist) * remains;
		double seqShareRelative = (seqShare / totalDist) * remains;
		double loopShareRelative = (loopShare / totalDist) * remains;
		double orShareRelative = (orShare / totalDist) * remains;
		double total = parShareFixed + xorShareRelative + seqShareRelative + loopShareRelative + orShareRelative;
		if (total < 0.999 || total > 1.001) {
			throw new OperatorException("Incorrect probability distribution");
		} else {
			return String.valueOf(seqShareRelative) + ";" + String.valueOf(xorShareRelative) + ";"
					+ String.valueOf(parShareFixed) + ";" + String.valueOf(loopShareRelative) + ";"
					+ String.valueOf(orShareRelative) + ";";
		}
	}

	private String operatorDistributionLoopFixed(final double loopShareFixed, final double remains,
			final double seqShare, final double xorShare, final double parShare, final double orShare)
			throws OperatorException {
		double totalDist = xorShare + seqShare + parShare + orShare;
		double xorShareRelative = (xorShare / totalDist) * remains;
		double seqShareRelative = (seqShare / totalDist) * remains;
		double parShareRelative = (parShare / totalDist) * remains;
		double orShareRelative = (orShare / totalDist) * remains;
		double total = loopShareFixed + xorShareRelative + seqShareRelative + parShareRelative + orShareRelative;
		if (total < 0.999 || total > 1.001) {
			throw new OperatorException("Incorrect probability distribution");
		} else {
			return String.valueOf(seqShareRelative) + ";" + String.valueOf(xorShareRelative) + ";"
					+ String.valueOf(parShareRelative) + ";" + String.valueOf(loopShareFixed) + ";"
					+ String.valueOf(orShareRelative) + ";";
		}
	}

	private String operatorDistributionOrFixed(final double orShareFixed, final double remains, final double seqShare,
			final double xorShare, final double parShare, final double loopShare) throws OperatorException {
		double totalDist = xorShare + seqShare + parShare + loopShare;
		double xorShareRelative = (xorShare / totalDist) * remains;
		double seqShareRelative = (seqShare / totalDist) * remains;
		double parShareRelative = (parShare / totalDist) * remains;
		double loopShareRelative = (loopShare / totalDist) * remains;
		double total = orShareFixed + xorShareRelative + seqShareRelative + parShareRelative + loopShare;
		if (total < 0.999 || total > 1.001) {
			throw new OperatorException("Incorrect probability distribution");
		} else {
			return String.valueOf(seqShareRelative) + ";" + String.valueOf(xorShareRelative) + ";"
					+ String.valueOf(parShareRelative) + ";" + String.valueOf(loopShareRelative) + ";"
					+ String.valueOf(orShareFixed) + ";";
		}
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> res = super.getParameterTypes();

		ParameterTypeInt minActs = new ParameterTypeInt(PARAM_KEY_MIN_ACTIVITIES, PARAM_DESC_MIN_ACTIVITIES, 0,
				Integer.MAX_VALUE, PARAM_DEFAULT_MIN_ACTIVITIES, false);
		res.add(minActs);

		ParameterTypeInt modeActs = new ParameterTypeInt(PARAM_KEY_MODE_ACTIVITIES, PARAM_DESC_MODE_ACTIVITIES, 0,
				Integer.MAX_VALUE, PARAM_DEFAULT_MODE_ACTIVITIES, false);
		res.add(modeActs);

		ParameterTypeInt maxActs = new ParameterTypeInt(PARAM_KEY_MAX_ACTIVITIES, PARAM_DESC_MAX_ACTIVITIES, 0,
				Integer.MAX_VALUE, PARAM_DEFAULT_MAX_ACTIVITIES, false);
		res.add(maxActs);

		ParameterTypeInt collSize = new ParameterTypeInt(PARAM_KEY_COLLECTION_SIZE, PARAM_DESC_COLLECTION_SIZE, 0,
				Integer.MAX_VALUE, PARAM_DEFAULT_COLLECTION_SIZE, false);
		res.add(collSize);

		ParameterTypeDouble silent = new ParameterTypeDouble(PARAM_KEY_PROBABILITY_SILENT,
				PARAM_DESC_PROBABILITY_SILENT, 0, 1, PARAM_DEFAULT_PROBABILITY_SILENT, false);
		res.add(silent);

		ParameterTypeDouble ltrm = new ParameterTypeDouble(PARAM_KEY_LONG_TERM, PARAM_DESC_LONG_TERM, 0, 1,
				PARAM_DEFAULT_LONG_TERM, false);
		res.add(ltrm);

		ParameterTypeDouble duplicates = new ParameterTypeDouble(PARAM_KEY_DUPLICATE_ACTIVITIES,
				PARAM_DESC_DUPLICATE_ACTIVITIES, 0, 1, PARAM_DEFAULT_DUPLICATE_ACTIVITIES, false);
		res.add(duplicates);

		ParameterTypeBoolean infrequentPaths = new ParameterTypeBoolean(PARAM_KEY_INFREQUENT_PATHS,
				PARAM_DESC_INFREQUENT_PATHS, PARAM_DEFAULT_INFREQUENT_PATHS, false);
		res.add(infrequentPaths);

		ParameterTypeCategory config = new ParameterTypeCategory(PARAM_KEY_CONFIG, PARAM_DESC_CONFIG, PARAM_VAL_CONFIG,
				PARAM_DEFAULT_CONFIG, false);
		res.add(config);

		ParameterTypeDouble confSeqProb = new ParameterTypeDouble(PARAM_KEY_CONFIG_MANUAL_SEQ_PROB,
				PARAM_DESC_CONFIG_MANUAL_SEQ_PROB, 0, 1, PARAM_DEFAULT_CONFIG_MANUAL_SEQ_PROB, false);
		confSeqProb.registerDependencyCondition(
				new EqualStringCondition(this, PARAM_KEY_CONFIG, true, PARAM_DEFAULT_VALUE_CONFIG_MANUAL));
		res.add(confSeqProb);

		ParameterTypeDouble confParProb = new ParameterTypeDouble(PARAM_KEY_CONFIG_MANUAL_PAR_PROB,
				PARAM_DESC_CONFIG_MANUAL_PAR_PROB, 0, 1, PARAM_DEFAULT_CONFIG_MANUAL_PAR_PROB, false);
		confParProb.registerDependencyCondition(
				new EqualStringCondition(this, PARAM_KEY_CONFIG, true, PARAM_DEFAULT_VALUE_CONFIG_MANUAL));
		res.add(confParProb);

		ParameterTypeDouble confChoProb = new ParameterTypeDouble(PARAM_KEY_CONFIG_MANUAL_XOR_PROB,
				PARAM_DESC_CONFIG_MANUAL_XOR_PROB, 0, 1, PARAM_DEFAULT_CONFIG_MANUAL_XOR_PROB, false);
		confChoProb.registerDependencyCondition(
				new EqualStringCondition(this, PARAM_KEY_CONFIG, true, PARAM_DEFAULT_VALUE_CONFIG_MANUAL));
		res.add(confChoProb);

		ParameterTypeDouble confOrProb = new ParameterTypeDouble(PARAM_KEY_CONFIG_MANUAL_OR_PROB,
				PARAM_DESC_CONFIG_MANUAL_OR_PROB, 0, 1, PARAM_DEFAULT_CONFIG_MANUAL_OR_PROB, false);
		confOrProb.registerDependencyCondition(
				new EqualStringCondition(this, PARAM_KEY_CONFIG, true, PARAM_DEFAULT_VALUE_CONFIG_MANUAL));
		res.add(confOrProb);

		ParameterTypeDouble confLoopProb = new ParameterTypeDouble(PARAM_KEY_CONFIG_MANUAL_LOOP_PROB,
				PARAM_DESC_CONFIG_MANUAL_LOOP_PROB, 0, 1, PARAM_DEFAULT_CONFIG_MANUAL_LOOP_PROB, false);
		confLoopProb.registerDependencyCondition(
				new EqualStringCondition(this, PARAM_KEY_CONFIG, true, PARAM_DEFAULT_VALUE_CONFIG_MANUAL));
		res.add(confLoopProb);

		ParameterTypeCategory repeatedExp = new ParameterTypeCategory(PARAM_KEY_CONFIG_EXP_OPERATORS,
				PARAM_DESC_CONFIG_EXP_OPERATORS, PARAM_VAL_CONFIG_EXP_OPERATORS, PARAM_DEFAULT_CONFIG_EXP_OPERATORS,
				false);
		repeatedExp.registerDependencyCondition(
				new EqualStringCondition(this, PARAM_KEY_CONFIG, true, PARAM_DEFAULT_VALUE_CONFIG_REPEATED));
		res.add(repeatedExp);

		ParameterTypeDouble repeatedExpProb = new ParameterTypeDouble(PARAM_KEY_CONFIG_EXP_PROBABILITY,
				PARAM_DESC_CONFIG_EXP_PROBABILITY, 0, 1, PARAM_DEFAULT_CONFIG_EXP_PROBABILITY, false);
		repeatedExpProb.registerDependencyCondition(
				new EqualStringCondition(this, PARAM_KEY_CONFIG, true, PARAM_DEFAULT_VALUE_CONFIG_REPEATED));
		res.add(repeatedExpProb);

		ParameterTypeBoolean repeatedManualScaling = new ParameterTypeBoolean(
				PARAM_KEY_USE_MANUAL_DISTRIBUTION_FOR_SCALING, PARAM_DESC_USE_MANUAL_DISTRIBUTION_FOR_SCALING,
				PARAM_DEFAULT_USE_MANUAL_DISTRIBUTION_FOR_SCALING, false);
		repeatedManualScaling.registerDependencyCondition(
				new EqualStringCondition(this, PARAM_KEY_CONFIG, true, PARAM_DEFAULT_VALUE_CONFIG_REPEATED));
		res.add(repeatedManualScaling);

		return res;
	}
}
