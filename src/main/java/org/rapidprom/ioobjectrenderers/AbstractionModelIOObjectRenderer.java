package org.rapidprom.ioobjectrenderers;

import java.util.Map;

import javax.swing.JComponent;
import javax.swing.JLabel;

import org.processmining.logenhancement.abstraction.model.AbstractionModel;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionCho;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionId;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionInt;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionNCho;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionNPar;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionPar;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionParserVisitor;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionRepExact;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionRepInf;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionRepInfOne;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionRepZeroOne;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionRoot;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionSeq;
import org.processmining.logenhancement.abstraction.model.syntax.CompositionVisitorException;
import org.processmining.logenhancement.abstraction.model.syntax.SimpleNode;
import org.processmining.plugins.graphviz.dot.Dot;
import org.processmining.plugins.graphviz.dot.DotCluster;
import org.processmining.plugins.graphviz.dot.DotEdge;
import org.processmining.plugins.graphviz.dot.DotNode;
import org.processmining.plugins.graphviz.dot.Dot.GraphDirection;
import org.processmining.plugins.graphviz.visualisation.DotPanel;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.AbstractionModelIOObject;

import com.google.common.collect.ImmutableMap;
import com.kitfox.svg.SVGDiagram;

public class AbstractionModelIOObjectRenderer extends AbstractRapidProMIOObjectRenderer<AbstractionModelIOObject> {

	private static final class AbstractionModelAsDot {

		private final Dot dot;

		public AbstractionModelAsDot(AbstractionModel model, Dot dot, SVGDiagram svg) {
			this.dot = dot;
		}

		public Dot getDot() {
			return dot;
		}

	}

	private static final class DotVisitor implements CompositionParserVisitor {

		private static final ImmutableMap<String, String> OPERATOR_OPTIONS = ImmutableMap.of("shape", "circle", "width",
				"0", "height", "0", "margin", "0.05,0.05");
		private final Dot dot;

		public DotVisitor(Dot dot, AbstractionModel model) {
			this.dot = dot;
		}

		public Object visit(SimpleNode node, Object data) throws CompositionVisitorException {
			throw new IllegalStateException("No unamed nodes allowed!");
		}

		public Object visit(CompositionRoot node, Object data) throws CompositionVisitorException {
			if (node.jjtGetNumChildren() > 1) {
				throw new IllegalArgumentException("Invalid expression " + node
						+ " should not have been parsed! Top level element is only allowed to have one child.");
			}
			return node.jjtGetChild(0).jjtAccept(this, dot.addCluster());
		}

		public Object visit(CompositionPar node, Object data) throws CompositionVisitorException {
			DotCluster cluster = getOrCreateCurrentCluster(data);
			// cluster.setGraphOption("style", "invis");
			node.jjtGetChild(0).jjtAccept(this, cluster);
			node.jjtGetChild(1).jjtAccept(this, cluster);
			return cluster;
		}

		public Object visit(CompositionNPar node, Object data) throws CompositionVisitorException {
			DotCluster cluster = getOrCreateCurrentCluster(data);
			for (int i = 0; i < node.jjtGetNumChildren(); ++i) {
				node.jjtGetChild(i).jjtAccept(this, cluster);
			}
			return cluster;
		}

		public Object visit(CompositionCho node, Object data) throws CompositionVisitorException {
			DotCluster cluster = getOrCreateCurrentCluster(data);

			DotNode childNode1 = (DotNode) node.jjtGetChild(0).jjtAccept(this, cluster);
			DotNode choice = cluster.addNode("<<b>\u2715</b>>", OPERATOR_OPTIONS);
			DotNode childNode2 = (DotNode) node.jjtGetChild(1).jjtAccept(this, cluster);

			addEdge(cluster, choice, childNode1);
			addEdge(cluster, choice, childNode2);

			return cluster;
		}

		public Object visit(CompositionNCho node, Object data) throws CompositionVisitorException {
			DotCluster cluster = getOrCreateCurrentCluster(data);

			DotNode interleaving = cluster.addNode("<<b>\u2715</b>>", OPERATOR_OPTIONS);

			for (int i = 0; i < node.jjtGetNumChildren(); ++i) {
				DotNode childNode = (DotNode) node.jjtGetChild(i).jjtAccept(this, cluster);
				addEdge(cluster, interleaving, childNode, ImmutableMap.of("dir", "none"));
			}

			return cluster;
		}

		public Object visit(CompositionInt node, Object data) throws CompositionVisitorException {
			DotCluster cluster = getOrCreateCurrentCluster(data);

			DotNode interleaving = cluster.addNode("<<b>\u2194</b>>", OPERATOR_OPTIONS);

			for (int i = 0; i < node.jjtGetNumChildren(); ++i) {
				DotNode childNode = (DotNode) node.jjtGetChild(i).jjtAccept(this, cluster);
				addEdge(cluster, interleaving, childNode, ImmutableMap.of("dir", "none"));
			}

			return cluster;
		}

		public Object visit(CompositionSeq node, Object data) throws CompositionVisitorException {
			DotCluster cluster = getOrCreateCurrentCluster(data);

			DotNode childNode1 = (DotNode) node.jjtGetChild(0).jjtAccept(this, cluster);
			DotNode childNode2 = (DotNode) node.jjtGetChild(1).jjtAccept(this, cluster);

			if (childNode1 instanceof DotCluster || childNode2 instanceof DotCluster) {
				addDiEdge(getOrCreateParentCluster(data), childNode1, childNode2);
			} else {
				addDiEdge(cluster, childNode1, childNode2);
			}
			return cluster;
		}

		public Object visit(CompositionRepInf node, Object data) throws CompositionVisitorException {
			DotNode dotNode = (DotNode) node.jjtGetChild(0).jjtAccept(this, data);
			String constraint = "0..\u221E";
			if (!(dotNode instanceof DotCluster)) {
				String patternName = dotNode.getLabel();
				prepareHTMLLabel(dotNode);
				dotNode.setLabel(generateRepetitionLabel(patternName, constraint));
			} else {
				DotCluster cluster = (DotCluster) dotNode;
				cluster.setLabel("[" + constraint + "]");
			}
			return dotNode;
		}

		public Object visit(CompositionRepInfOne node, Object data) throws CompositionVisitorException {
			DotNode dotNode = (DotNode) node.jjtGetChild(0).jjtAccept(this, data);
			String constraint = "1..\u221E";
			if (!(dotNode instanceof DotCluster)) {
				String patternName = dotNode.getLabel();
				prepareHTMLLabel(dotNode);
				dotNode.setLabel(generateRepetitionLabel(patternName, constraint));
			} else {
				DotCluster cluster = (DotCluster) dotNode;
				cluster.setLabel("[" + constraint + "]");
			}
			return dotNode;
		}

		public Object visit(CompositionRepZeroOne node, Object data) throws CompositionVisitorException {
			DotNode dotNode = (DotNode) node.jjtGetChild(0).jjtAccept(this, data);
			String constraint = "0..1";
			if (!(dotNode instanceof DotCluster)) {
				String patternName = dotNode.getLabel();
				prepareHTMLLabel(dotNode);
				dotNode.setLabel(generateRepetitionLabel(patternName, constraint));
			} else {
				DotCluster cluster = (DotCluster) dotNode;
				cluster.setLabel("[" + constraint + "]");
			}
			return dotNode;
		}

		public Object visit(CompositionRepExact node, Object data) throws CompositionVisitorException {
			DotNode dotNode = (DotNode) node.jjtGetChild(0).jjtAccept(this, data);
			String constraint = node.getN() + ".." + node.getM();
			if (!(dotNode instanceof DotCluster)) {
				String patternName = dotNode.getLabel();
				prepareHTMLLabel(dotNode);
				dotNode.setLabel(generateRepetitionLabel(patternName, constraint));
			} else {
				DotCluster cluster = (DotCluster) dotNode;
				cluster.setLabel("[" + constraint + "]");
			}
			return dotNode;
		}

		private void prepareHTMLLabel(DotNode dotNode) {
			dotNode.setOption("shape", "plaintext");
			dotNode.setOption("margin", "0");
			dotNode.setOption("height", "0");
			dotNode.setOption("width", "0");
		}

		private String generateRepetitionLabel(String patternName, String repetition) {
			return "<" + "<table border=\"0\" cellborder=\"1\" cellspacing=\"0\" cellpadding=\"0\">\r\n" + "<tr>\r\n"
					+ "<td sides=\"tl\"></td>\r\n" + "<td sides=\"t\"></td>\r\n" + "<td sides=\"t\"></td>\r\n"
					+ "<td align=\"center\">" + "<font point-size=\"6.0\">" + repetition + "</font>" + "</td>\r\n"
					+ "</tr>\r\n" + "<tr>\r\n" + "<td sides=\"lrb\" colspan=\"4\" cellpadding=\"2\">" + patternName
					+ "</td>\r\n" + "</tr>\r\n" + "</table>" + ">";
		}

		public Object visit(CompositionId node, Object data) throws CompositionVisitorException {
			DotCluster cluster = (DotCluster) data;
			return cluster.addNode(getLiteralValue(node), ImmutableMap.of("shape", "box"));
		}

		private String getLiteralValue(CompositionId node) {
			String id = (String) node.jjtGetValue();
			if (id.charAt(0) == '"') {
				id = id.substring(1, id.length() - 1);
			}
			return id;
		}

		private void addDiEdge(DotCluster parent, DotNode source, DotNode target) {
			addEdge(parent, source, target, ImmutableMap.of("dir", "forward"));
		}

		private void addEdge(DotCluster parent, DotNode node1, DotNode node2) {
			addEdge(parent, node1, node2, ImmutableMap.of("dir", "none"));
		}

		private void addEdge(DotCluster parent, DotNode node1, DotNode node2, Map<String, String> options) {
			DotNode bridgeNode1 = addBridgeNodeIfNeccesary(node1);
			DotNode bridgeNode2 = addBridgeNodeIfNeccesary(node2);
			DotEdge edge = parent.addEdge(bridgeNode1, bridgeNode2, "", options);
			if (node1 instanceof DotCluster) {
				edge.setOption("ltail", node1.getId());
			}
			if (node2 instanceof DotCluster) {
				edge.setOption("lhead", node2.getId());
			}
		}

		private DotNode addBridgeNodeIfNeccesary(DotNode node) {
			if (node instanceof DotCluster) {
				return ((DotCluster) node).addNode("",
						ImmutableMap.of("shape", "plain", "style", "invis", "width", "0", "fixedsize", "true"));
			} else {
				return node;
			}
		}

		private DotCluster getOrCreateParentCluster(Object data) {
			if (data != null) {
				return (DotCluster) data;
			} else {
				throw new IllegalStateException("Should have a parent cluster!");
			}
		}

		private DotCluster getOrCreateCurrentCluster(Object data) {
			if (data != null) {
				DotCluster parentCluster = (DotCluster) data;
				DotCluster newCluster = parentCluster.addCluster();
				newCluster.setLabel("");
				return newCluster;
			} else {
				throw new IllegalStateException("Should have parent cluster!");
			}
		}

	}

	@Override
	public String getName() {
		return "Abstraction model renderer";
	}

	@Override
	protected JComponent runVisualization(AbstractionModelIOObject artifact) {
		AbstractionModel abstractionModel = artifact.getArtifact();
		try {
			AbstractionModelAsDot dot = convert(abstractionModel);
			return new DotPanel(dot.getDot());
		} catch (CompositionVisitorException e) {
			return new JLabel(e.getMessage());
		}
	}

	public static AbstractionModelAsDot convert(AbstractionModel model) throws CompositionVisitorException {
		Dot dot = new Dot();
		dot.setOption("compound", "true");
		dot.setNodeOption("fontname", "helvetica");
		dot.setGraphOption("labeljust", "r");
		dot.setDirection(GraphDirection.topDown);
		dot.setKeepOrderingOfChildren(false);
		CompositionRoot composition = model.getComposition();
		DotVisitor visitor = new DotVisitor(dot, model);
		visitor.visit(composition, null);

		SVGDiagram svg = DotPanel.dot2svg(dot);
		return new AbstractionModelAsDot(model, dot, svg);
	}

}