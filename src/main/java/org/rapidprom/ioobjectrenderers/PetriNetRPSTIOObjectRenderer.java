package org.rapidprom.ioobjectrenderers;

import java.awt.BorderLayout;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.SwingConstants;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;
import javax.swing.tree.TreeSelectionModel;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.util.ui.scalableview.interaction.CompoundViewInteractionPanel;
import org.processmining.models.graphbased.directed.DirectedGraphElement;
import org.processmining.models.graphbased.directed.petrinet.PetrinetNode;
import org.processmining.models.jgraph.ProMJGraphVisualizer;
import org.processmining.models.jgraph.visualization.ProMJGraphPanel;
import org.processmining.models.util.ListSelectionPanel;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.PetriNetRPSTIOObject;
import org.rapidprom.operators.decomposition.rpst.PetriNetRPST;
import org.rapidprom.operators.decomposition.rpst.PetriNetRPSTNode;
import com.fluxicon.slickerbox.colors.SlickerColors;
import com.fluxicon.slickerbox.components.SlickerTabbedPane;
import com.fluxicon.slickerbox.factory.SlickerFactory;

public class PetriNetRPSTIOObjectRenderer
		extends AbstractRapidProMIOObjectRenderer<PetriNetRPSTIOObject> {
	
	private class FragSelectionPanel extends ListSelectionPanel {

		public FragSelectionPanel(String name, String title, boolean interactive) {
			super(name, title, interactive);
		}

		private static final long serialVersionUID = 1L;

		public void selectElements(Set<PetrinetNode> nodes) {

			Map<DirectedGraphElement, String> labelled = new HashMap<DirectedGraphElement, String>();
			for (DirectedGraphElement elt : nodes) {
				labelled.put(elt, elt.getLabel());
			}

			this.selectElements(labelled);
		}

	}


	/*
	 * 
	 * Code taken from DecomposedReplayer, thanks to Jorge
	 * 
	 */
	private class RPSTVisualization extends JPanel {
		
		private static final long serialVersionUID = 3920366007430435562L;
		
		private final FragSelectionPanel fsp;

		public RPSTVisualization(PluginContext context, PetriNetRPST rpst) {
			
			fsp = createHighLightComponent(rpst);
			
			//Create the central Panel
			JComponent centralP = createCentralPanel(context, rpst);
			
			//Create the Tree Panel
			JComponent treeP = createTreePanel(rpst);	
			
			//Create the The Final Pane and add the Panels
			SlickerTabbedPane pane = SlickerFactory.instance()
			.createTabbedPane("", SlickerColors.COLOR_BG_1,
					SlickerColors.COLOR_FG,SlickerColors.COLOR_FG);
			pane.setLayout(new BorderLayout(0, 0));
			pane.add(centralP, BorderLayout.CENTER);
			pane.add(treeP, BorderLayout.WEST);
			
			setLayout(new BorderLayout());
			add(pane, BorderLayout.CENTER);
		}

		public FragSelectionPanel createHighLightComponent(PetriNetRPST rpst){
			FragSelectionPanel fsp = new FragSelectionPanel("RPST", "RPST", true); 
			for(PetriNetRPSTNode node : rpst.getNodes()){
				Set<PetrinetNode> all = new HashSet<>();
				all.addAll(node.getTrans());
				all.addAll(node.getPlaces());
				fsp.addElementCollection(all, node.getName());
			}	
			return fsp;
		}
		
		public JComponent createCentralPanel(PluginContext context, PetriNetRPST rpst){
			//Create the Graph Panel
			ProMJGraphPanel p = ProMJGraphVisualizer.instance().visualizeGraph(context, rpst.getNet().getNet());
			//Create the HighLight Button in the bottom
			CompoundViewInteractionPanel res = new CompoundViewInteractionPanel("RPST");
			res.addViewInteractionPanel(fsp);
			p.addViewInteractionPanel(res, SwingConstants.SOUTH);
			return p;
		}
		
		public JComponent createTreePanel(PetriNetRPST rpst) {
			
			JScrollPane res = new JScrollPane();
			res.setViewportView(createTree(rpst));
			return res;
		}
		
		public JTree createTree(PetriNetRPST rpst){
			
			//CREATE AND EMPTY TREE
			JTree tree = new JTree();
			tree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
			
			//FILL THE TREE
			Queue<DefaultMutableTreeNode> toExplore = new LinkedList<>();
			DefaultMutableTreeNode root = new DefaultMutableTreeNode(rpst.getRoot());
			toExplore.add(root);
			while(!toExplore.isEmpty()){
				DefaultMutableTreeNode curr = toExplore.poll();
				for(PetriNetRPSTNode childRPST : rpst.getChildren((PetriNetRPSTNode) curr.getUserObject())){
					DefaultMutableTreeNode child = new DefaultMutableTreeNode(childRPST);
					curr.add(child);
					toExplore.add(child);
				}
			}
			tree.setModel(new DefaultTreeModel(root));
			
			//CREATE A SELECTION LISTENER TO HIGHLIGHT RPST NODES
			tree.addTreeSelectionListener(new TreeSelectionListener() {
				@Override
				public void valueChanged(TreeSelectionEvent e) {
					//Returns the last path element of the selection.
					//This method is useful only when the selection model allows a single selection.
					    DefaultMutableTreeNode node = (DefaultMutableTreeNode)
					    		e.getPath().getLastPathComponent();
					    if (node == null)
					    //Nothing is selected.     
					    return;

					    //Component Selected
					    PetriNetRPSTNode nodeRPST = (PetriNetRPSTNode) node.getUserObject();
					    
					    //Highlight the component elements in the Petri Net
					    Set<PetrinetNode> all = new HashSet<>();
						all.addAll(nodeRPST.getTrans());
						all.addAll(nodeRPST.getPlaces());
					    fsp.selectElements(all);
					    
				}
			});
			
			//Expand all tree
			expandAll(tree,true);
			
			//Select initially the root
			tree.addSelectionRow(0);
			tree.putClientProperty("JTree.lineStyle", "None");
			
			return tree;
		}
		
		
		
		private void expandAll(JTree tree, boolean expand) {
		    TreeNode root = (TreeNode)tree.getModel().getRoot();

		    // Traverse tree from root
		    expandAll(tree, new TreePath(root), expand);
		}
		
		private void expandAll(JTree tree, TreePath parent, boolean expand) {
		    // Traverse children
		    TreeNode node = (TreeNode)parent.getLastPathComponent();
		    if (node.getChildCount() >= 0) {
		        for (Enumeration<?> e=node.children(); e.hasMoreElements(); ) {
		            TreeNode n = (TreeNode)e.nextElement();
		            TreePath path = parent.pathByAddingChild(n);
		            expandAll(tree, path, expand);
		        }
		    }

		    // Expansion or collapse must be done bottom-up
		    if (expand) {
		        tree.expandPath(parent);
		    } else {
		        tree.collapsePath(parent);
		    }
		}

	}
	
	
	@Override
	public String getName() {
		return "Petri Net RPST renderer";
	}

	@Override
	protected JComponent runVisualization(PetriNetRPSTIOObject artifact) {
		return new RPSTVisualization(artifact.getPluginContext(), artifact.getArtifact());
	}
	
}