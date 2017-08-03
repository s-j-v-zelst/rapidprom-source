/*
 * Taken from DecomposedReplayer package of ProM 6.7
 */
package org.rapidprom.operators.decomposition.rpst;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.jbpt.algo.tree.rpst.IRPSTNode;
import org.jbpt.petri.Flow;
import org.jbpt.petri.Node;
import org.processmining.acceptingpetrinet.models.AcceptingPetriNet;
import org.processmining.acceptingpetrinet.models.impl.AcceptingPetriNetFactory;
import org.processmining.models.graphbased.directed.DirectedGraphElement;
import org.processmining.models.graphbased.directed.petrinet.Petrinet;
import org.processmining.models.graphbased.directed.petrinet.PetrinetNode;
import org.processmining.models.graphbased.directed.petrinet.elements.Arc;
import org.processmining.models.graphbased.directed.petrinet.elements.Place;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.models.graphbased.directed.petrinet.impl.PetrinetFactory;
import org.processmining.models.semantics.petrinet.Marking;

import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseGraph;


/**
 * Class representing the RPST structure of a Petri Net.
 * 
 * See: Artem Polyvyanyy, Jussi Vanhatalo, Hagen Volzer: Simplified Computation 
 * and Generalization of the Refined Process Structure Tree. WS-FM 2010: 25-41
 * 
 * @author Jorge Munoz-Gama (jmunoz)
 */
public class PetriNetRPST implements Cloneable {
		
	/** Name of the RPST */
	private String name;
	/** Petri Net base of the RPST decomposition */
	private AcceptingPetriNet net;
	/** Tree Structure */
	private DirectedGraph<PetriNetRPSTNode, String> tree;
	/** Root */
	private PetriNetRPSTNode root;
	
	protected PetriNetRPST(){	
	}
	
	public PetriNetRPST(AcceptingPetriNet origNet){
		this("RPST of "+origNet.getNet().getLabel(), origNet);
	}
	
	public PetriNetRPST(String name, AcceptingPetriNet origNet){
		this.name = name;
		this.net = cloneAcceptingPetriNet(origNet, new HashMap<DirectedGraphElement, DirectedGraphElement>());
		this.tree = new DirectedSparseGraph<PetriNetRPSTNode, String>();
		init();
	}
	
	private void init(){
		//Build Prom/JBPT Petri Net
		ProMJBPTPetriNet multiNet = new ProMJBPTPetriNet(this.net.getNet());
		
		//Compute the RPST and the SESEs by the JBPT tool
		org.jbpt.algo.tree.rpst.RPST<Flow,Node> rpstJBPT = new org.jbpt.algo.tree.rpst.RPST<Flow,Node>(multiNet.getJbpt());
		
		//Root
		IRPSTNode<Flow, Node> rootJBPT = rpstJBPT.getRoot();
		PetriNetRPSTNode root = createRPSTNode(rootJBPT, multiNet);
		this.root = root;
		this.tree.addVertex(root);
		
		//Preparation for exploring the RPST Tree
		Queue<IRPSTNode<Flow, Node>> toExploreJBPT = new LinkedList<IRPSTNode<Flow, Node>>();
		toExploreJBPT.add(rootJBPT);
		Queue<PetriNetRPSTNode> toExploreRPST = new LinkedList<PetriNetRPSTNode>();
		toExploreRPST.add(root);
				
		//Exploration of RPST Tree
		while(!toExploreRPST.isEmpty()){
			IRPSTNode<Flow, Node> currJBPT = toExploreJBPT.poll();
			PetriNetRPSTNode curr = toExploreRPST.poll();
			
			Collection<IRPSTNode<Flow, Node>> childrenJBPT = rpstJBPT.getChildren(currJBPT);
			for(IRPSTNode<Flow, Node> childJBPT : childrenJBPT){
				PetriNetRPSTNode child = createRPSTNode(childJBPT, multiNet);
				this.tree.addVertex(child);
				this.tree.addEdge(curr.getId()+"=>"+child.getId(), curr, child);
					
				toExploreJBPT.add(childJBPT);
				toExploreRPST.add(child);
			}
		}
	}
	
	
	private PetriNetRPSTNode createRPSTNode(IRPSTNode<Flow, Node> nodeJBPT, ProMJBPTPetriNet multiNet) {
		//Arcs
		Set<Arc> arcs = new HashSet<Arc>();
		Set<Node> nodes = new HashSet<Node>();
		for(Flow flow: nodeJBPT.getFragment()){
			arcs.add(multiNet.jbpt2PromArc(flow));
			nodes.add(flow.getSource());
			nodes.add(flow.getTarget());
		}
		
		//Places and Transitions
		Set<Transition> trans = new HashSet<Transition>();
		Set<Place> places = new HashSet<Place>();
		for(Node node: nodes){
			if(multiNet.jbpt2PromNode(node) instanceof Transition) trans.add((Transition) multiNet.jbpt2PromNode(node));
			else if(multiNet.jbpt2PromNode(node) instanceof Place) places.add((Place) multiNet.jbpt2PromNode(node));
		}
		
		//Build the node
		return new PetriNetRPSTNode(nodeJBPT.getId(), nodeJBPT.getName(), nodeJBPT.getDescription(), 
				trans, places, arcs, multiNet.jbpt2PromNode(nodeJBPT.getEntry()), multiNet.jbpt2PromNode(nodeJBPT.getExit()));
	}

	
	
	
	public String getName() {
		return name;
	}

	public AcceptingPetriNet getNet() {
		return net;
	}

	public PetriNetRPSTNode getRoot() {
		return root;
	}
	
	public Collection<PetriNetRPSTNode> getNodes(){
		return tree.getVertices();
	}
	
	public Collection<PetriNetRPSTNode> getChildren(PetriNetRPSTNode parent){
		return tree.getSuccessors(parent);
	}
	
	
	public void removeNodeAndCollapse(PetriNetRPSTNode node){
		Collection<PetriNetRPSTNode> parents = this.tree.getPredecessors(node);
		Collection<PetriNetRPSTNode> children = this.tree.getSuccessors(node);
		
		for(PetriNetRPSTNode parent: parents){
			for(PetriNetRPSTNode child: children){
				this.tree.addEdge(parent.getId()+"=>"+child.getId(), parent, child);
			}
		}
		this.tree.removeVertex(node);
	}
	
	public PetriNetRPST clone(){
		Map<DirectedGraphElement, DirectedGraphElement> map = 
	    		new HashMap<DirectedGraphElement, DirectedGraphElement>();
		return clone(map);
	}
	
	public PetriNetRPST clone(Map<DirectedGraphElement, DirectedGraphElement> map){

	    PetriNetRPST clone = new PetriNetRPST();
	    clone.name = this.name;
	    
	    
	    clone.net = cloneAcceptingPetriNet(this.net, map);
	    
	    clone.tree = new DirectedSparseGraph<PetriNetRPSTNode, String>();
	    //Create Nodes
	    Map<PetriNetRPSTNode, PetriNetRPSTNode> node2node = 
	    		new HashMap<PetriNetRPSTNode, PetriNetRPSTNode>();
	    for(PetriNetRPSTNode node: this.tree.getVertices()){
	    	
	    	String id = node.getId();
	        String name = node.getName();
	        String desc = node.getDesc();
	    	
	    	Set<Transition> trans = new HashSet<Transition>();
	    	for(Transition t: node.getTrans()){
	    		trans.add((Transition)map.get(t));
	    	}
	    	Set<Place> places = new HashSet<Place>();
	    	for(Place p: node.getPlaces()){
	    		places.add((Place)map.get(p));
	    	}
	    	
	    	Set<Arc> arcs = new HashSet<Arc>();
	    	for(Arc a: node.getArcs()){
	    		PetrinetNode source = a.getSource();
	    		PetrinetNode target = a.getTarget();
	    		arcs.add(clone.net.getNet().getArc((PetrinetNode)map.get(source), (PetrinetNode)map.get(target)));
	    	}
	    	
	    	PetrinetNode entry = (PetrinetNode) map.get(node.getEntry());
	    	PetrinetNode exit = (PetrinetNode) map.get(node.getExit());
	    	
	    	PetriNetRPSTNode cloneNode = new PetriNetRPSTNode(id,name,desc,trans,places,arcs,entry,exit);
	    	node2node.put(node, cloneNode);
	    	clone.tree.addVertex(cloneNode);
	    }
	    //Root
	    clone.root = node2node.get(this.root);
	    
	    //Create Arcs
	    for(PetriNetRPSTNode node: this.tree.getVertices()){
	    	for(PetriNetRPSTNode child: this.tree.getSuccessors(node)){
	    		String edge = this.tree.findEdge(node, child);
	    		clone.tree.addEdge(edge, node2node.get(node), node2node.get(child));
	    	}
	    }

	    return clone;

	  }
	

	//TODO Move the function to DivideAndConquereFactory
	public static AcceptingPetriNet cloneAcceptingPetriNet(AcceptingPetriNet origAcceptingNet, Map<DirectedGraphElement, DirectedGraphElement> map){
		//Clone the Net
		
		Petrinet cloneNet = PetrinetFactory.clonePetrinet(origAcceptingNet.getNet(), map);
		
		//Clone the Initial Marking
		Marking cloneIniM = new Marking();
		for(Place origP: origAcceptingNet.getInitialMarking()){
			cloneIniM.add((Place) map.get(origP));
		}
		
		//Clone the Final Markings
		Set<Marking> cloneEndMarkings = new HashSet<Marking>();
		for(Marking origEndM: origAcceptingNet.getFinalMarkings()){
			Marking cloneEndM = new Marking();
			for(Place origP: origEndM){
				cloneEndM.add((Place) map.get(origP));
			}
			cloneEndMarkings.add(cloneEndM);
		}
		
		//Construct the cloned Accepting Petri Net
		AcceptingPetriNet cloneAcceptingNet = AcceptingPetriNetFactory.createAcceptingPetriNet(cloneNet);
		cloneAcceptingNet.setInitialMarking(cloneIniM);
		cloneAcceptingNet.setFinalMarkings(cloneEndMarkings);

		return cloneAcceptingNet;
	}

}
