package org.rapidprom.operators.padas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreeNode;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.processmining.openslex.metamodel.SLEXMMActivity;
import org.processmining.openslex.metamodel.SLEXMMActivityResultSet;
import org.processmining.openslex.metamodel.SLEXMMCase;
import org.processmining.openslex.metamodel.SLEXMMClass;
import org.processmining.openslex.metamodel.SLEXMMClassResultSet;
import org.processmining.openslex.metamodel.SLEXMMLog;
import org.processmining.openslex.metamodel.SLEXMMProcess;
import org.processmining.openslex.metamodel.SLEXMMRelationship;
import org.processmining.openslex.metamodel.SLEXMMRelationshipResultSet;
import org.processmining.openslex.metamodel.SLEXMMSQLResult;
import org.processmining.openslex.metamodel.SLEXMMSQLResultSet;
import org.processmining.openslex.metamodel.SLEXMMStorageMetaModel;
import org.rapidprom.ioobjects.padas.SLEXMMIOObject;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.set.SimpleExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DoubleArrayDataRow;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

public class PADASLogSplitterOperator extends Operator {

	public static final String PARAMETER_1 = "Log Name";
	public static final String PARAMETER_2 = "Process Name";

	private InputPort inputDMClasses = getInputPorts().createPort (
			"Classes definition", ExampleSet.class);
	
	private InputPort inputRelationships = getInputPorts().createPort(
			"Relationships definition", ExampleSet.class);

	private InputPort inputMM = getInputPorts().createPort(
			"OpenSLEX Meta Model", SLEXMMIOObject.class);
	
	private OutputPort outputMM = getOutputPorts().createPort(
			"OpenSLEX Meta Model");
	
	private MetaData outputMD = new MetaData(SLEXMMIOObject.class);
	
	public PADASLogSplitterOperator(OperatorDescription description) {
		super(description);
		getTransformer().addRule(new GenerateNewMDRule(outputMM, SLEXMMIOObject.class));
		//outputMMSet.deliverMD(outputMD);
	}
	
	@Override
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: padas log splitter");
		long time = System.currentTimeMillis();
		
		boolean failed = true;
		String msgFailure = "Building the log failed"; // FIXME
		
		String logName = getParameter(PARAMETER_1);
		String procName = getParameter(PARAMETER_2);

		ExampleSet exsDMClasses = inputDMClasses.getData(ExampleSet.class);
		ExampleSet exsRelationships = inputRelationships.getData(ExampleSet.class);
		SLEXMMIOObject slexmm = inputMM.getData(SLEXMMIOObject.class);
		
		Attribute atClassName = exsDMClasses.getAttributes().get(PADASConstants.AT_CLASS);
		Attribute atRootClass = exsDMClasses.getAttributes().get(PADASConstants.AT_ROOT);
		Attribute atLimitClass = exsDMClasses.getAttributes().get(PADASConstants.AT_LIMIT);
		Attribute atRelshName = exsRelationships.getAttributes().get(PADASConstants.AT_RS_NAME);
		
		try {

			HashSet<String> classes = new HashSet<>();
			HashSet<String> rootClasses = new HashSet<>();
			String rootClassName = null;
			HashSet<String> limitClasses = new HashSet<>();
			
			for (Example ex: exsDMClasses) {
				String cname = ex.getValueAsString(atClassName);
				String root = ex.getValueAsString(atRootClass);
				String limit = ex.getValueAsString(atLimitClass);
				classes.add(cname);
				if (root.equalsIgnoreCase("Y")) {
					rootClasses.add(cname);
				}
				if (limit.equalsIgnoreCase("Y")) {
					limitClasses.add(cname);
				}
			}
			
			if (rootClasses.size() != 1) {
				throw new Exception("Only one class can be root");
			}
						
			HashSet<String> relationships = new HashSet<>();
			
			for (Example ex: exsRelationships) {
				String rsname = ex.getValueAsString(atRelshName);
				relationships.add(rsname);
			}
		
			CaseNotionSubGraph caseNotion = getSubGraphDataModel(slexmm.getArtifact(),
					classes, rootClasses, limitClasses, relationships);
			
			ConnectivityInspector<SLEXMMClass, SLEXMMRelationship> conInspector = new ConnectivityInspector<>(caseNotion.graph);
			
			if (!conInspector.isGraphConnected()) {
				throw new Exception("Chosen Case Notion results in a disconnected graph");
			}
			
			String query = generateQueryFromCaseNotion(caseNotion);
			
			System.out.println("QUERY: ");
			System.out.println(query);
			
			HashMap<String,HashSet<Integer>> caseMap = new HashMap<>();
			
			SLEXMMSQLResultSet sqlrset = slexmm.getArtifact().executeSQL(query);
			
			int[] aiindx = aiColumns(sqlrset.getColumnNames());
			int[] cnindx = cnIndexes(sqlrset.getColumnNames(),caseNotion);
			
			SLEXMMSQLResult sqlr = null;
			
			while ((sqlr = sqlrset.getNext()) != null) {
				String[] vals = sqlr.getValues();
				String caseName = computeCaseName(vals,cnindx);
				if (!caseMap.containsKey(caseName)) {
					caseMap.put(caseName,new HashSet<Integer>());
				}
				for (int i: aiindx) {
					try {
						Integer aiid = Integer.valueOf(vals[i]);
						caseMap.get(caseName).add(aiid);
					} catch (NumberFormatException e) {
						// Number cannot be parsed. Probably it is a NULL
					}
				}
			}
			
			slexmm.getArtifact().setAutoCommit(false);
			
			SLEXMMProcess slxproc = slexmm.getArtifact().createProcess(procName);
			SLEXMMLog slxlog = slexmm.getArtifact().createLog(slxproc.getId(), logName);
			int[] casesIds = new int[caseMap.size()];
			int pos = 0;
			
			for (String cn: caseMap.keySet()) {
				SLEXMMCase slxcase = slexmm.getArtifact().createCase(cn);
				slxlog.add(slxcase.getId());
				for (Integer aiid: caseMap.get(cn)) {
					slxcase.add(aiid);
				}
				casesIds[pos] = slxcase.getId();
				pos++;
			}
			
			SLEXMMActivityResultSet arset = slexmm.getArtifact().getActivitiesForCases(casesIds);
			SLEXMMActivity act = null;
			HashSet<Integer> aiIds = new HashSet<>();
						
			while((act = arset.getNext()) != null) {
				aiIds.add(act.getId());
			}
			
			for (int aiid: aiIds) {
				slxproc.add(aiid);
			}
			
			slexmm.getArtifact().commit();
			slexmm.getArtifact().setAutoCommit(true);
			
			failed = false;
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new OperatorException(e.getMessage());
		}
		
		if (!failed) {
			outputMM.deliverMD(outputMD);
			outputMM.deliver(slexmm);
		} else {
			outputMM.deliverMD(outputMD);
			outputMM.deliver(slexmm);
			throw new OperatorException(msgFailure);
		}
		logger.log(Level.INFO,
				"End: padas log splitter ("
						+ (System.currentTimeMillis() - time) / 1000 + " sec)");
	}
	
	public int[] aiColumns(String[] cols) {
		ArrayList<Integer> list = new ArrayList<>();
		
		for (int i = 0; i < cols.length; i++) {
			String c = cols[i];
			if (c.startsWith("aiid_")) {
				list.add(i);
			}
		}
		
		int[] indexes = new int[list.size()];
		for (int i = 0; i < indexes.length; i++) {
			indexes[i] = list.get(i);
		}
		
		return indexes;
	}
	
	public String computeCaseName(String[] values, int[] cnindexes) {
		StringBuilder cnB = new StringBuilder();
		for (int i: cnindexes) {
			cnB.append(values[i]);
			cnB.append("#");
		}
		return cnB.toString();
	}
	
	public int[] cnIndexes(String[] cols, CaseNotionSubGraph cn) {
		HashSet<Integer> classIds = new HashSet<>();
		
		buildCnIndex(classIds,cn.rootClass,cn);
		
		int[] cnindxs = new int[classIds.size()];
		int pos = 0;
		
		for (int i = 0; i < cols.length; i++) {
			String colName = cols[i];
			if (colName.startsWith("oid_")) {
				int cid = Integer.valueOf(colName.substring("oid_".length(), colName.length()));
				if (classIds.contains(cid)) {
					cnindxs[pos] = i;
					pos++;
				}
			}
		}
		
		return cnindxs;
	}
	
	public void buildCnIndex(HashSet<Integer> classIds, SLEXMMClass c, CaseNotionSubGraph cn) {
		
		if (cn.limitClasses.contains(c)) {
			return;
		}
		
		classIds.add(c.getId());
		
		for (SLEXMMRelationship slxrs: cn.treeGraph.outgoingEdgesOf(c)) {
			SLEXMMClass tc = cn.treeGraph.getEdgeTarget(slxrs);
			buildCnIndex(classIds,tc,cn);
		}
	
	}
	
	public String generateQueryFromCaseNotion(CaseNotionSubGraph cnsg) {
		StringBuilder q = new StringBuilder();
		
		SLEXMMClass c = cnsg.rootClass;

		q.append("SELECT DISTINCT * FROM ( SELECT OBJV.object_id as oid_");
		q.append(c.getId());
		q.append(", OBJV.id as vid_");
		q.append(c.getId());
		q.append(", EV.activity_instance_id as aiid_");
		q.append(c.getId());
		q.append(" FROM object_version as OBJV, object as OBJ, event_to_object_version as ETOV, event as EV ");
		q.append(" WHERE OBJV.object_id = OBJ.id AND ");
		q.append(" ETOV.object_version_id = OBJV.id AND ");
		q.append(" ETOV.event_id = EV.id AND ");
		q.append(" OBJ.class_id = ");
		q.append(c.getId());
		
		q.append(") AS JSQ_");
		q.append(c.getId());
		
		generateSubQueryFromCaseNotion(cnsg.rootClass, q, cnsg);
				
		return q.toString();
	}
	
	public void generateSubQueryFromCaseNotion(SLEXMMClass pc, StringBuilder q, CaseNotionSubGraph cnsg) {
		
		for (SLEXMMRelationship rs: cnsg.treeGraph.outgoingEdgesOf(pc)) {
			
			int tid = rs.getTargetClassId();
			int sid = rs.getSourceClassId();
			
			SLEXMMClass c = null;
			boolean target = true;
			
			if (sid == pc.getId()) {
				c = cnsg.mapAllClasses.get(tid);
			} else {
				target = false;
				c = cnsg.mapAllClasses.get(sid);
			}

			q.append(" LEFT OUTER JOIN ( ");

			q.append(" SELECT OBJV.object_id as oid_");
			q.append(c.getId());
			q.append(", OBJV.id as vid_");
			q.append(c.getId());
			q.append(", REL");
			if (target) {
				q.append(".source_object_version_id as tvid_");
			} else {
				q.append(".target_object_version_id as tvid_");
			}
			q.append(c.getId());
			q.append(", EV.activity_instance_id as aiid_");
			q.append(c.getId());
			q.append(" FROM object_version as OBJV, relation as REL, event_to_object_version as ETOV, event as EV ");
			
			if (target) {
				q.append(" WHERE REL.target_object_version_id = OBJV");
			} else {
				q.append(" WHERE REL.source_object_version_id = OBJV");
			}
			q.append(".id AND ");
			q.append(" REL.relationship_id = ");
			q.append(rs.getId());
			q.append(" AND ETOV.object_version_id = OBJV.id ");
			q.append(" AND ETOV.event_id = EV.id ");
			
			q.append(" ) as JSQ_");
			q.append(c.getId());
			q.append(" ON JSQ_");
			q.append(c.getId());
			q.append(".tvid_");
			q.append(c.getId());
			q.append(" = JSQ_");
			q.append(pc.getId());
			q.append(".vid_");
			q.append(pc.getId());

			generateSubQueryFromCaseNotion(c,q,cnsg);
		}
		
	}
	
	public class CaseNotionSubGraph {
		DirectedAcyclicGraph<SLEXMMClass, SLEXMMRelationship> graph = null;
		DirectedAcyclicGraph<SLEXMMClass, SLEXMMRelationship> treeGraph = null;
		HashMap<Integer,SLEXMMClass> mapAllClasses = null;
		
		SLEXMMClass rootClass = null;
		HashSet<SLEXMMClass> limitClasses = null;
	}
	
	public CaseNotionSubGraph getSubGraphDataModel(SLEXMMStorageMetaModel mm, HashSet<String> classes,
			HashSet<String> rootClassesNames, HashSet<String> limitClassesNames, HashSet<String> relationships) {
		
		CaseNotionSubGraph cnsg = new CaseNotionSubGraph();
				
		DirectedAcyclicGraph<SLEXMMClass, SLEXMMRelationship> graph = new DirectedAcyclicGraph<>(
				SLEXMMRelationship.class);
		
		SLEXMMClassResultSet crset = mm.getClasses();
		HashMap<Integer,SLEXMMClass> mapAllClasses = new HashMap<>();
		
		SLEXMMClass c = null;
		SLEXMMClass rootClass = null;
		HashSet<SLEXMMClass> limitClasses = new HashSet<>();
		
		while ((c = crset.getNext()) != null) {
			mapAllClasses.put(c.getId(),c);
			if (rootClassesNames.contains(c.getName())) {
				rootClass = c;
				graph.addVertex(rootClass,true);
			} else if (classes.contains(c.getName())) {
				graph.addVertex(c);
			}
			if (limitClassesNames.contains(c.getName())) {
				limitClasses.add(c);
			}
		}
		
		List<SLEXMMRelationship> listRelationships = mm.getRelationships();
		
		for (SLEXMMRelationship rs: listRelationships) {
			if (relationships.contains(rs.getName())) {
				int scid = rs.getSourceClassId();
				int tcid = rs.getTargetClassId();
				SLEXMMClass sc = mapAllClasses.get(scid);
				SLEXMMClass tc = mapAllClasses.get(tcid);
				graph.addEdge(sc, tc, rs);
			}
		}
		
		cnsg.mapAllClasses = mapAllClasses;
		cnsg.limitClasses = limitClasses;
		cnsg.graph = graph;
		cnsg.rootClass = rootClass;
		cnsg.treeGraph = new DirectedAcyclicGraph<>(SLEXMMRelationship.class);
		
		buildTreeGraph(rootClass,cnsg);
		
		return cnsg;
	}
	
	public void buildTreeGraph(SLEXMMClass node, CaseNotionSubGraph cnsg) {
		Set<SLEXMMRelationship> aeset = cnsg.graph.edgesOf(node);
		
		for (SLEXMMRelationship rs: aeset) {
			if (!cnsg.treeGraph.edgeSet().contains(rs)) {
				SLEXMMClass tc = null;
				
				if (node.getId() == rs.getSourceClassId()) {
					tc = cnsg.mapAllClasses.get(rs.getTargetClassId());
				} else {
					tc = cnsg.mapAllClasses.get(rs.getSourceClassId()); 
				}
				
				if (!cnsg.treeGraph.containsVertex(node)) {
					cnsg.treeGraph.addVertex(node);
				}
				
				if (!cnsg.treeGraph.containsVertex(tc)) {
					cnsg.treeGraph.addVertex(tc);
				}

				cnsg.treeGraph.addEdge(node, tc, rs);
				buildTreeGraph(tc,cnsg);
			}
		}
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> parameterTypes = super.getParameterTypes();

		ParameterType logNameType = new ParameterTypeString(PARAMETER_1,PARAMETER_1,"",true);
		parameterTypes.add(logNameType);
		ParameterType procNameType = new ParameterTypeString(PARAMETER_2,PARAMETER_2,"",true);
		parameterTypes.add(procNameType);

		return parameterTypes;
	}
	
}
