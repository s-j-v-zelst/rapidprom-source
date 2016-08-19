package org.rapidprom.operators.padas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.processmining.openslex.metamodel.SLEXMMActivity;
import org.processmining.openslex.metamodel.SLEXMMActivityInstance;
import org.processmining.openslex.metamodel.SLEXMMAttribute;
import org.processmining.openslex.metamodel.SLEXMMAttributeValue;
import org.processmining.openslex.metamodel.SLEXMMCase;
import org.processmining.openslex.metamodel.SLEXMMClass;
import org.processmining.openslex.metamodel.SLEXMMDataModel;
import org.processmining.openslex.metamodel.SLEXMMEvent;
import org.processmining.openslex.metamodel.SLEXMMEventAttribute;
import org.processmining.openslex.metamodel.SLEXMMEventAttributeValue;
import org.processmining.openslex.metamodel.SLEXMMEventResultSet;
import org.processmining.openslex.metamodel.SLEXMMLog;
import org.processmining.openslex.metamodel.SLEXMMObject;
import org.processmining.openslex.metamodel.SLEXMMObjectVersion;
import org.processmining.openslex.metamodel.SLEXMMObjectVersionResultSet;
import org.processmining.openslex.metamodel.SLEXMMPeriod;
import org.processmining.openslex.metamodel.SLEXMMProcess;
import org.processmining.openslex.metamodel.SLEXMMRelation;
import org.processmining.openslex.metamodel.SLEXMMRelationship;
import org.rapidprom.ioobjects.padas.SLEXMMSubSetIOObject;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DoubleArrayDataRow;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.IOObjectCollection;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.CollectionMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.GenerateNewMDRule;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

public class SLEXMMSubSetToExampleSetConversionOperator extends Operator {

	public static final String TABLE_SLEXMM_TYPE_ANNOTATION = "slxmmtype";
	public static final String TABLE_SLEXMM_TYPE_STR_DATAMODEL = "slxmmdatamodel";
	public static final String TABLE_SLEXMM_TYPE_STR_PERIOD = "slxmmperiod";
	public static final String TABLE_SLEXMM_TYPE_STR_OBJECT = "slxmmobject";
	public static final String TABLE_SLEXMM_TYPE_STR_VERSION = "slxmmversion";
	public static final String TABLE_SLEXMM_TYPE_STR_CLASS = "slxmmclass";
	public static final String TABLE_SLEXMM_TYPE_STR_ATTRIBUTE = "slxmmattribute";
	public static final String TABLE_SLEXMM_TYPE_STR_RELATIONSHIP = "slxmmrelationship";
	public static final String TABLE_SLEXMM_TYPE_STR_RELATION = "slxmmrelation";
	public static final String TABLE_SLEXMM_TYPE_STR_EVENT = "slxmmevent";
	public static final String TABLE_SLEXMM_TYPE_STR_ACTIVITY_INSTANCE = "slxmmactivityinstance";
	public static final String TABLE_SLEXMM_TYPE_STR_ACTIVITY = "slxmmactivity";
	public static final String TABLE_SLEXMM_TYPE_STR_PROCESS = "slxmmprocess";
	public static final String TABLE_SLEXMM_TYPE_STR_CASE = "slxmmcase";
	public static final String TABLE_SLEXMM_TYPE_STR_LOG = "slxmmlog";
	
	/** defining the ports */
	private InputPort inputSet = getInputPorts().createPort("SLEXMMSubSetIO", SLEXMMSubSetIOObject.class);
	private OutputPort outputJoin = getOutputPorts().createPort("example set (Data Table)");
	//private OutputPort outputCol = getOutputPorts().createPort("collection of example sets (Data Table)");

	private ExampleSetMetaData metaDataJoin = null;
	//private CollectionMetaData metaDataCol = null;

	/**
	 * The default constructor needed in exactly this signature
	 */
	public SLEXMMSubSetToExampleSetConversionOperator(OperatorDescription description) {
		super(description);
	}

	@Override
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: SLEXMMSubSet to Table conversion");
		long time = System.currentTimeMillis();

		SLEXMMSubSetIOObject slexmmSubSet = inputSet.getData(SLEXMMSubSetIOObject.class);
		Class<?> type = slexmmSubSet.getType();
		Set<Object> set = slexmmSubSet.getResults();

		String typeName = null;
		
		MemoryExampleTable table = null;
		ExampleSet es = null;
		IOObjectCollection<ExampleSet> col = new IOObjectCollection<>();
		List<ExampleSet> esList = new ArrayList<>();
		
		boolean singleCol = true;
		
		try {

			metaDataJoin = new ExampleSetMetaData();
			//metaDataCol = new CollectionMetaData();

			ArrayList<Attribute> attributes = new ArrayList<>();
			Attribute atId = AttributeFactory.createAttribute("id", Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
			attributes.add(atId);

			if (type == SLEXMMObject.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_OBJECT;
				
				Attribute atClassId = AttributeFactory.createAttribute("classId",
						Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
				attributes.add(atClassId);

				table = new MemoryExampleTable(attributes);

				for (Object o : set) {
					SLEXMMObject slxmmobj = (SLEXMMObject) o;

					double[] doubleArray = new double[attributes.size()];
					doubleArray[0] = slxmmobj.getId();
					doubleArray[1] = slxmmobj.getClassId();

					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}

			} else if (type == SLEXMMObjectVersion.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_VERSION;
				
				singleCol = false;
				
				Attribute atObjectId = AttributeFactory.createAttribute("objectId",
						Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
				attributes.add(atObjectId);
				Attribute atStartTimestamp = AttributeFactory.createAttribute("startTimestamp",
						Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atStartTimestamp);
				Attribute atEndTimestamp = AttributeFactory.createAttribute("endTimestamp",
						Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atEndTimestamp);

				HashMap<SLEXMMAttribute,Attribute> attMap = new HashMap<>();
				HashMap<SLEXMMAttribute,Integer> attIndexMap = new HashMap<>();
								
				SLEXMMObjectVersionResultSet ovrset = slexmmSubSet.getArtifact()
						.getVersionsAndAttributeValues((Set<SLEXMMObjectVersion>) (Set<?>) set);
				
				Set<SLEXMMObjectVersion> setaux = new HashSet<>();
				
				SLEXMMObjectVersion slxmmov = null;
				
				while ((slxmmov = ovrset.getNextWithAttributes()) != null) {
					
					setaux.add(slxmmov);
					HashMap<SLEXMMAttribute, SLEXMMAttributeValue> attValsMap = slxmmov.getAttributeValues();
					
					for (SLEXMMAttribute slxmmAt : attValsMap.keySet()) {
						Attribute at = attMap.get(slxmmAt);
						if (at == null) {
							at = AttributeFactory.createAttribute(slxmmAt.toString(),
									Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
							attributes.add(at);
							attMap.put(slxmmAt, at);
							attIndexMap.put(slxmmAt,attributes.size()-1);
						}
					}
				}
				
				table = new MemoryExampleTable(attributes);

				for (SLEXMMObjectVersion slxmmobjV : setaux) {

					double[] doubleArray = new double[attributes.size()];
					
					for (int i = 0; i < doubleArray.length; i++) {
						doubleArray[i] = Double.NaN;
					}
					
					doubleArray[0] = slxmmobjV.getId();
					doubleArray[1] = slxmmobjV.getObjectId();
					doubleArray[2] = atStartTimestamp.getMapping()
							.mapString(String.valueOf(slxmmobjV.getStartTimestamp()));
					doubleArray[3] = atEndTimestamp.getMapping().mapString(String.valueOf(slxmmobjV.getEndTimestamp()));

					HashMap<SLEXMMAttribute, SLEXMMAttributeValue> attValsMap = slxmmobjV.getAttributeValues();
					
					for (SLEXMMAttribute slxmmAt : attValsMap.keySet()) {
						SLEXMMAttributeValue atV = attValsMap.get(slxmmAt);
						Attribute at = attMap.get(slxmmAt);
						Integer atIndex = attIndexMap.get(slxmmAt);
						
						doubleArray[atIndex] = at.getMapping().mapString(atV.getValue());
					}
					
					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}
				
				

			} else if (type == SLEXMMEvent.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_EVENT;
				
				singleCol = false;
				
				Attribute atActivityInstanceId = AttributeFactory.createAttribute("activityInstanceId",
						Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
				attributes.add(atActivityInstanceId);
				Attribute atOrder = AttributeFactory.createAttribute("order", Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
				attributes.add(atOrder);
				Attribute atTimestamp = AttributeFactory.createAttribute("timestamp",
						Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atTimestamp);
				Attribute atLifecycle = AttributeFactory.createAttribute("lifecycle",
						Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atLifecycle);
				Attribute atResource = AttributeFactory.createAttribute("resource",
						Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atResource);

				HashMap<SLEXMMEventAttribute,Attribute> attMap = new HashMap<>();
				HashMap<SLEXMMEventAttribute,Integer> attIndexMap = new HashMap<>();
								
				SLEXMMEventResultSet erset = slexmmSubSet.getArtifact()
						.getEventsAndAttributeValues((Set<SLEXMMEvent>) (Set<?>) set);
				
				Set<SLEXMMEvent> setaux = new HashSet<>();
				
				SLEXMMEvent slxmme = null;
				
				while ((slxmme = erset.getNextWithAttributes()) != null) {
					
					setaux.add(slxmme);
					HashMap<SLEXMMEventAttribute, SLEXMMEventAttributeValue> attValsMap = slxmme.getAttributeValues();
					
					for (SLEXMMEventAttribute slxmmAt : attValsMap.keySet()) {
						Attribute at = attMap.get(slxmmAt);
						if (at == null) {
							at = AttributeFactory.createAttribute(slxmmAt.toString(),
									Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
							at.setDefault(Double.NaN);
							attributes.add(at);
							attMap.put(slxmmAt, at);
							attIndexMap.put(slxmmAt,attributes.size()-1);
						}
					}
				}
				
				table = new MemoryExampleTable(attributes);
				
				for (Object o : setaux) {
					SLEXMMEvent slxmmo = (SLEXMMEvent) o;

					double[] doubleArray = new double[attributes.size()];
					
					for (int i = 0; i < doubleArray.length; i++) {
						doubleArray[i] = Double.NaN;
					}
					
					doubleArray[0] = slxmmo.getId();
					doubleArray[1] = slxmmo.getActivityInstanceId();
					doubleArray[2] = slxmmo.getOrder();
					doubleArray[3] = atTimestamp.getMapping().mapString(String.valueOf(slxmmo.getTimestamp()));
					doubleArray[4] = atLifecycle.getMapping().mapString(slxmmo.getLifecycle());
					doubleArray[5] = atResource.getMapping().mapString(slxmmo.getResource());

					HashMap<SLEXMMEventAttribute, SLEXMMEventAttributeValue> attValsMap = slxmmo.getAttributeValues();
					
					for (SLEXMMEventAttribute slxmmAt : attValsMap.keySet()) {
						SLEXMMEventAttributeValue atV = attValsMap.get(slxmmAt);
						Attribute at = attMap.get(slxmmAt);
						Integer atIndex = attIndexMap.get(slxmmAt);
						
						doubleArray[atIndex] = at.getMapping().mapString(atV.getValue());
					}
					
					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}
				
			} else if (type == SLEXMMActivity.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_ACTIVITY;
				
				Attribute atName = AttributeFactory.createAttribute("name", Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atName);

				table = new MemoryExampleTable(attributes);

				for (Object o : set) {
					SLEXMMActivity slxmmo = (SLEXMMActivity) o;

					double[] doubleArray = new double[attributes.size()];
					doubleArray[0] = slxmmo.getId();
					doubleArray[1] = atName.getMapping().mapString(slxmmo.getName());

					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}
			} else if (type == SLEXMMCase.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_CASE;
				
				Attribute atName = AttributeFactory.createAttribute("name", Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atName);

				table = new MemoryExampleTable(attributes);

				for (Object o : set) {
					SLEXMMCase slxmmo = (SLEXMMCase) o;

					double[] doubleArray = new double[attributes.size()];
					doubleArray[0] = slxmmo.getId();
					doubleArray[1] = atName.getMapping().mapString(slxmmo.getName());

					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}
			} else if (type == SLEXMMActivityInstance.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_ACTIVITY_INSTANCE;
				
				Attribute atActivityId = AttributeFactory.createAttribute("activityId",
						Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
				attributes.add(atActivityId);

				table = new MemoryExampleTable(attributes);

				for (Object o : set) {
					SLEXMMActivityInstance slxmmo = (SLEXMMActivityInstance) o;

					double[] doubleArray = new double[attributes.size()];
					doubleArray[0] = slxmmo.getId();
					doubleArray[1] = slxmmo.getActivityId();

					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}
			} else if (type == SLEXMMClass.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_CLASS;
				
				Attribute atName = AttributeFactory.createAttribute("name", Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atName);
				Attribute atDatamodelId = AttributeFactory.createAttribute("datamodelId",
						Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
				attributes.add(atDatamodelId);

				table = new MemoryExampleTable(attributes);

				for (Object o : set) {
					SLEXMMClass slxmmo = (SLEXMMClass) o;

					double[] doubleArray = new double[attributes.size()];
					doubleArray[0] = slxmmo.getId();
					doubleArray[1] = atName.getMapping().mapString(slxmmo.getName());
					doubleArray[2] = slxmmo.getDataModelId();

					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}
			} else if (type == SLEXMMRelation.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_RELATION;
				
				Attribute atRelationshipId = AttributeFactory.createAttribute("relationshipId",
						Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
				attributes.add(atRelationshipId);
				Attribute atSourceObjectVersionId = AttributeFactory.createAttribute("sourceObjectVersionId",
						Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
				attributes.add(atSourceObjectVersionId);
				Attribute atTargetObjectVersionId = AttributeFactory.createAttribute("targetObjectVersionId",
						Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
				attributes.add(atTargetObjectVersionId);
				Attribute atStartTimestamp = AttributeFactory.createAttribute("startTimestamp",
						Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atStartTimestamp);
				Attribute atEndTimestamp = AttributeFactory.createAttribute("endTimestamp",
						Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atEndTimestamp);

				table = new MemoryExampleTable(attributes);

				for (Object o : set) {
					SLEXMMRelation slxmmo = (SLEXMMRelation) o;

					double[] doubleArray = new double[attributes.size()];
					doubleArray[0] = slxmmo.getId();
					doubleArray[1] = slxmmo.getRelationshipId();
					doubleArray[2] = slxmmo.getSourceObjectVersionId();
					doubleArray[3] = slxmmo.getTargetObjectVersionId();
					doubleArray[4] = atStartTimestamp.getMapping()
							.mapString(String.valueOf(slxmmo.getStartTimestamp()));
					doubleArray[5] = atEndTimestamp.getMapping().mapString(String.valueOf(slxmmo.getEndTimestamp()));

					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}
			} else if (type == SLEXMMRelationship.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_RELATIONSHIP;
				
				Attribute atName = AttributeFactory.createAttribute("name", Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atName);

				Attribute atSourceClassId = AttributeFactory.createAttribute("sourceClassId",
						Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
				attributes.add(atSourceClassId);

				Attribute atTargetClassId = AttributeFactory.createAttribute("targetClassId",
						Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
				attributes.add(atTargetClassId);

				table = new MemoryExampleTable(attributes);

				for (Object o : set) {
					SLEXMMRelationship slxmmo = (SLEXMMRelationship) o;

					double[] doubleArray = new double[attributes.size()];
					doubleArray[0] = slxmmo.getId();
					doubleArray[1] = atName.getMapping().mapString(slxmmo.getName());
					doubleArray[2] = slxmmo.getSourceClassId();
					doubleArray[3] = slxmmo.getTargetClassId();

					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}
			} else if (type == SLEXMMAttribute.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_ATTRIBUTE;
				
				Attribute atName = AttributeFactory.createAttribute("name", Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atName);

				Attribute atClassId = AttributeFactory.createAttribute("classId",
						Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
				attributes.add(atClassId);

				table = new MemoryExampleTable(attributes);

				for (Object o : set) {
					SLEXMMAttribute slxmmo = (SLEXMMAttribute) o;

					double[] doubleArray = new double[attributes.size()];
					doubleArray[0] = slxmmo.getId();
					doubleArray[1] = atName.getMapping().mapString(slxmmo.getName());
					doubleArray[2] = slxmmo.getClassId();

					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}
			} else if (type == SLEXMMPeriod.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_PERIOD;
				
				attributes.clear();
				Attribute atStart = AttributeFactory.createAttribute("start", Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atStart);

				Attribute atEnd = AttributeFactory.createAttribute("end", Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atEnd);

				table = new MemoryExampleTable(attributes);

				for (Object o : set) {
					SLEXMMPeriod slxmmo = (SLEXMMPeriod) o;

					double[] doubleArray = new double[attributes.size()];
					doubleArray[0] = atStart.getMapping().mapString(String.valueOf(slxmmo.getStart()));
					doubleArray[1] = atEnd.getMapping().mapString(String.valueOf(slxmmo.getEnd()));

					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}
			} else if (type == SLEXMMDataModel.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_DATAMODEL;
				
				Attribute atName = AttributeFactory.createAttribute("name", Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atName);

				table = new MemoryExampleTable(attributes);

				for (Object o : set) {
					SLEXMMDataModel slxmmo = (SLEXMMDataModel) o;

					double[] doubleArray = new double[attributes.size()];
					doubleArray[0] = slxmmo.getId();
					doubleArray[1] = atName.getMapping().mapString(slxmmo.getName());

					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}
			} else if (type == SLEXMMLog.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_LOG;
				
				Attribute atName = AttributeFactory.createAttribute("name", Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atName);

				Attribute atProcessId = AttributeFactory.createAttribute("processId",
						Ontology.ATTRIBUTE_VALUE_TYPE.INTEGER);
				attributes.add(atProcessId);

				table = new MemoryExampleTable(attributes);

				for (Object o : set) {
					SLEXMMLog slxmmo = (SLEXMMLog) o;

					double[] doubleArray = new double[attributes.size()];
					doubleArray[0] = slxmmo.getId();
					doubleArray[1] = atName.getMapping().mapString(slxmmo.getName());
					doubleArray[2] = slxmmo.getProcessId();

					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}
			} else if (type == SLEXMMProcess.class) {
				typeName = TABLE_SLEXMM_TYPE_STR_PROCESS;
				
				Attribute atName = AttributeFactory.createAttribute("name", Ontology.ATTRIBUTE_VALUE_TYPE.NOMINAL);
				attributes.add(atName);

				table = new MemoryExampleTable(attributes);

				for (Object o : set) {
					SLEXMMProcess slxmmo = (SLEXMMProcess) o;

					double[] doubleArray = new double[attributes.size()];
					doubleArray[0] = slxmmo.getId();
					doubleArray[1] = atName.getMapping().mapString(slxmmo.getName());

					table.addDataRow(new DoubleArrayDataRow(doubleArray));
				}
			} else {
				String msg = "ERROR: Unknown type of result " + type;
				System.err.println(msg);
				throw new OperatorException(msg);
			}

			// create the exampleset
			es = table.createExampleSet();
			es.getAnnotations().put(TABLE_SLEXMM_TYPE_ANNOTATION, typeName);
			
//			if (singleCol) {
//				col.add(es);
//			} else {
//				for (ExampleSet exs: esList) {
//					col.add(exs);
//				}
//			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("error when creating exampleset, creating empty exampleset");
			List<Attribute> attributes = new LinkedList<Attribute>();
			table = new MemoryExampleTable(attributes);
			es = table.createExampleSet();
			throw new OperatorException(e.getMessage());
		}
		/** Adding a rule for the output */
		getTransformer().addRule(new GenerateNewMDRule(outputJoin, this.metaDataJoin));
		outputJoin.deliverMD(metaDataJoin);
		outputJoin.deliver(es);
		
		//getTransformer().addRule(new GenerateNewMDRule(outputCol, this.metaDataCol));
		//outputCol.deliverMD(metaDataCol);
		//outputCol.deliver(col);

		logger.log(Level.INFO,
				"End: Event Log to Table conversion (" + (System.currentTimeMillis() - time) / 1000 + " sec)");

	}

}
