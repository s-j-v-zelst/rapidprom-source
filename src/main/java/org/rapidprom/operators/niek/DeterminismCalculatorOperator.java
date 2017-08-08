package org.rapidprom.operators.niek;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.rapidprom.ioobjects.PNRepResultIOObject;
import org.rapidprom.ioobjects.PetriNetIOObject;
import org.rapidprom.ioobjects.XLogIOObject;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

import javassist.tools.rmi.ObjectNotFoundException;

public class DeterminismCalculatorOperator extends Operator {

	private InputPort input1 = getInputPorts()
			.createPort("petri net (ProM Petri net)", PetriNetIOObject.class);
	private InputPort input2 = getInputPorts().
			createPort("alignments (ProM PNRepResult)", PNRepResultIOObject.class);

	private OutputPort outputMetrics = getOutputPorts().createPort("example set (Data Table)");

	//private ExampleSetMetaData metaData = null;

	private final String NAMECOL = "Name";
	private final String VALUECOL = "Value";

	public DeterminismCalculatorOperator(OperatorDescription description) {
		super(description);
	}

	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		logger.log(Level.INFO, "Start: Calculate Determinism");
		
		long time = System.currentTimeMillis();
		
		PetriNetIOObject net = input1.getData(PetriNetIOObject.class);
		PNRepResultIOObject alignment = input2.getData(PNRepResultIOObject.class);

		double nondeterminism = 0d;
		try {
			nondeterminism = DeterminismCalculator.getDeterminism(net.getArtifact(), alignment.getArtifact(), net.getInitialMarking());
		} catch (ObjectNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		ExampleSet es = null;
		MemoryExampleTable table = null;
		List<Attribute> attributes = new LinkedList<Attribute>();
		attributes.add(AttributeFactory.createAttribute(this.NAMECOL, Ontology.STRING));
		attributes.add(AttributeFactory.createAttribute(this.VALUECOL, Ontology.NUMERICAL));
		table = new MemoryExampleTable(attributes);

		fillTableWithRow(table, "Nondeterminism", nondeterminism, attributes);
		
		es = table.createExampleSet();
		outputMetrics.deliver(es);

		logger.log(Level.INFO, "End: Calculate Determinism ("
				+ (System.currentTimeMillis() - time) / 1000 + " sec)");
	}
	
	private void fillTableWithRow(MemoryExampleTable table, String name,
			Object value, List<Attribute> attributes) {
		// fill table
		DataRowFactory factory = new DataRowFactory(
				DataRowFactory.TYPE_DOUBLE_ARRAY, '.');
		Object[] vals = new Object[2];
		vals[0] = name;
		vals[1] = value;
		// convert the list to array
		Attribute[] attribArray = new Attribute[attributes.size()];
		for (int i = 0; i < attributes.size(); i++) {
			attribArray[i] = attributes.get(i);
		}
		DataRow dataRow = factory.create(vals, attribArray);
		table.addDataRow(dataRow);
	}

}