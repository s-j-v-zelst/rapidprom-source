package org.rapidprom.ioobjects;

import java.util.Comparator;
import java.util.Map.Entry;

import org.deckfour.xes.classification.XEventClass;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.models.graphbased.directed.petrinet.elements.Transition;
import org.processmining.plugins.connectionfactories.logpetrinet.TransEvClassMapping;
import org.rapidprom.ioobjects.abstr.AbstractRapidProMIOObject;

import com.google.common.collect.Ordering;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.tools.Ontology;

/**
 * Wraps an {@link TransEvClassMapping}.
 * 
 * @author F. Mannhardt
 *
 */
public class TransEvMappingIOObject extends AbstractRapidProMIOObject<TransEvClassMapping> {

	private static final long serialVersionUID = -990748394971952717L;
	private static final Comparator<Entry<Transition, XEventClass>> TRANSITION_ORDER = new Comparator<Entry<Transition, XEventClass>>() {

		@Override
		public int compare(Entry<Transition, XEventClass> o1, Entry<Transition, XEventClass> o2) {
			return o1.getKey().getLabel().compareTo(o2.getKey().getLabel());
		}
	};

	public TransEvMappingIOObject(TransEvClassMapping t, PluginContext context) {
		super(t, context);
	}

	public ExampleSet getAsExampleSet() {

		Attribute transitionAttr = AttributeFactory.createAttribute("transition", Ontology.NOMINAL);
		Attribute eventClassAttr = AttributeFactory.createAttribute("eventclass", Ontology.NOMINAL);

		MemoryExampleTable table = new MemoryExampleTable(transitionAttr, eventClassAttr);

		DataRowFactory factory = new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.');

		TransEvClassMapping mapping = getArtifact();
		for (Entry<Transition, XEventClass> entry : Ordering.from(TRANSITION_ORDER).sortedCopy(mapping.entrySet())) {
			Transition t = entry.getKey();
			XEventClass e = entry.getValue();
			DataRow row = factory.create(2);
			row.set(transitionAttr, transitionAttr.getMapping().mapString(t.getLabel()));
			if (e == null || mapping.getDummyEventClass().equals(e)) {
				row.set(eventClassAttr, Double.NaN);
			} else {
				row.set(eventClassAttr, eventClassAttr.getMapping().mapString(e.getId()));
			}
			table.addDataRow(row);
		}

		return table.createExampleSet();
	}

}