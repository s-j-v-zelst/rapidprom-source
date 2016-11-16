package org.rapidprom.ioobjectrenderers;

import java.awt.Color;
import java.util.Map;

import javax.swing.JComponent;

import org.deckfour.xes.model.XTrace;
import org.processmining.plugins.DataConformance.visualization.alignment.ColorTheme;
import org.processmining.plugins.DataConformance.visualization.alignment.XTraceResolver;
import org.processmining.plugins.DataConformance.visualization.grouping.GroupedAlignmentMasterDetail;
import org.processmining.plugins.DataConformance.visualization.grouping.GroupedAlignmentsSimpleImpl;
import org.processmining.plugins.DataConformance.visualization.grouping.GroupedAlignmentMasterView.GroupedAlignmentInput;
import org.processmining.xesalignmentextension.XAlignmentExtension.XAlignment;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.XAlignedLogIOObject;

public class XAlignedLogIOObjectWedgesRenderer extends AbstractRapidProMIOObjectRenderer<XAlignedLogIOObject> {

	private final static class NoTraceResolver implements XTraceResolver {
		@Override
		public boolean hasOriginalTraces() {
			return false;
		}

		@Override
		public XTrace getOriginalTrace(String name) {
			return null;
		}
	}

	@Override
	public String getName() {
		return "Explore Aligned Log";
	}

	@Override
	protected JComponent runVisualization(XAlignedLogIOObject ioObject) {
		Iterable<XAlignment> alignments = ioObject.getAsAlignedLog();
		Map<String, Color> activityColorMap = ColorTheme.createColorMap(alignments);
		GroupedAlignmentInput<XAlignment> input = new GroupedAlignmentInput<>(
				new GroupedAlignmentsSimpleImpl(alignments, activityColorMap), new NoTraceResolver(), activityColorMap);
		return new GroupedAlignmentMasterDetail(ioObject.getPluginContext(), input);
	}

}