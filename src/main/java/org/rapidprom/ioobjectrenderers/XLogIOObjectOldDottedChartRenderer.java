package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.plugins.dottedchartanalysis.DottedChartAnalysis;
import org.processmining.plugins.dottedchartanalysis.model.DottedChartModel;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.XLogIOObject;

public class XLogIOObjectOldDottedChartRenderer extends AbstractRapidProMIOObjectRenderer<XLogIOObject> {

	@Override
	public String getName() {
		return "XLog (Dotted Chart - Legacy)  renderer";
	}

	@Override
	protected JComponent runVisualization(XLogIOObject ioObject) {

		DottedChartModel result = new DottedChartModel(ioObject.getPluginContext(), ioObject.getArtifact());
		return new DottedChartAnalysis(ioObject.getPluginContext(), result);
	}

}
