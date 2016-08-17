package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.logprojection.LogProjectionPlugin;
import org.processmining.logprojection.LogView;
import org.processmining.logprojection.plugins.dottedchart.DottedChart.DottedChartException;
import org.processmining.logprojection.plugins.dottedchart.ui.DottedChartInspector;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.XLogIOObject;

public class XLogIOObjectNewDottedChartRenderer extends AbstractRapidProMIOObjectRenderer<XLogIOObject> {

	@Override
	public String getName() {
		return "XLog (Dotted Chart)  renderer";
	}

	@Override
	protected JComponent runVisualization(XLogIOObject ioObject) {

		LogView result = new LogView(ioObject.getArtifact());
		DottedChartInspector panel = null;
		try {
			panel = LogProjectionPlugin.visualize(ioObject.getPluginContext(), result);
		} catch (DottedChartException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return (JComponent) panel;
	}

}
