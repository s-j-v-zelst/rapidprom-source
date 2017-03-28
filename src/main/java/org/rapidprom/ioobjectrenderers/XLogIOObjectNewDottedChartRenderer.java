package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;
import javax.swing.JLabel;

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

		LogView result = null;
		DottedChartInspector panel = null;
		try {
			result = new LogView(ioObject.getArtifact());
			panel = LogProjectionPlugin.visualize(ioObject.getPluginContext(), result);
		} catch (DottedChartException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return new JLabel("rendering failed (dotted chart exception)");
		} catch (Exception e) {
			return new JLabel("rendering failed (other exception)");
		}
		return (JComponent) panel;
	}

}
