package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.plugins.log.ui.logdialog.LogDialogInitializer;
import org.processmining.plugins.log.ui.logdialog.SlickerOpenLogSettings;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.XLogIOObject;

public class XLogIOObjectDefaultRenderer extends AbstractRapidProMIOObjectRenderer<XLogIOObject> {

	@Override
	public String getName() {
		return "XLog default renderer";
	}

	@Override
	protected JComponent runVisualization(XLogIOObject ioObject) {

		SlickerOpenLogSettings o = new SlickerOpenLogSettings();

		return o.showLogVis(ioObject.getPluginContext(), ioObject.getArtifact());
	}

}