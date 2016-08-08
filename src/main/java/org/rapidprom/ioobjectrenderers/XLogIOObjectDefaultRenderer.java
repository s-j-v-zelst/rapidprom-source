package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.plugins.log.ui.logdialog.LogDialogInitializer;
import org.processmining.plugins.log.ui.logdialog.SlickerOpenLogSettings;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.XLogIOObject;

public class XLogIOObjectDefaultRenderer extends AbstractRapidProMIOObjectRenderer<XLogIOObject> {

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected JComponent runVisualization(XLogIOObject ioObject) {

		LogDialogInitializer i = new LogDialogInitializer();
		SlickerOpenLogSettings o = new SlickerOpenLogSettings();

		return o.showLogVis(ioObject.getPluginContext(), ioObject.getArtifact());
	}

}