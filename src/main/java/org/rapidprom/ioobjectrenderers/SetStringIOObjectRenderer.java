package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;
import javax.swing.JLabel;

import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.SetStringIOObject;

public class SetStringIOObjectRenderer extends AbstractRapidProMIOObjectRenderer<SetStringIOObject> {

	@Override
	public String getName() {
	return "Predictor renderer";
	}
	
	@Override
	protected JComponent runVisualization(SetStringIOObject artifact) {
		return new JLabel();
	}
	
}
