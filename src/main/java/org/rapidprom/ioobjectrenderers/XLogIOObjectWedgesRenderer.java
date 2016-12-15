package org.rapidprom.ioobjectrenderers;

import java.awt.Color;

import javax.swing.BorderFactory;
import javax.swing.JComponent;

import org.processmining.logenhancement.view.LogViewContextStandalone;
import org.processmining.logenhancement.view.LogViewVisualizer;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.XLogIOObject;

public class XLogIOObjectWedgesRenderer extends AbstractRapidProMIOObjectRenderer<XLogIOObject> {

	@Override
	public String getName() {
		return "Explore Event Log (Wedges)";
	}

	@Override
	protected JComponent runVisualization(XLogIOObject ioObject) {
		LogViewVisualizer visualizer = new LogViewVisualizer(new LogViewContextStandalone(null), ioObject.getArtifact());
		visualizer.setBorder(BorderFactory.createMatteBorder(1, 1, 1, 1, Color.BLACK));
		return visualizer;
	}

}