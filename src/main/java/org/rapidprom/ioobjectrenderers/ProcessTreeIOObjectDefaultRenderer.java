package org.rapidprom.ioobjectrenderers;

import javax.swing.JComponent;

import org.processmining.processtree.visualization.tree.TreeVisualization;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.ProcessTreeIOObject;

public class ProcessTreeIOObjectDefaultRenderer extends AbstractRapidProMIOObjectRenderer<ProcessTreeIOObject> {

	@Override
	public String getName() {
		return "Process Tree (Default) renderer";
	}

	@Override
	protected JComponent runVisualization(ProcessTreeIOObject ioObject) {
		TreeVisualization visualizer = new TreeVisualization();
		return visualizer.visualize(null, ioObject.getArtifact());
	}
}