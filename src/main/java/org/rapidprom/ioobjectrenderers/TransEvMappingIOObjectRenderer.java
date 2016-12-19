package org.rapidprom.ioobjectrenderers;

import java.awt.Component;

import org.rapidprom.ioobjects.TransEvMappingIOObject;

import com.rapidminer.example.ExampleSet;
import com.rapidminer.gui.renderer.AbstractRenderer;
import com.rapidminer.gui.renderer.data.ExampleSetDataRenderer;
import com.rapidminer.operator.IOContainer;
import com.rapidminer.report.Reportable;

public class TransEvMappingIOObjectRenderer extends AbstractRenderer {

	@Override
	public String getName() {
		return "Transition to Event Class Mapping";
	}

	@Override
	public Component getVisualizationComponent(Object renderable, IOContainer ioContainer) {
		ExampleSet exampleSet = ((TransEvMappingIOObject) renderable).getAsExampleSet();
		ExampleSetDataRenderer renderer = new ExampleSetDataRenderer();
		return renderer.getVisualizationComponent(exampleSet, null);
	}

	@Override
	public Reportable createReportable(Object renderable, IOContainer ioContainer, int desiredWidth,
			int desiredHeight) {
		ExampleSet exampleSet = ((TransEvMappingIOObject) renderable).getAsExampleSet();
		ExampleSetDataRenderer renderer = new ExampleSetDataRenderer();
		return renderer.createReportable(exampleSet, null);
	}

}