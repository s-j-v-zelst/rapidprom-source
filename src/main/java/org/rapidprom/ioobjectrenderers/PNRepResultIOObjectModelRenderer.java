package org.rapidprom.ioobjectrenderers;

import java.util.logging.Level;

import javax.swing.JComponent;
import javax.swing.JLabel;

import org.processmining.framework.connections.ConnectionCannotBeObtained;
import org.processmining.plugins.pnalignanalysis.visualization.projection.PNLogReplayProjectedVisPanel;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.PNRepResultIOObject;

import com.rapidminer.tools.LogService;

import javassist.tools.rmi.ObjectNotFoundException;

public class PNRepResultIOObjectModelRenderer extends AbstractRapidProMIOObjectRenderer<PNRepResultIOObject> {

	@Override
	public String getName() {
		return "PNRepResult (Project on Model) renderer";
	}

	@Override
	protected JComponent runVisualization(PNRepResultIOObject ioObject) {
		try {
			if (ioObject.getArtifact() == null) {
				return new JLabel("No alignment could be computed");
			} 
			return new PNLogReplayProjectedVisPanel(ioObject.getPluginContext(), ioObject.getPn().getArtifact(),
					ioObject.getPn().getInitialMarking(), ioObject.getXLog(), ioObject.getMapping(),
					ioObject.getArtifact());
		} catch (ObjectNotFoundException e) {
			LogService.getRoot().log(Level.SEVERE, "Failed to retrieve marking", e);
		} catch (ConnectionCannotBeObtained e) {
			LogService.getRoot().log(Level.SEVERE, "Failed to find connection", e);
		}
		return new JLabel("No alignment could be computed");
	}

}