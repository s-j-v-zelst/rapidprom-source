package org.rapidprom.ioobjectrenderers.padas;

import java.lang.ref.WeakReference;
import java.util.Set;

import javax.swing.JComponent;

import org.processmining.database.metamodel.dapoql.ui.components.DAPOQLResultsPanel;
import org.processmining.openslex.metamodel.SLEXMMStorageMetaModelImpl;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.padas.SLEXMMSubSetIOObject;

public class SLEXMMIOSubSetObjectRenderer extends AbstractRapidProMIOObjectRenderer<SLEXMMSubSetIOObject> {

	private JComponent defaultComponent = null;
	private WeakReference<Set<?>> defaultMM = null;
	
	@Override
	public String getName() {
		return "DAPOQ-Lang Query Result Visualizer";
	}

	@Override
	protected JComponent runVisualization(SLEXMMSubSetIOObject soobject) {

		SLEXMMStorageMetaModelImpl mm = soobject.getArtifact();
		
		if (defaultComponent == null || defaultMM == null
				|| !(mm.equals(defaultMM.get()))) {
			try {
				defaultComponent = new DAPOQLResultsPanel(mm,soobject.getType(),soobject.getResults());
				
				defaultMM = new WeakReference<Set<?>>(soobject.getResults());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return defaultComponent;
		
	}

}