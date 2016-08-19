package org.rapidprom.ioobjectrenderers.padas;

import java.lang.ref.WeakReference;
import javax.swing.JComponent;

import org.processmining.database.metamodel.dapoql.ui.components.MetaModelInspectorPanel;
import org.processmining.openslex.metamodel.SLEXMMStorageMetaModelImpl;
import org.rapidprom.ioobjectrenderers.abstr.AbstractRapidProMIOObjectRenderer;
import org.rapidprom.ioobjects.padas.SLEXMMIOObject;

public class SLEXMMIOObjectRenderer extends AbstractRapidProMIOObjectRenderer<SLEXMMIOObject> {

	private JComponent defaultComponent = null;
	private WeakReference<SLEXMMStorageMetaModelImpl> defaultMM = null;
	
	@Override
	public String getName() {
		return "Meta Model Visualizer";
	}

	@Override
	protected JComponent runVisualization(SLEXMMIOObject ioObject) {

		SLEXMMStorageMetaModelImpl mm = ioObject.getArtifact();
		
		if (defaultComponent == null || defaultMM == null
				|| !(mm.equals(defaultMM.get()))) {
			try {
				defaultComponent = new MetaModelInspectorPanel(mm,true);
				defaultMM = new WeakReference<SLEXMMStorageMetaModelImpl>(mm);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return defaultComponent;

	}

}