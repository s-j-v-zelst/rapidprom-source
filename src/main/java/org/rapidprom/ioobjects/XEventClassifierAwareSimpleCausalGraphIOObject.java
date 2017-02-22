package org.rapidprom.ioobjects;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.models.causalgraph.XEventClassifierAwareSimpleCausalGraph;
import org.rapidprom.ioobjects.abstr.AbstractRapidProMIOObject;

public class XEventClassifierAwareSimpleCausalGraphIOObject
		extends AbstractRapidProMIOObject<XEventClassifierAwareSimpleCausalGraph> {

	private static final long serialVersionUID = 8813652811139369382L;

	public XEventClassifierAwareSimpleCausalGraphIOObject(XEventClassifierAwareSimpleCausalGraph t,
			PluginContext context) {
		super(t, context);
	}

}
