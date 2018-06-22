package org.rapidprom.ioobjects.streams;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.stream.core.interfaces.XSDataPacket;
import org.processmining.stream.core.interfaces.XSHub;
import org.rapidprom.ioobjects.abstr.AbstractRapidProMIOObject;

public class XSHubIOObject<T1 extends XSDataPacket<?, ?>, T2 extends XSDataPacket<?, ?>>
		extends AbstractRapidProMIOObject<XSHub<T1, T2>> {

	private static final long serialVersionUID = -7203404181727669081L;

	public XSHubIOObject(XSHub<T1, T2> t, PluginContext context) {
		super(t, context);
	}

}
