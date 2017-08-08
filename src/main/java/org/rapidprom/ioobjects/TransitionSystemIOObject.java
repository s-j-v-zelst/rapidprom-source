package org.rapidprom.ioobjects;

import org.processmining.framework.plugin.PluginContext;
import org.processmining.plugins.transitionsystem.miner.TSMinerInput;
import org.processmining.plugins.transitionsystem.miner.TSMinerTransitionSystem;
import org.rapidprom.ioobjects.abstr.AbstractRapidProMIOObject;

public class TransitionSystemIOObject extends AbstractRapidProMIOObject<TSMinerTransitionSystem> {


	private static final long serialVersionUID = 7513635369374245933L;
	
	private TSMinerInput settings;

	public TransitionSystemIOObject(TSMinerTransitionSystem t,TSMinerInput settings,
			PluginContext context) {
		super(t, context);
		this.settings = settings;
	}
	
	public TSMinerInput getSettings(){
		return settings;
	}

	
}
