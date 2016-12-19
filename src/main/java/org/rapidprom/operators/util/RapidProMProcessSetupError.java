package org.rapidprom.operators.util;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.rapidminer.operator.ProcessSetupError;
import com.rapidminer.operator.ports.PortOwner;
import com.rapidminer.operator.ports.quickfix.QuickFix;

public final class RapidProMProcessSetupError implements ProcessSetupError {

	private final Exception exception;
	private final Severity severity;
	private final PortOwner portOwner;

	public RapidProMProcessSetupError(Severity severity, PortOwner portOwner, Exception e) {
		this.severity = severity;
		this.portOwner = portOwner;
		this.exception = e;
	}

	@Override
	public Severity getSeverity() {
		return severity;
	}

	@Override
	public List<? extends QuickFix> getQuickFixes() {
		return ImmutableList.of();
	}

	@Override
	public PortOwner getOwner() {
		return portOwner;
	}

	@Override
	public String getMessage() {
		return exception.getMessage();
	}
}