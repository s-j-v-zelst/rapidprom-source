package org.rapidprom.operators.util;

import org.processmining.framework.plugin.Progress;

import com.rapidminer.operator.OperatorProgress;
import com.rapidminer.operator.ProcessStoppedException;

public class RapidProMProgress implements Progress {

	private final OperatorProgress progress;
	private boolean isCancelled = false;

	public RapidProMProgress(OperatorProgress progress) {
		this.progress = progress;
	}

	@Override
	public void cancel() {
		isCancelled = true;
	}

	@Override
	public String getCaption() {
		return "";
	}

	@Override
	public int getMaximum() {
		return progress.getTotal();
	}

	@Override
	public int getMinimum() {
		return 0;
	}

	@Override
	public int getValue() {
		return progress.getProgress();
	}

	@Override
	public void inc() {
		try {
			progress.step();
		} catch (ProcessStoppedException e) {
			isCancelled = true;
		}
	}

	@Override
	public boolean isCancelled() {
		return isCancelled;
	}

	@Override
	public boolean isIndeterminate() {
		return progress.isIndeterminate();
	}

	@Override
	public void setCaption(String arg0) {
	}

	@Override
	public void setIndeterminate(boolean indeterminate) {
		progress.setIndeterminate(indeterminate);
	}

	@Override
	public void setMaximum(int total) {
		progress.setTotal(total);
	}

	@Override
	public void setMinimum(int minimum) {
		// incompatible - assume 0
	}

	@Override
	public void setValue(int value) {
		try {
			progress.setCompleted(value);
		} catch (ProcessStoppedException e) {
			isCancelled = true;
		}
	}

}
