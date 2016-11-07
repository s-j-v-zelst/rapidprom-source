package org.rapidprom.parameter;

import org.w3c.dom.Element;

import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.MetaDataChangeListener;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.parameter.MetaDataProvider;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.XMLException;

abstract public class ParameterTypeDynamicString extends ParameterTypeString {

	private static final long serialVersionUID = 7149660883648713942L;

	private final MetaDataProvider metaDataProvider;

	public ParameterTypeDynamicString(final InputPort inputPort, Element element) throws XMLException {
		super(element);
		metaDataProvider = createMetaDataProvider(inputPort);
	}

	public ParameterTypeDynamicString(final InputPort inputPort, String key, String description, boolean optional) {
		super(key, description, optional);
		metaDataProvider = createMetaDataProvider(inputPort);
	}

	public ParameterTypeDynamicString(final InputPort inputPort, String key, String description, boolean optional,
			boolean expert) {
		super(key, description, optional, expert);
		metaDataProvider = createMetaDataProvider(inputPort);
	}

	public ParameterTypeDynamicString(final InputPort inputPort, String key, String description, String defaultValue,
			boolean expert) {
		super(key, description, defaultValue, expert);
		metaDataProvider = createMetaDataProvider(inputPort);
	}

	public ParameterTypeDynamicString(final InputPort inputPort, String key, String description, String defaultValue) {
		super(key, description, defaultValue);
		metaDataProvider = createMetaDataProvider(inputPort);
	}

	public ParameterTypeDynamicString(final InputPort inputPort, String key, String description) {
		super(key, description);
		metaDataProvider = createMetaDataProvider(inputPort);
	}

	protected abstract String updateValue();

	private static MetaDataProvider createMetaDataProvider(final InputPort inputPort) {
		return new MetaDataProvider() {

			@Override
			public void addMetaDataChangeListener(MetaDataChangeListener l) {
				inputPort.registerMetaDataChangeListener(l);
			}

			@Override
			public MetaData getMetaData() {
				if (inputPort != null) {
					return inputPort.getMetaData();
				} else {
					return null;
				}
			}

			@Override
			public void removeMetaDataChangeListener(MetaDataChangeListener l) {
				inputPort.removeMetaDataChangeListener(l);
			}

		};
	}

	@Override
	public Object getDefaultValue() {
		setDefaultValue(updateValue());
		return super.getDefaultValue();
	}

	protected MetaDataProvider getMetaDataProvider() {
		return metaDataProvider;
	}

}