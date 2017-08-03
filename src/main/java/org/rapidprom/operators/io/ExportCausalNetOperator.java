package org.rapidprom.operators.io;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;

import org.processmining.models.cnet.CausalNet;
import org.processmining.models.cnet.exporting.ExportCNet;
import org.rapidprom.ioobjects.CausalNetIOObject;
import org.rapidprom.operators.abstr.AbstractRapidProMExporterOperator;
import org.rapidprom.operators.io.ExportCausalNetOperator.CausalNetFileFormat;

import com.rapidminer.operator.OperatorDescription;

public class ExportCausalNetOperator
		extends AbstractRapidProMExporterOperator<CausalNetIOObject, CausalNet, CausalNetFileFormat> {

	public enum CausalNetFileFormat {
		CNET("cnet");

		private final String format;

		private CausalNetFileFormat(String format) {
			this.format = format;
		}

		@Override
		public String toString() {
			return format;
		}
	}

	public ExportCausalNetOperator(OperatorDescription description) {
		super(description, CausalNetIOObject.class,
				EnumSet.allOf(CausalNetFileFormat.class)
						.toArray(new CausalNetFileFormat[EnumSet.allOf(CausalNetFileFormat.class).size()]),
				CausalNetFileFormat.CNET);
	}

	@Override
	protected void writeToFile(File file, CausalNet object, CausalNetFileFormat format) throws IOException {
		switch (format) {
		case CNET:
		default:
			new ExportCNet().exportCNetToCNetFile(object, file);
			break;
		}
	}

}
