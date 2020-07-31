//   Copyright 2020 IBM Corporation
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
package ilog.opl.externaldata.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.OldExcelFormatException;
import org.apache.poi.poifs.filesystem.OfficeXmlFileException;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Name;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import ilog.concert.IloException;
import ilog.opl.IloOplModel;
import ilog.opl.IloOplTupleSchemaDefinition;
import ilog.opl.dbsupport.DataBaseDataHandler;
import ilog.opl.dbsupport.DataBaseDataHandler.ConnectionInfo;
import ilog.opl.externaldata.DataConnection;
import ilog.opl.externaldata.InputRowIterator;
import ilog.opl.externaldata.OutputRowIterator;
import ilog.opl.externaldata.TupleIO;

/** Data connection that is backed up by an Excel workbook.
 * The code here has the following limitations:
 * - A cell can only be filled from one contiguous, rectangular range. Input
 *   from disconnected or non-rectangular ranges is not supported.
 * - Data is always read in row-major fashion.
 * To overcome the row major limitation you can append <code>^t</code> or
 * <code>^T</code> to the range. This will transpose the range before processing
 * it so that you will actually get a column major order. This is only supported
 * when reading data.
 */
public class ExcelConnection implements DataConnection {
	/** Write back data to disk, depending on Excel format type. */
	private interface WriteBack {
		public void write() throws IOException;
	}

	/** The file that contains the workbook. */
	private final File file;
	/** The workbook read from the file. */
	private Workbook wb;
	/** How to write back data to the workbook (depends on the Excel version). */
	private WriteBack writeBack;
	/** Create a new connection with the specified filename.
	 * @param filename The name of the file that contains the workbook.
	 * @param write <code>true</code> if the connection is opened for writing. In this
	 *              case the constructor will create the file if it does not yet exist.
	 * @throws IOException If the workbook cannot be created from the file.
	 */
	public ExcelConnection(String filename, boolean write) throws IOException {
		file = new File(filename);

		if (write && !file.exists()) {
			// For output create the file if it does not yet exist.
			// Note that this does not create any sheets, so sheet names cannot be used,
			// there will only be the default sheet.
			final XSSFWorkbook xssfWb = new XSSFWorkbook();
			xssfWb.createSheet();
			final FileOutputStream empty = new FileOutputStream(file);
			xssfWb.write(empty);
			empty.close();
		}

		POIFSFileSystem fs = null;
		try {
			// Both the POIFSFileSystem and the HSSFWorkbook constructor may raise
			// an exception in case the Excel format does not match.
			fs = new POIFSFileSystem(file);
			final HSSFWorkbook hssfWb = new HSSFWorkbook(fs);
			wb = hssfWb;
			writeBack = new WriteBack() {
				@Override
				public void write() throws IOException {
					hssfWb.write();
				}
			};
			fs = null; // mark success for finally block
			return;
		}
		catch (OldExcelFormatException e) {
			// This exception is expected, we ignore it and try to open other format below.
			System.out.println(filename + " looks like an old Excel format file, trying different driver");
		}
		catch (OfficeXmlFileException e) {
			// This exception is expected, we ignore it and try to open other format below.
			System.out.println(filename + " looks like an XML Excel file, trying different driver");
		}
		finally {
			if (fs != null)
				fs.close();
		}

		// Opening with the above format failed, so attempt to open with
		// another format.
		try {
			final XSSFWorkbook xssfWb = new XSSFWorkbook(file);
			wb = xssfWb;
			writeBack = new WriteBack() {
				@Override
				public void write() throws IOException {
					File backup = null;
					if (file.exists()) {
						// Overwriting an existing file throws
						//  org.apache.poi.ooxml.POIXMLException: java.io.EOFException: Unexpected end of ZLIB input stream
						// So we first move the existing file out of the way and then create a new file.
						// If writing the new file fails then we move back the original file.
						final File old = new File(file.getAbsolutePath());
						backup = new File(old.getAbsolutePath() + ".bck");
						old.renameTo(backup);
					}
					final FileOutputStream fos = new FileOutputStream(file);
					try {
						xssfWb.write(fos);
						backup.delete();
						backup = null;
					}
					finally {
						fos.close();
						if (backup != null)
							backup.renameTo(file);
					}
				} 
			};
		}
		catch (InvalidFormatException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			throw new IOException(e);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			throw e;
		}
	}

	/** A parsed address specification. */
	private static final class AddressInfo {
		public final Sheet sheet;
		public final CellRangeAddress cells;
		public AddressInfo(Sheet sheet, CellRangeAddress cells) {
			super();
			this.sheet = sheet;
			this.cells = cells;
		}
	}

	/** Decode an address specification.
	 * It is assumed that <code>addr</code> specifies a contiguous and rectangular range on a single sheet.
	 * @param addr The address to decode.
	 * @return The decoded address information.
	 */
	private AddressInfo decode(String addr) {
		int defaultSheetIndex = wb.getActiveSheetIndex();
		int sheetIndex = -1;
		String range = null;

		// Handle named ranges.
		Name name = wb.getName(addr);
		if (name != null) {
			if (name.getSheetIndex() >= 0) {
				// If the name is defined only for a single sheet then this sheet
				// becomes the default sheet.
				defaultSheetIndex = name.getSheetIndex();
			}
			addr = name.getRefersToFormula();
		}

		// Sheet and range specification are separated by "!"
		final String[] fields = addr.split("!", 2);
		if (fields.length == 2) {
			// The sheet name may be wrapped in quotes.
			if (fields[0].startsWith("'") && fields[0].endsWith("'"))
				fields[0] = fields[0].substring(1, fields[0].length() - 1);
			else if (fields[0].startsWith("\"") && fields[0].endsWith("\""))
				fields[0] = fields[0].substring(1, fields[0].length() - 1);
			sheetIndex = wb.getSheetIndex(fields[0]);
			range = fields[1];
		}
		else {
			sheetIndex = defaultSheetIndex;
			range = fields[0];
		}

		return new AddressInfo(wb.getSheetAt(sheetIndex), CellRangeAddress.valueOf(range));
	}

	/** Base class for iterators.
	 * Stores the sheet and the dimensions of the rectangle from which data is read
	 * or to which data is written.
	 */
	private static class IteratorBase {
		protected Sheet sheet;
		protected final int firstRow;
		protected final int lastRow;
		protected final int firstCol;
		protected final int lastCol;
		public IteratorBase(AddressInfo addr) {
			this.sheet = addr.sheet;
			this.firstRow = addr.cells.getFirstRow();
			this.lastRow = addr.cells.getLastRow();
			this.firstCol = addr.cells.getFirstColumn();
			this.lastCol = addr.cells.getLastColumn();
		}

		/** Maps a zero-based relative column index to an absolute index.
		 * @param index The relative index to map.
		 * @return The mapped absolute index.
		 * @throws IOException if the index is out of bounds.
		 */
		protected final int mapIndex(int index) throws IOException {
			final int idx = firstCol + index;
			if (idx < firstCol || idx > lastCol)
				throw new IOException("index " + index + " is out of range [" + firstCol + ", " + lastCol + "]");
			return idx;
		}
	}

	private static final class InputIterator extends IteratorBase implements InputRowIterator {
		private Row currentRow;
		private int currentIndex;
		public InputIterator(AddressInfo addr) {
			super(addr);
			this.currentRow = null;
			this.currentIndex = firstRow - 1;
		}

		@Override
		public int getInt(int index) throws IOException {
			return (int)Math.round(getDouble(index));
		}
		@Override
		public double getDouble(int index) throws IOException {
			return currentRow.getCell(mapIndex(index)).getNumericCellValue();
		}
		@Override
		public String getString(int index) throws IOException {
			return currentRow.getCell(mapIndex(index)).getRichStringCellValue().getString();
		}
		@Override
		public boolean next() throws IOException {
			if (currentIndex > lastRow)
				return false;
			++currentIndex;
			if (currentIndex <= lastRow) {
				currentRow = sheet.getRow(currentIndex);
				return true;
			}
			else {
				currentRow = null;
				return false;
			}
		}
		@Override
		public int getColumnCount() throws IOException {
			return 1 + lastCol - firstCol;
		}
		@Override
		public void close() throws IOException {
			currentRow = null;
			sheet = null;
		}
		@Override
		public TupleIO makeTupleIO(IloOplTupleSchemaDefinition schema) throws IOException {
			try {
				return new TupleIO(schema);
			}
			catch (IloException e) {
				throw new IOException(e);
			}
		}
	}

	/** Iterate over a transposed range. */
	private static final class TransposeInputIterator extends IteratorBase implements InputRowIterator {
		private int currentCol = -1;
		public TransposeInputIterator(AddressInfo addr) {
			super(addr);
		}
		/** Maps a zero-based relative row index to an absolute index.
		 * @param index The relative index to map.
		 * @return The mapped absolute index.
		 * @throws IOException if the index is out of bounds.
		 */
		private int mapRow(int index) throws IOException {
			final int idx = firstRow + index;
			if (idx < firstRow || idx > lastRow)
				throw new IOException("index " + index + " is out of range [" + firstRow + ", " + lastRow + "]");
			return idx;
		}
		@Override
		public int getInt(int index) throws IOException {
			return (int)Math.round(getDouble(index));
		}
		@Override
		public double getDouble(int index) throws IOException {
			return sheet.getRow(mapRow(index)).getCell(currentCol).getNumericCellValue();
		}
		@Override
		public String getString(int index) throws IOException {
			return sheet.getRow(mapRow(index)).getCell(currentCol).getRichStringCellValue().getString();
		}
		@Override
		public boolean next() throws IOException {
			if (currentCol > lastCol)
				return false;
			if (currentCol < 0) {
				currentCol = firstCol;
			}
			else {
				++currentCol;
			}
			return currentCol <= lastCol;
		}
		@Override
		public int getColumnCount() throws IOException {
			return 1 + lastRow - firstRow;
		}
		@Override
		public void close() throws IOException {
			sheet = null;
		}
		@Override
		public TupleIO makeTupleIO(IloOplTupleSchemaDefinition schema) throws IOException {
			try {
				return new TupleIO(schema);
			}
			catch (IloException e) {
				throw new IOException(e);
			}
		}
	}

	@Override
	public InputRowIterator openInputRows(String command) throws IOException {
		if (command.endsWith("^t") || command.endsWith("^T"))
			return new TransposeInputIterator(decode(command.substring(0, command.length() - 2)));
		else
			return new InputIterator(decode(command));
	}

	private static final class OutputIterator extends IteratorBase implements OutputRowIterator {
		private WriteBack writeBack;
		private Row currentRow;
		private int currentIndex;
		public OutputIterator(AddressInfo addr, WriteBack writer) {
			/** TODO: If a sheet does not exist in the workbook then we could create it. */
			super(addr);
			writeBack = writer;
			currentRow = sheet.getRow(firstRow);
			if (currentRow == null)
				currentRow = sheet.createRow(firstRow);
			currentIndex = firstRow;
		}
		/** Get reference to a cell.
		 * Will create the cell if it does not yet exist.
		 * @param index Relative cell index (0-based).
		 * @return Reference to the specified cell.
		 * @throws IOException if the cell does not exist and cannot be created.
		 */
		private Cell getCell(int index) throws IOException {
			final int idx = mapIndex(index);
			final Cell cell = currentRow.getCell(idx);
			if (cell != null)
				return cell;
			return currentRow.createCell(idx);
		}
		@Override
		public void setInt(int index, int value) throws IOException {
			setDouble(index, value);
		}
		@Override
		public void setDouble(int index, double value) throws IOException {			
			getCell(index).setCellValue(value);
		}
		@Override
		public void setString(int index, String value) throws IOException {
			getCell(index).setCellValue(value);
		}
		@Override
		public void completeRow() throws IOException {
			++currentIndex;
			if (currentIndex > lastRow)
				currentRow = null;
			else {
				currentRow = sheet.getRow(currentIndex);
				if (currentRow == null)
					currentRow = sheet.createRow(currentIndex);
			}
		}
		@Override
		public void commit() throws IOException {
			/** TODO: This is questionable: if the workbook is huge we write it every time! */
			writeBack.write();
		}
		@Override
		public void close() throws IOException {
			// We don't write the workbook here since we may be closed due to an error.
			// the book is written in function commit()
			sheet = null;
			currentRow = null;
			currentIndex = -1;
		}
	}


	@Override
	public OutputRowIterator openOutputRows(String command) throws IOException {
		return new OutputIterator(decode(command), writeBack);
	}
	@Override
	public void close() throws IOException {
		try {
			if (wb != null)
				wb.close();
		}
		finally {
			wb = null;
		}
	}

	/** Convenience function to create and register data handlers.
	 * This is intended to be called from a .dat file's <code>prepare</code> section
	 * and provides an easy way to add Excel support to a .dat file. Another typical
	 * situation to call this is right after creating an <code>IloOplModel</code>
	 * instance.
	 * @param prefix The prefix for *Connection, *Read, *Publish statements.
	 * @param model The model to which the new syntax is added.
	 */
	public static void register(String prefix, IloOplModel model) {
		System.err.println("Registering " + prefix);
		new DataBaseDataHandler(prefix, model, new DataBaseDataHandler.ConnectionFactory() {
			@Override
			public DataConnection newConnection(ConnectionInfo info, boolean write) throws IOException {
				return new ExcelConnection(info.connstr, write);
			}
		});
		System.err.println("Prefix " + prefix + " registered for Excel");
	}
}
