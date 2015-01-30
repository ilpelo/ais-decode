package org.pelizzari;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import dk.dma.ais.message.AisMessage;
import dk.dma.ais.message.AisPositionMessage;
import dk.dma.ais.message.AisStaticCommon;
import dk.dma.ais.reader.AisReader;
import dk.dma.ais.reader.AisReaders;
import dk.dma.enav.util.function.Consumer;
import dk.dma.enav.model.geometry.Position;

public class AisDecode {

	public static BufferedWriter posFileWriter;
	public static BufferedWriter shipTypeFileWriter;
	// public static String outputFileName = "c:\\master_data\\ais_output.txt";
	public static int msgCounter = 0;

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err
					.println("Usage: java AisDecode ais_message_file pos_output_csv_file shiptype_output_csv_file");
			System.exit(1);
		}

		String inputFileName = args[0];
		String posOutputFileName = args[1];
		String shipTypeOutputFileName = args[2];
		System.out.println("Input: " + inputFileName + " Pos Output: "
				+ posOutputFileName + " ShipType Output: " + posOutputFileName);

		try {
			posFileWriter = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(posOutputFileName), "utf-8"));
		} catch (IOException ex) {
			System.err.println("Cannot open: " + posOutputFileName);
		}
		try {
			shipTypeFileWriter = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(shipTypeOutputFileName), "utf-8"));
		} catch (IOException ex) {
			System.err.println("Cannot open: " + shipTypeOutputFileName);
		}
		FileInputStream dataFile = new FileInputStream(inputFileName);
		// "C:\\Users\\andrea\\Desktop\\ANSData_RawDBaisSat 11 Apr 2014 ML22959.dat");
		AisReader reader = AisReaders.createReaderFromInputStream(dataFile);
		// AisReader reader =
		// AisReaders.createReaderFromInputStream(Main.class.getResourceAsStream("/ais-data.txt"));
		reader.registerHandler(new Consumer<AisMessage>() {
			@Override
			public void accept(AisMessage aisMessage) {
				//String aisMsg = aisMessage.getClass().getName();
				//System.out.println("Msg: " + aisMsg);
				String outputRecord = "";
				int mmsi = aisMessage.getUserId();
				outputRecord = outputRecord + mmsi;
				if (aisMessage instanceof AisPositionMessage) {
					String tsUnixEpoch = aisMessage.getVdm().getCommentBlock()
							.getString("c");
					outputRecord = outputRecord + "," + tsUnixEpoch;
					Position pos = aisMessage.getValidPosition();
					if (pos != null) {
						double lat = aisMessage.getValidPosition()
								.getLatitude();
						outputRecord = outputRecord + "," + String.valueOf(lat);
						double lon = aisMessage.getValidPosition()
								.getLongitude();
						outputRecord = outputRecord + "," + String.valueOf(lon);
						try {
							posFileWriter.write(outputRecord);
							posFileWriter.newLine();
						} catch (IOException e) {
							System.err.println("Cannot write " + outputRecord
									+ " to pos output file"); 
						}
					}
				} else if (aisMessage instanceof AisStaticCommon) {
					AisStaticCommon aisStatic = (AisStaticCommon) aisMessage;
					int shipType = aisStatic.getShipType();
					outputRecord = outputRecord + "," + shipType;
					try {
						shipTypeFileWriter.write(outputRecord);
						shipTypeFileWriter.newLine();
					} catch (IOException e) {
						System.err.println("Cannot write " + outputRecord
								+ " to ship type output file");
					}					
				}

				if (msgCounter % 1000 == 0) {
					System.out.println("Counter: " + msgCounter);
				}
				msgCounter += 1;

				// System.out.println("message id: " + aisMessage.getMsgId());
				// System.out.println("user id: " + aisMessage.getUserId());
				// System.out.println("coord: " +
				// aisMessage.getValidPosition());
				// System.out.println("targetType: " +
				// aisMessage.getTargetType());
				// System.out.println("ts: " +
				// aisMessage.getVdm().getCommentBlock().getString("c"));

			}
		});
		reader.start();
		reader.join();
		System.out.println("Total messages: " + msgCounter);

		try {
			posFileWriter.close();
		} catch (Exception ex) {
			System.err.println("Cannot close: " + posOutputFileName);
		}
		try {
			shipTypeFileWriter.close();
		} catch (Exception ex) {
			System.err.println("Cannot close: " + shipTypeOutputFileName);
		}
	}

}
