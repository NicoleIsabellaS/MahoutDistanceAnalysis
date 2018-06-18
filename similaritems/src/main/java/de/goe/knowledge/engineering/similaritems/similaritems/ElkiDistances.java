package de.goe.knowledge.engineering.similaritems;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import de.lmu.ifi.dbs.elki.data.NumberVector;
import de.lmu.ifi.dbs.elki.distance.distancefunction.PrimitiveDistanceFunction;
import de.lmu.ifi.dbs.elki.math.linearalgebra.Vector;

public class ElkiDistances<O> {

	Map<Integer, Vector> finalVectorMap = new HashMap<Integer, Vector>();
	Map<Integer, Integer> admissionPatientMap = new HashMap<Integer, Integer>();

	public Map<Integer, Vector> getFinalVectorMap() {
		return finalVectorMap;
	}

	public Map<Integer, Integer> getAdmissionPatientMap() {
		return admissionPatientMap;
	}

	public Map<Integer, Vector> createVector(String database) throws SQLException, ClassNotFoundException {
		// DB connection and fetching
		Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/demo", "monetdb", "monetdb");
		PreparedStatement st = con.prepareStatement("SELECT * FROM " + database + " LIMIT 5");
		ResultSet rs;

		rs = st.executeQuery();
		ResultSetMetaData md = rs.getMetaData();
		int rowNumber = 0;
		int i = 0;

		// Vectors and mapping
		while (rs.next()) {
			rowNumber++;
			double[] vectorValues = new double[md.getColumnCount()];
			Vector patientVector = new Vector(md.getColumnCount());

			patientVector = new Vector(setMIMICVec(rs, vectorValues));
			finalVectorMap.put(rowNumber, patientVector);
			admissionPatientMap.put(rowNumber, rs.getInt("icustay_id"));
		}
		con.close();
		return finalVectorMap;
	}

	public double[] setMIMICVec(ResultSet rs, double[] vectorValues) throws SQLException {
		vectorValues[1] = rs.getDouble("urine_6h");
		vectorValues[2] = rs.getDouble("urine_12h");
		vectorValues[3] = rs.getDouble("urine_18h");
		vectorValues[4] = rs.getDouble("urine_24h");
		vectorValues[5] = rs.getDouble("hr_12h_max");
		vectorValues[6] = rs.getDouble("hr_18h_max");
		vectorValues[7] = rs.getDouble("hr_24h_max");
		vectorValues[8] = rs.getDouble("hr_6h_max");
		vectorValues[9] = rs.getDouble("map_12h_max");
		vectorValues[10] = rs.getDouble("map_18h_max");
		vectorValues[11] = rs.getDouble("map_24h_max");
		vectorValues[12] = rs.getDouble("map_6h_max");
		vectorValues[13] = rs.getDouble("rr_12h_max");
		vectorValues[14] = rs.getDouble("rr_18h_max");
		vectorValues[15] = rs.getDouble("rr_24h_max");
		vectorValues[16] = rs.getDouble("rr_6h_max");
		vectorValues[17] = rs.getDouble("sbp_12h_max");
		vectorValues[18] = rs.getDouble("sbp_18h_max");
		vectorValues[19] = rs.getDouble("sbp_24h_max");
		vectorValues[20] = rs.getDouble("sbp_6h_max");
		vectorValues[21] = rs.getDouble("spo2_12h_max");
		vectorValues[22] = rs.getDouble("spo2_18h_max");
		vectorValues[23] = rs.getDouble("spo2_24h_max");
		vectorValues[24] = rs.getDouble("spo2_6h_max");
		vectorValues[25] = rs.getDouble("temperature_12h_max");
		vectorValues[26] = rs.getDouble("temperature_18h_max");
		vectorValues[27] = rs.getDouble("temperature_24h_max");
		vectorValues[28] = rs.getDouble("temperature_6h_max");
		vectorValues[29] = rs.getDouble("hr_12h_min");
		vectorValues[30] = rs.getDouble("hr_18h_min");
		vectorValues[31] = rs.getDouble("hr_24h_min");
		vectorValues[32] = rs.getDouble("hr_6h_min");
		vectorValues[33] = rs.getDouble("map_12h_min");
		vectorValues[34] = rs.getDouble("map_18h_min");
		vectorValues[35] = rs.getDouble("map_24h_min");
		vectorValues[36] = rs.getDouble("map_6h_min");
		vectorValues[37] = rs.getDouble("rr_12h_min");
		vectorValues[38] = rs.getDouble("rr_18h_min");
		vectorValues[39] = rs.getDouble("rr_24h_min");
		vectorValues[40] = rs.getDouble("rr_6h_min");
		vectorValues[41] = rs.getDouble("sbp_12h_min");
		vectorValues[42] = rs.getDouble("sbp_18h_min");
		vectorValues[43] = rs.getDouble("sbp_24h_min");
		vectorValues[44] = rs.getDouble("sbp_6h_min");
		vectorValues[45] = rs.getDouble("sspo2_12h_min");
		vectorValues[46] = rs.getDouble("spo2_18h_min");
		vectorValues[47] = rs.getDouble("spo2_24h_min");
		vectorValues[48] = rs.getDouble("spo2_6h_min");
		vectorValues[49] = rs.getDouble("temperature_12h_min");
		vectorValues[50] = rs.getDouble("temperature_18h_min");
		vectorValues[51] = rs.getDouble("temperature_24h_min");
		vectorValues[52] = rs.getDouble("temperature_6h_min");
		vectorValues[53] = rs.getDouble("hematocrit_min");
		vectorValues[54] = rs.getDouble("hematocrit_max");
		vectorValues[55] = rs.getDouble("wbc_min");
		vectorValues[56] = rs.getDouble("wbc_max");
		vectorValues[57] = rs.getDouble("glucose_min");
		vectorValues[58] = rs.getDouble("glucose_max");
		vectorValues[59] = rs.getDouble("bicarbonate_min");
		vectorValues[60] = rs.getDouble("bicarbonate_max");
		vectorValues[61] = rs.getDouble("potassium_min");
		vectorValues[62] = rs.getDouble("potassium_max");
		vectorValues[63] = rs.getDouble("sodium_min");
		vectorValues[64] = rs.getDouble("sodium_max");
		vectorValues[65] = rs.getDouble("bun_min");
		vectorValues[66] = rs.getDouble("bun_max");
		vectorValues[67] = rs.getDouble("ccreatinine_min");
		vectorValues[68] = rs.getDouble("creatinine_max");
		vectorValues[69] = rs.getInt("age");
		vectorValues[70] = rs.getInt("gender");
		vectorValues[71] = rs.getDouble("vent");
		vectorValues[72] = rs.getDouble("gcs");
		vectorValues[73] = rs.getInt("icd9_numeric");
		vectorValues[74] = rs.getDouble("vasopressor");
		return vectorValues;
	}

	public Map<Integer, Vector> getChunksFrom(int currentID)
			throws UnsupportedEncodingException, FileNotFoundException, IOException {
		Map<Integer, Vector> chunkingVectors = new HashMap<>();
		int compNumOfVec;
		int id = 1;
		int joinID = 1;

		// Odd/Even data size?
		if ((finalVectorMap.size() & 1) != 0) {
			compNumOfVec = (finalVectorMap.size() - 1) / 2;
		} else {
			compNumOfVec = finalVectorMap.size() / 2;
			if ((currentID % 2) == 0) {
				compNumOfVec--;
			}
		}

		// Assigning vector chunks
		if (currentID <= Math.round((double) finalVectorMap.size() / (double) 2)) {
			// id < finalVectorMap.size() / 2 are from (id + 1) to (id + compNumOfVec)
			joinID = currentID + 1;
			while ((joinID <= (currentID + (finalVectorMap.size() / 2))) && (id <= compNumOfVec)) {
				chunkingVectors.put(joinID, finalVectorMap.get(joinID));
				joinID = joinID + 1;
				id += 1;
			}
		} else if (currentID == finalVectorMap.size()) {
			// last id is from (1) to (finalVectorMap.size() / 2)
			while ((joinID <= finalVectorMap.size() / 2) && (id <= compNumOfVec)) {
				chunkingVectors.put(joinID, finalVectorMap.get(joinID));
				joinID = joinID + 1;
				id += 1;
			}
		} else {
			// id from (1) to (rest of remaining vector size)
			while (((id <= compNumOfVec) && (joinID <= (finalVectorMap.size() / 2
					- (finalVectorMap.size() - currentID) % (finalVectorMap.size() / 2))))) {
				chunkingVectors.put(joinID, finalVectorMap.get(joinID));
				joinID = joinID + 1;
				id += 1;
			}
			joinID = currentID + 1;

			// id from (id + 1) to (end)
			while ((id <= compNumOfVec) && ((joinID > currentID) && (joinID <= finalVectorMap.size()))) {
				chunkingVectors.put(joinID, finalVectorMap.get(joinID));
				joinID = joinID + 1;
				id += 1;
			}
		}
		return chunkingVectors;
	}

	public void calcDistance(PrimitiveDistanceFunction<NumberVector> dist, String data) throws ClassNotFoundException,
			UnsupportedEncodingException, FileNotFoundException, IOException, InterruptedException, SQLException {
		Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/demo", "monetdb", "monetdb");
		Statement stat = con.createStatement();
		String dataset = data + "_elki_" + dist.getClass().getSimpleName();
		String query = "CREATE TABLE " + dataset + " (id_1 int, id_2 int," + dist.getClass().getSimpleName()
				+ " double precision)";
		try {
			stat.execute(query);
		} catch (SQLException e) {
			System.out.println(query + ": " + e.getMessage());
			System.exit(1);
		}
		con.close();

		long startTime = System.nanoTime();
		Iterator<?> dataMatrix = finalVectorMap.entrySet().iterator();
		int currentID = 1;

		Writer writer = null;
		int i = 0;
		int counter = 1;
		boolean isNew = true;

		while (dataMatrix.hasNext()) {
			if (isNew) {
				writer = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream("output/" + dataset + "_" + counter + ".csv"), "utf-8"));
				isNew = false;
				counter++;
			}

			Entry<?, ?> pair = (Entry<?, ?>) dataMatrix.next();
			for (Entry<?, ?> e : getChunksFrom(currentID).entrySet()) {
				writer.write(admissionPatientMap.get(pair.getKey()) + "," + admissionPatientMap.get(e.getKey()) + ","
						+ dist.distance((NumberVector) pair.getValue(), (NumberVector) e.getValue()) + "\n");
			}
			currentID++;
			if (i == 1000000) {
				writer.close();

				DistancesThread t = new DistancesThread(admissionPatientMap, dataset, counter - 1);
				t.start();
				t.join();
				i = 0;
				isNew = true;
			}
			i++;
		}

		// Necessary for last file which might be less then i
		writer.close();
		DistancesThread t = new DistancesThread(admissionPatientMap, dataset, counter - 1);
		t.start();
		t.join();

		long stopTime = System.nanoTime();
		long duration = stopTime - startTime;
		final double minutes = ((double) duration * 0.0000000000166667);

		System.out.println("For " + dataset + " it took " + new DecimalFormat("#.##########").format(minutes) + " min");
	}
}