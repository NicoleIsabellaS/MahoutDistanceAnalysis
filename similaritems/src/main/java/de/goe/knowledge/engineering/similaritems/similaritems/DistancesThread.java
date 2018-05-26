package de.goe.knowledge.engineering.similaritems.similaritems;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class DistancesThread extends Thread {
	private String dataset;
	private int counter;
	private String fromDB;

	public DistancesThread(long time, String dataset, int counter, String fromDB) {
		super();
		this.fromDB = fromDB;
		this.dataset = dataset;
		this.counter = counter;
	}

	public void run() {
		try {
			Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
			Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/" + fromDB, "monetdb", "monetdb");
			Statement stmt = con.createStatement();
			String queryInsert = "COPY INTO " + dataset + " FROM '/sarna/eclipse-workspace/similaritems/output/"
					+ dataset + "_" + counter + ".csv' USING DELIMITERS ',','\n','\"';";
			stmt.execute(queryInsert);
			con.close();

			//Files.delete(Paths.get("output/" + dataset + "_" + counter + ".csv"));
			System.out.println("=> DELETED FILE " + dataset + "_" + counter + ".csv");
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
//		} catch (IOException e) {
//			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
