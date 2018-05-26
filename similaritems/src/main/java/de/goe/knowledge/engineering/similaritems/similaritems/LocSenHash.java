package de.goe.knowledge.engineering.similaritems.similaritems;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.neighborhood.LocalitySensitiveHashSearch;
import org.apache.mahout.math.random.WeightedThing;

public class LocSenHash {
	Map<Integer, Vector> vectors;
	List<Vector> data = new ArrayList<Vector>();

	public LocSenHash(Map<Integer, Vector> finalVectorMap) {
		this.vectors = finalVectorMap;
		createVectorList();
	}

	public void createVectorList() {
		Iterator<?> it = vectors.entrySet().iterator();
		while (it.hasNext()) {
			Entry<?, ?> pair = (Entry<?, ?>) it.next();
			data.add((DenseVector) pair.getValue());
		}
	}

	public void executeLSH(DistanceMeasure dist, Map<Integer, Integer> map, String source) throws SQLException, ClassNotFoundException, IOException, InterruptedException {
		LocalitySensitiveHashSearch lsh = new LocalitySensitiveHashSearch(dist, 3);
		lsh.addAll(data);

		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/cos", "monetdb", "monetdb");
		Statement stat = con.createStatement();
		String dataset = source + "_LSH_" + dist.getClass().getSimpleName();
		String query = "CREATE TABLE " + dataset + " (id_1 int, id_2 int, priority double)";
		try {
			stat.execute(query);
		} catch (SQLException e) {
			System.out.println(query + ": " + e.getMessage());
			String dropQuery = "DROP TABLE " + dataset;
			stat.execute(dropQuery);
			stat.close();
			System.out.println("Restart " + dataset);
			System.exit(1);
		}
		con.close();

		Writer writer = null;
		long startTime = System.nanoTime();
		
		int i = 0;
		int counter = 1;
		boolean isNew = true;

		Iterator<?> it = vectors.entrySet().iterator();
		while (it.hasNext()) {
			if (isNew) {
				writer = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream("output/" + dataset + "_" + counter + ".csv"), "utf-8"));
				isNew = false;
				counter++;
			}
			
			Entry<?, ?> entry = (Entry<?, ?>) it.next();
			List<WeightedThing<Vector>> results = lsh.search((Vector) entry.getValue(), 160);
			int priority = 0;

			for (WeightedThing<Vector> result : results) {
				DenseVector v = (DenseVector) result.getValue();

				int vectorKey = map.get(entry.getKey());
				int vectorComparedWith = map
						.get(Integer.parseInt(getKey(vectors, v).toString().replaceAll("\\p{P}", "")));

				if (vectorKey != vectorComparedWith) {
					writer.write(vectorKey + "," + vectorComparedWith + "," + priority + "\n");
				}
				priority++;
			}
			if (i == 10000) {
				writer.close();
				DistancesThread t = new DistancesThread(startTime, dataset, counter - 1, "cos");
				t.start();
				t.join();
				i = 0;
				isNew = true;
			}
			i++;
		}
		long stopTime = System.nanoTime();
		long duration = stopTime - startTime;
		final double minutes = ((double) duration * 0.0000000000166667);
		
		System.out.println("For " + dataset + " it took " + new DecimalFormat("#.##########").format(minutes) + " min");
	}

	public static <T, E> Set<Object> getKey(Map<T, E> map, E value) {
		return map.entrySet().stream().filter(entry -> Objects.equals(entry.getValue(), value)).map(Map.Entry::getKey)
				.collect(Collectors.toSet());
	}
}
