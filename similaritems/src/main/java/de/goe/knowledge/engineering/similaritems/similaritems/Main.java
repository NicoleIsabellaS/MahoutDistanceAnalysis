package de.goe.knowledge.engineering.similaritems.similaritems;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.mahout.common.distance.ChebyshevDistanceMeasure;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.MahalanobisDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.common.distance.MinkowskiDistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;

public class Main {
	public static void main(String[] args) throws ClassNotFoundException, SQLException, UnsupportedEncodingException,
			FileNotFoundException, IOException, IllegalAccessException, InstantiationException, InterruptedException {
		// Distances distancePerformanceMimic = new Distances();
		// long startTime = System.nanoTime();
		// distancePerformanceMimic.createVector("MIMICIII.PATIENTS");

		// distancePerformanceMimic.calcDistance(new EuclideanDistanceMeasure(),
		// "mimiciii.patients");
		// long eucStopTime = System.nanoTime();
		// long eucDuration = eucStopTime - startTime;
		// final double eucMinutes = ((double) eucDuration * 0.0000000000166667);
		// System.out.println("Euclidean took " + new
		// DecimalFormat("#.##########").format(eucMinutes) + " min");

		// distancePerformanceMimic.calcDistance(new CosineDistanceMeasure(),
		// "mimiciii.patients");
		// long cosStopTime = System.nanoTime();
		// long cosDuration = cosStopTime - startTime;
		// final double cosMinutes = ((double) cosDuration * 0.0000000000166667);
		// System.out.println("Cosine took " + new
		// DecimalFormat("#.##########").format(cosMinutes) + " min");

		ElkiDistances distancePerformanceElki = new ElkiDistances<>();
		long startTime_ELKI = System.nanoTime();
		distancePerformanceElki.createVector("MIMICIII.PATIENTS");

		distancePerformanceElki.calcDistance(new CosineDistanceFunction(), "mimiciii.patients");
		long cosStopTime_ELKI = System.nanoTime();
		long cosDuration_ELKI = cosStopTime_ELKI - startTime_ELKI;
		final double cosMinutes_ELKI = ((double) cosDuration_ELKI * 0.0000000000166667);
		System.out.println("Cosine took " + new DecimalFormat("#.##########").format(cosMinutes_ELKI) + " min");

		// distancePerformanceElki.calcDistance(new CosineDistanceFunction(),
		// "mimiciii.patients");
		// long eucStopTime_ELKI = System.nanoTime();
		// long eucDuration_ELKI = eucStopTime_ELKI - startTime_ELKI;
		// final double eucMinutes_ELKI = ((double) eucDuration_ELKI *
		// 0.0000000000166667);
		// System.out.println("Euclidean took " + new
		// DecimalFormat("#.##########").format(eucMinutes_ELKI) + " min");
				
				
		// MIMICIII DATASET
		String db = "yourDB";
		Distances distancePerformanceMimic = new Distances();
		distancePerformanceMimic.createVector("sys.patients", db);

		System.out.println("STARTING EuclideanDistanceMeasure");
		distancePerformanceMimic.calcDistance(new EuclideanDistanceMeasure(),"sys.patients_nicole", db);
		
		System.out.println("STARTING SquaredEuclideanDistanceMeasure");
		distancePerformanceMimic.calcDistance(new SquaredEuclideanDistanceMeasure(),"sys.patients_nicole",db);
		
		System.out.println(" STARTING ManhattanDistanceMeasure");
		distancePerformanceMimic.calcDistance(new ManhattanDistanceMeasure(),"sys.patients",db);

		System.out.println("STARTING MinkowskiDistanceMeasure");
		distancePerformanceMimic.calcDistance(new MinkowskiDistanceMeasure(),"sys.patients",db);
		
		System.out.println("STARTING CosineDistanceMeasure");
		distancePerformanceMimic.calcDistance(new CosineDistanceMeasure(),"sys.patients",db);

		System.out.println("STARTING TanimotoDistanceMeasure");
		distancePerformanceMimic.calcDistance(new TanimotoDistanceMeasure(),"sys.patients",db);

		System.out.println("STARTING ChebyshevDistanceMeasure");
		distancePerformanceMimic.calcDistance(new ChebyshevDistanceMeasure(),"sys.patients",db);

		// MIMICIII LSH
		System.out.println("MIMIC STARTING LSH!");

		LocSenHash lshMimic = new LocSenHash(distancePerformanceMimic.getFinalVectorMap());
		lshMimic.executeLSH(new EuclideanDistanceMeasure(), distancePerformanceMimic.getAdmissionPatientMap(), "sys.patients");
		lshMimic.executeLSH(new SquaredEuclideanDistanceMeasure(),distancePerformanceMimic.getAdmissionPatientMap(),"sys.patients");
		lshMimic.executeLSH(new ManhattanDistanceMeasure(),distancePerformanceMimic.getAdmissionPatientMap(), "sys.patients");
		lshMimic.executeLSH(new MinkowskiDistanceMeasure(), distancePerformanceMimic.getAdmissionPatientMap(), "sys.patients");
		lshMimic.executeLSH(new CosineDistanceMeasure(),distancePerformanceMimic.getAdmissionPatientMap(), "sys.patients");
		lshMimic.executeLSH(new TanimotoDistanceMeasure(),distancePerformanceMimic.getAdmissionPatientMap(), "sys.patients");
		lshMimic.executeLSH(new ChebyshevDistanceMeasure(),distancePerformanceMimic.getAdmissionPatientMap(), "sys.patients");

		// DIABETES DATASET
		//String dia = "diabetes";
		//Distances distancePerformanceDiabetes = new Distances();
		//distancePerformanceDiabetes.createVector("sys.diabetes", dia);

		//distancePerformanceDiabetes.calcDistance(new EuclideanDistanceMeasure(), "diabetes", dia);
		//distancePerformanceDiabetes.calcDistance(new SquaredEuclideanDistanceMeasure(), "diabetes", dia);
		//distancePerformanceDiabetes.calcDistance(new ManhattanDistanceMeasure(), "diabetes", dia);
		//distancePerformanceDiabetes.calcDistance(new MinkowskiDistanceMeasure(), "diabetes", dia);
		//distancePerformanceDiabetes.calcDistance(new CosineDistanceMeasure(), "diabetes", dia);
		//distancePerformanceDiabetes.calcDistance(new TanimotoDistanceMeasure(), "diabetes", dia);
		//distancePerformanceDiabetes.calcDistance(new ChebyshevDistanceMeasure(), "diabetes", dia);
	}
}
