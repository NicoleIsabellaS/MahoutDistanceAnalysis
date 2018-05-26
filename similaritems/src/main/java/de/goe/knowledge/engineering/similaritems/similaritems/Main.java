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
		
		// MIMICIII DATASET
		String cos = "cos";
		Distances distancePerformanceMimic = new Distances();
		distancePerformanceMimic.createVector("sys.patients_nicole", cos);

		// System.out.println("STARTING EuclideanDistanceMeasure");
		 //distancePerformanceMimic.calcDistance(new EuclideanDistanceMeasure(),
		 //"sys.patients_nicole", cos);
		
		 //System.out.println("STARTING SquaredEuclideanDistanceMeasure");
		 //distancePerformanceMimic.calcDistance(new SquaredEuclideanDistanceMeasure(),
		 //"sys.patients_nicole",cos);
		
		 //System.out.println(" STARTING ManhattanDistanceMeasure");
		 //distancePerformanceMimic.calcDistance(new ManhattanDistanceMeasure(),"sys.patients_nicole",cos);

		 //System.out.println("STARTING MinkowskiDistanceMeasure");
		 //distancePerformanceMimic.calcDistance(new MinkowskiDistanceMeasure(),"sys.patients_nicole",cos);
		
		 //System.out.println("STARTING CosineDistanceMeasure");
		 //distancePerformanceMimic.calcDistance(new CosineDistanceMeasure(),"sys.patients_nicole",cos);

		// distancePerformanceMimic.calcDistance(new
		// MahalanobisDistanceMeasure(),"sys.patients_nicole");

		 //System.out.println("STARTING TanimotoDistanceMeasure");
		 //distancePerformanceMimic.calcDistance(new TanimotoDistanceMeasure(),"sys.patients_nicole",cos);

		 //System.out.println("STARTING ChebyshevDistanceMeasure");
		 //distancePerformanceMimic.calcDistance(new ChebyshevDistanceMeasure(),"sys.patients_nicole",cos);

		System.out.println("MIMIC STARTING LSH!");

		LocSenHash lshMimic = new LocSenHash(distancePerformanceMimic.getFinalVectorMap());
		lshMimic.executeLSH(new EuclideanDistanceMeasure(), distancePerformanceMimic.getAdmissionPatientMap(), "sys.patients_nicole");
		lshMimic.executeLSH(new SquaredEuclideanDistanceMeasure(),distancePerformanceMimic.getAdmissionPatientMap(),"sys.patients_nicole");
		lshMimic.executeLSH(new ManhattanDistanceMeasure(),distancePerformanceMimic.getAdmissionPatientMap(), "sys.patients_nicole");
		lshMimic.executeLSH(new MinkowskiDistanceMeasure(), distancePerformanceMimic.getAdmissionPatientMap(), "sys.patients_nicole");
		lshMimic.executeLSH(new CosineDistanceMeasure(),distancePerformanceMimic.getAdmissionPatientMap(), "sys.patients_nicole");
		lshMimic.executeLSH(new TanimotoDistanceMeasure(),distancePerformanceMimic.getAdmissionPatientMap(), "sys.patients_nicole");
		lshMimic.executeLSH(new ChebyshevDistanceMeasure(),distancePerformanceMimic.getAdmissionPatientMap(), "sys.patients_nicole");

		// DIABETES DATASET
		//String dia = "diabetes";
		//Distances distancePerformanceDiabetes = new Distances();
		//distancePerformanceDiabetes.createVector("sys.diabetes_nicole", dia);

		//LocSenHash lshDia = new LocSenHash(distancePerformanceDiabetes.getFinalVectorMap());
		//lshDia.executeLSH(new EuclideanDistanceMeasure(), distancePerformanceDiabetes.getAdmissionPatientMap(),"sys.diabetes_nicole");

//
//		distancePerformanceDiabetes.calcDistance(new EuclideanDistanceMeasure(), "diabetes_nicole", dia);
//		distancePerformanceDiabetes.calcDistance(new SquaredEuclideanDistanceMeasure(), "diabetes_nicole", dia);
//		distancePerformanceDiabetes.calcDistance(new ManhattanDistanceMeasure(), "diabetes_nicole", dia);
//		distancePerformanceDiabetes.calcDistance(new MinkowskiDistanceMeasure(), "diabetes_nicole", dia);
//		distancePerformanceDiabetes.calcDistance(new CosineDistanceMeasure(), "diabetes_nicole", dia);
//		// distancePerformanceDiabetes.calcDistance(new MahalanobisDistanceMeasure(),
//		// "monetdb.diabetes");
//		distancePerformanceDiabetes.calcDistance(new TanimotoDistanceMeasure(), "diabetes_nicole", dia);
//		distancePerformanceDiabetes.calcDistance(new ChebyshevDistanceMeasure(), "diabetes_nicole", dia);
	}
}
