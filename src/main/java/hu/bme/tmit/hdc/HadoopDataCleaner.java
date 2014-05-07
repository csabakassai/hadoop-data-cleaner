
package hu.bme.tmit.hdc;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

import lombok.Getter;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class HadoopDataCleaner {
	
	private static final String SPARK_HOST = "local";
	private static final String SPARK_HOME = "/home/cskassai/Dropbox/Egyetem/Onlab/spark/spark-0.9.0-incubating";
	private static final Logger log = LoggerFactory.getLogger( HadoopDataCleaner.class );
	
	public static void main( String[] args ) {
		
		JavaSparkContext sc = new JavaSparkContext( SPARK_HOST, "Simple App",
			SPARK_HOME, new String[] { "target/hdc-0.0.1-SNAPSHOT.jar" } );
		Scanner in = new Scanner( System.in );
		System.out.println( "Inputfile:" );
		String fileName = in.nextLine();
		if (StringUtils.isBlank( fileName )) {
			fileName = "/home/cskassai/Downloads/phm-collection-tsv.csv";
			fileName = "/home/cskassai/Egyetem/Önlab/Dokumentáció/keruletek.csv";
		}
		
		JavaRDD<String> data = sc.textFile( fileName ).cache();
		int method = 0;
		while (method < 1 || method > 2) {
			System.out.println( "(1) Clustering" );
			System.out.println( "(2) Transform" );
			method = in.nextInt();
		}
		
		switch (method) {
			case 1:
				int algoritm = 0;
				while (algoritm < 1 || algoritm > 8) {
					System.out.println( "(1) Fingerprinting" );
					System.out.println( "(2) NGramFingerPrint" );
					System.out.println( "(3) Metaphone" );
					System.out.println( "(4) Metaphone3" );
					System.out.println( "(5) DoubleMetaphone" );
					System.out.println( "(6) Soundex" );
					System.out.println( "(7) ColognePhonetic" );
					System.out.println( "(8) Kerulet keyer" );
					algoritm = in.nextInt();
				}
				final Keyer keyer;
				switch (algoritm) {
					case 1:
						keyer = new FingerprintKeyer();
						break;
					case 2:
						keyer = new NGramFingerprintKeyer();
						break;
					case 3:
						keyer = new MetaphoneKeyer();
						break;
					case 4:
						keyer = new Metaphone3Keyer();
						break;
					case 5:
						keyer = new DoubleMetaphoneKeyer();
						break;
					case 6:
						keyer = new SoundexKeyer();
						break;
					case 7:
						keyer = new ColognePhoneticKeyer();
						break;
					case 8:
						keyer = new KeruletKeyer();
						break;
					default:
						throw new UnsupportedOperationException( "Unsupported algorithm: " + algoritm );
				}
				
				JavaPairRDD<String, FingeringCluster> javaPairRDD = data.map( new PairFunction<String, String, FingeringCluster>() {
					
					@Override
					public Tuple2<String, FingeringCluster> call( String arg0 ) throws Exception {
						
						return new Tuple2<String, FingeringCluster>( keyer.key( arg0 ), new FingeringCluster( arg0 ) );
					}
				} );
				
				JavaPairRDD<String, FingeringCluster> reduceByKey = javaPairRDD
					.reduceByKey( new Function2<FingeringCluster, FingeringCluster, FingeringCluster>() {
						
						@Override
						public FingeringCluster call( FingeringCluster arg0, FingeringCluster arg1 ) throws Exception {
							
							return arg1.merge( arg0 );
						}
						
					} );
				int klaszterek = reduceByKey.collect().size();
				JavaPairRDD<String, FingeringCluster> filter = reduceByKey
					.filter( new Function<Tuple2<String, FingeringCluster>, Boolean>() {
						
						@Override
						public Boolean call( Tuple2<String, FingeringCluster> arg0 ) throws Exception {
							return arg0._2().elementCountMap.size() > 1;
						}
					} );
				
				JavaPairRDD<String, FingeringCluster> sortByKey = filter.sortByKey();
				
				JavaRDD<FingeringCluster> values = sortByKey.values();
				
				List<Tuple2<String, FingeringCluster>> collect = sortByKey.collect();
				String outputFile = in.next();
				values.saveAsTextFile( outputFile );
				int cserelt = 0;
				for (Tuple2<String, FingeringCluster> elem : collect) {
					
					FingeringCluster fingeringCluster = elem._2();
					Set<Entry<String, Integer>> entrySet = fingeringCluster.elementCountMap.entrySet();
					for (Entry<String, Integer> entry : entrySet) {
						if (!entry.getKey().equals( fingeringCluster.getDominantElement() )) {
							cserelt = cserelt + entry.getValue();
						}
					}
					System.out.println( fingeringCluster );
				}
				
				System.out.println( "Klaszterek szama: " + klaszterek );
				System.out.println( "Cserélt: " + cserelt );
				
				break;
			
			case 3:
				
				List<double[]> list = Lists.newArrayList();
				list.add( new double[] { 0 } );
				list.add( new double[] { 1 } );
				list.add( new double[] { 2 } );
				
				list.add( new double[] { 6 } );
				list.add( new double[] { 7 } );
				list.add( new double[] { 8 } );
				
				list.add( new double[] { 11 } );
				list.add( new double[] { 12 } );
				list.add( new double[] { 13 } );
				
				JavaRDD<double[]> parallelize = sc.parallelize( list );
				KMeansModel kMeansModel = KMeans.train( parallelize.rdd(), 3, 9 );
				double[][] clusterCenters = kMeansModel.clusterCenters();
				
				for (double[] ds : clusterCenters) {
					for (double d : ds) {
						System.out.println( d );
					}
				}
				
				int predict = kMeansModel.predict( new double[] { 13.1 } );
				
				System.out.println( predict );
				break;
			case 2:
				System.out.println( "Transformfile:" );
				String transformFileName = "/home/cskassai/Dropbox/Egyetem/Onlab/repository/kerulet/part-00000";
				JavaRDD<String> transformFile = sc.textFile( transformFileName );
				
				List<String> lines = transformFile.collect();
				final Map<String, String> dictionary = Maps.newHashMap();
				
				for (String line : lines) {
					String[] splitted = line.split( "\\|\\|" );
					Preconditions.checkArgument( splitted.length == 2 );
					String dominantElement = splitted[0];
					String[] elements = splitted[1].split( "\\|" );
					for (String element : elements) {
						String[] split = element.split( "#" );
						String from = split[0];
						if (!from.equals( dominantElement )) {
							dictionary.put( from, dominantElement );
						}
					}
					
				}
				for (Entry<String, String> entry : dictionary.entrySet()) {
					System.out.println( entry.getKey() + "->" + entry.getValue() );
				}
				
				JavaRDD<String> result = data.map( new Function<String, String>() {
					
					@Override
					public String call( String arg0 ) throws Exception {
						String result = arg0;
						if (dictionary.containsKey( arg0 )) {
							result = dictionary.get( arg0 );
						}
						return result;
					}
				} );
				
				result.saveAsTextFile( "result" );
				break;
			
			default:
				throw new UnsupportedOperationException( "Unsupported method: " + method );
				
		}
		
	}
	
	private static class FingeringCluster implements Serializable {
		
		@Getter
		private final Map<String, Integer> elementCountMap = Maps.newTreeMap();
		
		public FingeringCluster( String element ) {
			addElement( element );
		}
		
		public void addElement( String element ) {
			Integer count = this.elementCountMap.get( element );
			if (count == null) {
				count = 0;
			}
			
			this.elementCountMap.put( element, count + 1 );
		}
		
		public FingeringCluster merge( FingeringCluster fingeringCluster ) {
			for (Entry<String, Integer> entry : fingeringCluster.elementCountMap.entrySet()) {
				Integer integer = this.elementCountMap.get( entry.getKey() );
				if (integer == null) {
					this.elementCountMap.put( entry.getKey(), entry.getValue() );
				}
				else {
					Preconditions.checkNotNull( entry );
					Preconditions.checkNotNull( entry.getValue() );
					this.elementCountMap.put( entry.getKey(), integer + entry.getValue() );
				}
			}
			return this;
		}
		
		@Override
		public String toString() {
			StringBuilder stringBuilder = new StringBuilder();
			String dominantElement = getDominantElement();
			stringBuilder.append( dominantElement + "|" );
			for (Entry<String, Integer> entry : this.elementCountMap.entrySet()) {
				stringBuilder.append( "|" );
				stringBuilder.append( entry.getKey() );
				stringBuilder.append( "#" );
				stringBuilder.append( entry.getValue() );
			}
			return stringBuilder.toString();
		}
		
		public String getDominantElement() {
			int max = 0;
			String maxElement = null;
			for (Entry<String, Integer> entry : this.elementCountMap.entrySet()) {
				if (entry.getValue() > max) {
					maxElement = entry.getKey();
					max = entry.getValue();
				}
			}
			
			return maxElement;
		}
		
	}
	
}
