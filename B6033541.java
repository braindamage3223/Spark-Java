package classTutorial;

import org.apache.log4j.Level;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.variance;

import java.io.FileWriter;

import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class B6033541 {
	
	private static String PATH = ("/H:/workspace/Spark-SQL-examples/ml-latest"); //$NON-NLS-1$

	static SparkConf conf = new SparkConf().setMaster("local").setAppName("My App"); //$NON-NLS-1$ //$NON-NLS-2$
	static JavaSparkContext sc = new JavaSparkContext(conf);
	static SparkSession spark = SparkSession.builder().appName("Java Spark SQL Coursework") //$NON-NLS-1$
			.config("spark.some.config.option", "some-value").getOrCreate(); //$NON-NLS-1$ //$NON-NLS-2$
	
	static Dataset<Row> ratings, movies;
	static Dataset<Row> moviesDF, genres;
	static Dataset<Row> movieGenresDF, genrePopularityDF, highestRatingsDF, explodedRatingsDF, highestRatingsDF2, highestUserRatingsDF;
	static Dataset<Row> userRatingsCountsDF = null, userGenres = null, ratingsCount = null, finalRatingsCount = null, userGenreRatingCountDF = null;	
	static Dataset<Row> avgRatingDF = null, varianceRatingDF = null, avgVarianceDF = null;

	
	
	public static void step1(){
		 		ratings = spark.read()
				.option("inferSchema", true)
				.option("header", true).option("multLine", true)
				.option("mode", "DROPMALFORMED")
				.csv(PATH+"/ratings.csv");
				ratings.printSchema();
	
	
				movies = spark.read()
				.option("inferSchema", true)
				.option("mode", "DROPMALFORMED")
				.option("header", true).option("multLine", true)
				.csv(PATH+"/movies.csv");
				movies.printSchema();
	}
	public static void step2(){
			moviesDF = movies.selectExpr("movieId", "split(genres, '\\\\|') as result");
			moviesDF.show(50,false);
		
			genres  = moviesDF.withColumn("genres", explode(moviesDF.col("result"))).drop("result");
			genres.show(60, false);
		
			try {
				FileWriter fw=new FileWriter("/H:/workspace/Spark-SQL-examples/ml-latest/movieGenres.csv");  
				fw.write("movieId,genre"+"\n");
			
				for(Row r:genres.collectAsList()){
					fw.write(r.getAs("movieId")+","+r.getAs("genres")+"\n");
				}
					fw.close();    
				} 
				catch(Exception e){  	
				}
			}
	
	public static void step3(){
				movieGenresDF = spark.read()
				.option("inferSchema", true)
				.option("mode", "DROPMALFORMED")
				.option("header", true).option("multLine", true)
				.csv(PATH+"/movieGenres.csv");
		        movieGenresDF.orderBy(col("movieId").desc()).select("movieId", "genre").show(50);
				movieGenresDF.printSchema();
	}
	
	public static void step4(){
			genrePopularityDF = movieGenresDF;
			genrePopularityDF.select("genre").groupBy("genre").count().orderBy(col("count").desc()).show(10);
	}
	
	public static void step5(){
		highestRatingsDF = ratings.join(moviesDF,moviesDF.col("movieId").equalTo(ratings.col("movieId"))).drop("movieId","rating", "timestamp");
				
		explodedRatingsDF = highestRatingsDF.withColumn("genres", explode(highestRatingsDF.col("result"))).drop("result");
		
		highestRatingsDF2 =  explodedRatingsDF.groupBy("userId", "genres").count().orderBy(col("count").desc()).dropDuplicates("genres");
		
		highestUserRatingsDF = highestRatingsDF2.orderBy((col("count").desc())).drop("count");
		highestUserRatingsDF.show(10, false);

	}
	/*
	 * 
	 Dataset<Row> newestTest1 = genrePopularity.join(sort, sort.col("genres").equalTo(genrePopularity.col("genres")))
	 .orderBy(sort.col("count").desc())
	 .drop(genrePopularity.col("count"))
	 .drop(sort.col("genres"))
	 .drop(sort.col("count")).limit(10);
		newestTest1.show(false);
	 * 
	 */
	
	public static void step6(){
		userRatingsCountsDF = ratings.groupBy(col("userId")).count().orderBy(col("count").desc());
		
		userGenres = explodedRatingsDF.groupBy("userId", "genres").count().orderBy(col("count").desc()).dropDuplicates("userId").orderBy(col("count").desc());
		
		ratingsCount = userRatingsCountsDF.join(userGenres, userGenres.col("userId")
				.equalTo(userRatingsCountsDF.col("userId"))).orderBy(userRatingsCountsDF.col("count")
				   .desc()).drop(userGenres.col("userId")).drop(userGenres.col("count"));
		
		finalRatingsCount = ratingsCount.withColumnRenamed("count", "ratingsCount").withColumnRenamed("genres", "mostCommonGenres");

		userGenreRatingCountDF = finalRatingsCount.select("userId","ratingsCount","mostCommonGenres");
		userGenreRatingCountDF.show(10,false);
	}
	
	public static void step7(){
		avgRatingDF = movies.join(ratings, "movieId").groupBy(col("title"),col("movieId")).avg("rating");
        varianceRatingDF = movies.join(ratings, "movieId").groupBy(col("title"),col("movieId")).agg(variance(col("rating")));
        avgVarianceDF = avgRatingDF.join(varianceRatingDF, varianceRatingDF.col("movieId").equalTo(avgRatingDF.col("movieId")),"right_outer");
        avgVarianceDF.select(avgRatingDF.col("movieId"),avgRatingDF.col("title"), avgRatingDF.col("avg(rating)"), varianceRatingDF.col("var_samp(rating)")).orderBy(col("avg(rating)").desc()).drop("title").show(10);
	}
	
	public static void main(String[] args) {
		
		LogManager.getLogger("org").setLevel(Level.ERROR);
				System.out.println("----------------------Step 1---------------------");
				step1();
				System.out.println("----------------------Step 2---------------------");		
				step2();
				System.out.println("----------------------Step 3---------------------");
				step3();
				System.out.println("----------------------Step 4---------------------");
				step4();
				System.out.println("----------------------Step 5---------------------");
				step5();
				System.out.println("----------------------Step 6---------------------");
				step6();
				System.out.println("----------------------Step 7---------------------");
				step7();
								
	
	}

	
}
