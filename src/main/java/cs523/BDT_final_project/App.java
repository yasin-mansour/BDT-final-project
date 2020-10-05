package cs523.BDT_final_project;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class App 
{
    public static void main( String[] args )
    {
 
  	  String consumerKey = "NfFp19WYY71KJmP5mRZaPochO";
  	  String consumerSecret = "fYm0BNXsLPvHmF18WzkDpZlOevZwbU4JjrYd2Xhcs0AKm4Zlx9";
  	  String accessToken = "1310352799553581056-bxHtElFEGgxjDe1XMhydzLMifniYPw";
  	  String accessTokenSecret = "HUK3kNC6sCnvV5xFmSDcZI1bowcBb2gKfj2FkXiDg97Ng";
  	    String[] filters = new String[]{};

  	    System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
  	    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
  	    System.setProperty("twitter4j.oauth.accessToken", accessToken);
  	    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
  	    System.setProperty("spark.master", "local");
  	  

  	    SparkConf sparkConf = new SparkConf().setAppName("yasin-spark-streaming").setMaster("local[*]").set( "parquet.block.size", "1kB" );
  	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));
  	    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, filters);
  	    
  	    
  	    JavaDStream<String> tweetWords = stream.flatMap(new FlatMapFunction<Status, String>() {
  	        @Override
  	        public Iterator<String> call(Status s) {
  	          return Arrays.asList(s.getText().split(" ")).iterator();
  	        }
  	      });
  	    
  	    JavaDStream<String> hashTags = tweetWords.filter(new Function<String, Boolean>(){
  	    	  @Override 
  	    	  public Boolean call(String s){
  	    		  return s.startsWith("@");
  	    	  }
  	    });
  	    
  	    JavaPairDStream<String, Integer> hashTagCount = hashTags.mapToPair(
  	    	      new PairFunction<String, String, Integer>() {
  	    	        @Override
  	    	        public Tuple2<String, Integer> call(String s) {
  	    	          return new Tuple2<String, Integer>(s.substring(1), 1);
  	    	        }
  	    });
  	    
  	    JavaPairDStream<String, Integer> hashTagTotals = hashTagCount.reduceByKeyAndWindow(
  	    	      new Function2<Integer, Integer, Integer>() {
  	    	        @Override
  	    	        public Integer call(Integer a, Integer b) {
  	    	          return a + b;
  	    	        }
  	    }, new Duration(60000));
  	    
  	    JavaDStream<String> rows = hashTagTotals.map(new Function<Tuple2<String, Integer>, String>(){
  	    	@Override
  	        public String call(Tuple2 t) {
  	          return t._1 +"," + t._2;
  	        }
  	    });
  	    
  	    //hashTagTotals.print();
  	    rows.dstream().saveAsTextFiles("hdfs://127.0.0.1/tmp/finalProject/createdat=","") ;
  	    jssc.start();
  	    try {
  	    	jssc.awaitTermination();
  	    } catch(InterruptedException i){
  	    	
  	    }
    }
}
