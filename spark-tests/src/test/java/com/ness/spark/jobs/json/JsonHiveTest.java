package com.ness.spark.jobs.json;

import com.ness.spark.jobs.sql.config.QueryJobConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.*;

import static com.ness.spark.jobs.sql.config.QueryJobConstants.APP_NAME;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;


/**
 * Created by p3700622 on 07/04/16.
 */
public class JsonHiveTest {
    static SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
    public static final String SPARK_MASTER_URL =  "spark://127.0.0.1:7077";;
    public static JavaSparkContext sc = new JavaSparkContext(sparkConf.setMaster(SPARK_MASTER_URL));
    public static final String SELECT_ITEM_1 = "tracks";
    private static final String SELECT_ITEM_2 = "tracks.items";
    private static final String SELECT_ITEM_3 = "tracks.items.added_by";
    private static final String SELECT_ITEM_4 = "external_urls";
    private static final String SELECT_ITEM_5 = "external_urls.spotify";
    private static final String SELECT_ITEM_SUM = "tracks.items.track.album.images";
    public static final String PLAYLIST_ID = "0kYVfIHcvrFe6Y6DmVjypv";
    public static final String SELECT_TOP_100_SPOTIFY = "SELECT tracks.items FROM jsonTable WHERE id='" + PLAYLIST_ID + "'";
    public static final String SELECT_SPOTIFY_PLAYLIST_ID = "SELECT * FROM jsonTable WHERE id='" + PLAYLIST_ID + "'";

    public static final String JSON_TABLE_NAME = "jsonTable";

    public static final String JSON_FILE = "/resources/Spotify_playlist.txt";


    JsonUtils job = new JsonUtils(sc);
    private DataFrame dataFrame;

    public JsonHiveTest() {

    }

    @BeforeClass
    public static void init() {
        sc.addJar(QueryJobConstants.CLASSES_JAR);
    }

    @Before
    public void setUp() throws Exception {
        HiveContext hiveContext = new HiveContext(job.ctx);
        HiveContext sqlContext = hiveContext;
        dataFrame = job.getJsonRecords(JSON_FILE,sqlContext);


    }

    @AfterClass
    public static void tearDown() throws Exception {

        sc.close();
    }



    @Ignore
    public void testSelectJson1Level() {
        dataFrame.printSchema();
        dataFrame.schema();
        Integer result = job.selectCountDataFrame(dataFrame, SELECT_ITEM_1);

        assertThat("", result, greaterThan(0));

    }



}