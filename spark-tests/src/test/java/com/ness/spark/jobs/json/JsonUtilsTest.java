package com.ness.spark.jobs.json;

import com.ness.spark.jobs.sql.config.QueryJobConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.junit.*;

import java.util.Date;
import java.util.List;

import static com.ness.spark.jobs.sql.config.QueryJobConstants.APP_NAME;
import static com.ness.spark.jobs.sql.config.QueryJobConstants.SPARK_MASTER_URL;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;


/**
 * Created by p3700622 on 07/04/16.
 */
public class JsonUtilsTest {
    static SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
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

    public static final String JSON_FILE = "src/test/resources/Spotify_playlist.txt";


    JsonUtils job = new JsonUtils(sc);
    private DataFrame dataFrame;

    public JsonUtilsTest() {

    }

    @BeforeClass
    public static void init() {
        sc.addJar(QueryJobConstants.CLASSES_JAR);
    }

    @Before
    public void setUp() throws Exception {
        dataFrame = job.getJsonRecords(JSON_FILE);

    }

    @AfterClass
    public static void tearDown() throws Exception {

        sc.close();
    }

    @Ignore
    public void testSplitJsonToRDD() {
        job.splitToRDD(JSON_FILE);

    }

    @Test
    public void testSaveAsParquet() {
        dataFrame.saveAsParquetFile("src/test/resources/out" + new Date().getTime());
    }

    @Ignore
    public void testJsonCorruptRecords() {
        String SELECT_CORRUPT_RECORDS = "SELECT _corrupt_record FROM " + JSON_TABLE_NAME + " WHERE _corrupt_record IS NOT NULL";
        Row[] rows = job.selectSqlContext(SELECT_CORRUPT_RECORDS, JSON_TABLE_NAME).collect();

        assertThat("There are corrupted records", rows.length, lessThan(1));
    }

    @Test
    public void testSelectJson1Level() {
        dataFrame.printSchema();
        dataFrame.schema();
        Integer result = job.selectCountDataFrame(dataFrame, SELECT_ITEM_1);

        assertThat("", result, greaterThan(0));

    }

    @Test
    public void testSelectJson2Level() {
        Integer result = job.selectCountDataFrame(dataFrame, SELECT_ITEM_2);

        assertThat("", result, greaterThan(0));
    }

    @Test
    public void testSelectJson3Level() {
        System.out.println(SELECT_ITEM_3);
        dataFrame.printSchema();
        Integer result = job.selectCountDataFrame(dataFrame, SELECT_ITEM_3);

        assertThat("", result, greaterThan(0));
    }

    @Test
    public void testSelectJson4Level() {
        Integer result = job.selectCountDataFrame(dataFrame, SELECT_ITEM_4);

        assertThat("", result, greaterThan(0));
    }

    @Test
    public void testSelectJson5Level() {
        dataFrame.printSchema();
        Integer result = job.selectCountDataFrame(dataFrame, SELECT_ITEM_5);

        assertThat("", result, greaterThan(0));
    }

    @Test
    public void testSelectJsonSUMDuration() {
        Double result = job.selectSumDataFrame(dataFrame, SELECT_ITEM_SUM);
        assertThat("", result, greaterThan(0D));
    }

    @Test
    public void testTop100Spotify() {
        DataFrame spotifydataFrame = job.selectSqlContext(SELECT_SPOTIFY_PLAYLIST_ID, JSON_TABLE_NAME);
        List<Object> list = spotifydataFrame.sqlContext().sql(SELECT_TOP_100_SPOTIFY).collectAsList().get(0).getList(0);

        assertThat("There should be 100 items in tracks, found  " + list.size(), list.size(), comparesEqualTo(100));
    }

    @Test
    public void testSelectBySpotifyPlaylistId() {
        DataFrame spotifydataFrame = job.selectSqlContext(SELECT_SPOTIFY_PLAYLIST_ID, JSON_TABLE_NAME);
        List<Row> rows = spotifydataFrame.collectAsList();
        System.out.println(rows.get(0));
        assertThat("There should one playlist, found  " + rows.size(), rows.size(), comparesEqualTo(1));
    }



}