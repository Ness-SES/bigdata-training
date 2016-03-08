package test.com.ness.bigdata.training.mapreduce;

import com.ness.bigdata.training.mapreduce.WordCount;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.testutil.ExtendedAssert;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestWordCount {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    private final List<Pair> expectedMapperResults = Arrays.asList(
            new Pair[]{new Pair(new Text("test"), new IntWritable(1)),
                    new Pair(new Text("123"), new IntWritable(1)),
                    new Pair(new Text("inca"), new IntWritable(1)),
                    new Pair(new Text("un"), new IntWritable(1)),
                    new Pair(new Text("test"), new IntWritable(1))
            });
    private final List<Pair> expectedMapperEmptyResults = Arrays.asList(
            new Pair[]{
                    new Pair(new Text("1q"), new IntWritable(1)),
                    new Pair(new Text("1q"), new IntWritable(1))
            });

    private List<Pair> expectedReducerResults = Arrays.asList(new Pair[]{
            new Pair(new Text("test"), new IntWritable(2))
    });

    private List<Pair> expectedMapReduceResults = Arrays.asList(
            new Pair[]{new Pair(new Text("123"), new IntWritable(1)),
                    new Pair(new Text("inca"), new IntWritable(1)),
                    new Pair(new Text("test"), new IntWritable(2)),
                    new Pair(new Text("un"), new IntWritable(1))
            });

    private List<Pair> expectedWrongMapReduceResults = Arrays.asList(
            new Pair[]{
                    new Pair(new Text("1q"), new IntWritable(2)),
                    new Pair(new Text("-2"), new IntWritable(2)),
                    new Pair(new Text("21-44dd"), new IntWritable(1)),
                    new Pair(new Text("pingpong"), new IntWritable(1)),
                    new Pair(new Text("ping-pong"), new IntWritable(1)),
                    new Pair(new Text("32-"), new IntWritable(1)),
                    new Pair(new Text("qqq233w"), new IntWritable(1))
            });

    @Before
    public void setup() {
        WordCount.WordMapper mapper = new WordCount.WordMapper();
        WordCount.WordReducer reducer = new WordCount.WordReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new IntWritable(), new Text("test 123 inca un test"));
        mapDriver.withOutput(new Text("test"), new IntWritable(1));
        mapDriver.withOutput(new Text("123"), new IntWritable(1));
        mapDriver.withOutput(new Text("inca"), new IntWritable(1));
        mapDriver.withOutput(new Text("un"), new IntWritable(1));
        mapDriver.withOutput(new Text("test"), new IntWritable(1));
        mapDriver.runTest(false);
        ExtendedAssert.assertListEquals(expectedMapperResults, mapDriver.getExpectedOutputs());
    }

    @Test
    public void testWrongMapper() {
        mapDriver.withInput(new IntWritable(), new Text("; ; ; 7; 1q 1;q@! 1q"));
        mapDriver.withOutput(new Text("1q"), new IntWritable(1));
        mapDriver.withOutput(new Text("1q"), new IntWritable(1));
        mapDriver.runTest(false);
        ExtendedAssert.assertListEquals(expectedMapperEmptyResults, mapDriver.getExpectedOutputs());
        Assert.assertEquals(5, mapDriver.getCounters().findCounter(WordCount.WordMapper.WordMapperEnum.NoWordValue).getValue());
    }

    @Test
    public void testReducer() {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("test"), values);
        reduceDriver.withOutput(new Text("test"), new IntWritable(2));
        reduceDriver.runTest(false);
        ExtendedAssert.assertListEquals(expectedReducerResults, reduceDriver.getExpectedOutputs());
    }

    @Test
    public void testMapReduce() {
        mapReduceDriver.withInput(new IntWritable(), new Text("test 123 inca un test"));
        mapReduceDriver.withOutput(new Text("123"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("inca"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("test"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("un"), new IntWritable(1));
        mapReduceDriver.runTest(false);
        ExtendedAssert.assertListEquals(expectedMapReduceResults, mapReduceDriver.getExpectedOutputs());
    }

    @Test
    public void testWrongMapReduce() {
        Configuration config = new Configuration();
        config.set(WordCount.WordMapper.CONFIG_REGEX_KEY, "(.*)(-+?)(.*)");
        mapReduceDriver.setConfiguration(config);
        mapReduceDriver.withInput(new IntWritable(), new Text("; ; ; 7; 1q 1;q@! 1q -2   21-44dd -2 pingpong ping-pong 32- qqq233w"));
        mapReduceDriver.withOutput(new Text("1q"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("-2"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("21-44dd"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("pingpong"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("ping-pong"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("32-"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("qqq233w"), new IntWritable(1));
        mapReduceDriver.runTest(false);
        ExtendedAssert.assertListEquals(expectedWrongMapReduceResults, mapReduceDriver.getExpectedOutputs());
        Assert.assertEquals(5, mapReduceDriver.getCounters().findCounter(WordCount.WordMapper.WordMapperEnum.NoWordValue).getValue());
        Assert.assertEquals(9, mapReduceDriver.getCounters().findCounter(WordCount.WordReducer.WordReducerEnum.TOTAL_WORDS).getValue());
        Assert.assertEquals(7, mapReduceDriver.getCounters().findCounter(WordCount.WordReducer.WordReducerEnum.UNIQUE_WORDS).getValue());
        Assert.assertEquals(4, mapReduceDriver.getCounters().findCounter(WordCount.WordReducer.WordReducerEnum.DASH_WORDS).getValue());
        Assert.assertEquals(5, mapReduceDriver.getCounters().findCounter(WordCount.WordReducer.WordReducerEnum.WITH_NUMBERS_WORDS).getValue());
        Assert.assertEquals(2, mapReduceDriver.getCounters().findCounter(WordCount.WordReducer.WordReducerEnum.WITHOUT_NUMBERS_WORDS).getValue());
    }
}
