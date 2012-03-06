package org.cascading.js;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.aggregator.Count;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

import java.util.Properties;

public class BaselineJob {
    public static void main(String[] args) {
        (new BaselineJob()).baselineTest();
    }
    @Test
    public void baselineTest() {
// define source and sink Taps.
        Scheme sourceScheme = new TextLine(new Fields("word"));
        Tap source = new Hfs(sourceScheme, "smallwordlist.txt");

        Scheme sinkScheme = new TextLine(new Fields("word", "count"));
        Tap sink = new Hfs(sinkScheme, "output", SinkMode.REPLACE);

// the 'head' of the pipe assembly
        Pipe assembly = new Pipe("wordcount");
        assembly = new GroupBy( assembly, new Fields( "word" ) );

        // For every Tuple group
        // count the number of occurrences of "word" and store result in
        // a field named "count"
        Aggregator count = new Count( new Fields( "count" ) );
        assembly = new Every( assembly, count );


// initialize app properties, tell Hadoop which jar file to use
        Properties properties = new Properties();

// plan a new Flow from the assembly using the source and sink Taps
// with the above properties
        FlowConnector flowConnector = new FlowConnector(properties);
        Flow flow = flowConnector.connect("word-count", source, sink, assembly);

// execute the flow, block until complete
        flow.complete();

    }
}

