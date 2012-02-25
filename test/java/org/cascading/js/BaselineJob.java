package org.cascading.js;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Function;
import cascading.operation.regex.RegexGenerator;
import cascading.pipe.Each;
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
    @Test
    public void baselineTest() {
// define source and sink Taps.
        Scheme sourceScheme = new TextLine(new Fields("line"));
        Tap source = new Hfs(sourceScheme, "listings.txt");

        Scheme sinkScheme = new TextLine(new Fields("line", "word"));
        Tap sink = new Hfs(sinkScheme, "output", SinkMode.REPLACE);

// the 'head' of the pipe assembly
        Pipe assembly = new Pipe("wordcount");

// For each input Tuple
// parse out each word into a new Tuple with the field name "word"
// regular expressions are optional in Cascading
        String regex = "\\S+";
        Function function = new RegexGenerator(new Fields("word"), regex);
        assembly = new Each(assembly, new Fields("line"), function);

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
