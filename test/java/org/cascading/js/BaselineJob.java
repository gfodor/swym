package org.cascading.js;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.junit.Test;

import javax.lang.model.type.NullType;
import java.util.Iterator;
import java.util.Properties;

public class BaselineJob {
    public static void main(String[] args) {
        (new BaselineJob()).baselineTest();
    }
    @Test
    public void baselineTest() {
// define source and sink Taps.
        Scheme sourceScheme = new TextLine(new Fields("word"));
        Tap source = new Hfs(sourceScheme, "tinywordlist.txt");

        Scheme sinkScheme = new TextLine(new Fields("word", "count"));
        Tap sink = new Hfs(sinkScheme, "output", SinkMode.REPLACE);

// the 'head' of the pipe assembly
        Pipe assembly = new Pipe("wordcount");
        assembly = new GroupBy( assembly, new Fields( "word" ) );

        // For every Tuple group
        // count the number of occurrences of "word" and store result in
        // a field named "count"
        //Aggregator count = new TempCount( new Fields( "count" ) );
        assembly = new Every( assembly, new TempBuffer(new Fields("count")));


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

class TempBuffer extends BaseOperation<NullType> implements Buffer<NullType> {
    public TempBuffer(Fields fields) {
        super(fields.size(), fields);
    }

    public void operate(FlowProcess flowProcess, BufferCall<NullType> nullTypeBufferCall) {
        int count = 0;

        Iterator i = nullTypeBufferCall.getArgumentsIterator();
        nullTypeBufferCall.getGroup();

        while (i.hasNext()) {
            i.next();
            count += 1;
        }

        nullTypeBufferCall.getOutputCollector().add(new TupleEntry(new Tuple(count)));
    }

    long t0;

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall<NullType> operationCall ) {
        super.prepare(flowProcess, operationCall);
        t0 = System.currentTimeMillis();
    }

    public void cleanup(cascading.flow.FlowProcess flowProcess, cascading.operation.OperationCall<NullType> call) {
        System.out.println("Count time: " + (System.currentTimeMillis() - t0));
    }
}

