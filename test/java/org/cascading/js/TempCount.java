package org.cascading.js;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import java.beans.ConstructorProperties;

/**
 * Class Count is an {@link Aggregator} that calculates the number of items in the current group.
 * </p>
 * Note the resulting value for count is always a long. So any comparisons should be against a long value.
 */
public class TempCount extends BaseOperation<Long[]> implements Aggregator<Long[]>
{
    /** Field COUNT */
    public static final String FIELD_NAME = "count";

    /** Constructor Count creates a new Count instance using the defalt field declaration of name 'count'. */
    public TempCount()
    {
        super( new Fields( FIELD_NAME ) );
    }

    /**
     * Constructor Count creates a new Count instance and returns a field with the given fieldDeclaration name.
     *
     * @param fieldDeclaration of type Fields
     */
    @ConstructorProperties({"fieldDeclaration"})
    public TempCount( Fields fieldDeclaration )
    {
        super( fieldDeclaration ); // allow ANY number of arguments
    }
    private long t0;

    public void start( FlowProcess flowProcess, AggregatorCall<Long[]> aggregatorCall )
    {
        if( aggregatorCall.getContext() == null )
            aggregatorCall.setContext( new Long[]{0L} );
        else
            aggregatorCall.getContext()[ 0 ] = 0L;
    }

    public void aggregate( FlowProcess flowProcess, AggregatorCall<Long[]> aggregatorCall )
    {
        aggregatorCall.getContext()[ 0 ] += 1L;
    }

    public void complete( FlowProcess flowProcess, AggregatorCall<Long[]> aggregatorCall )
    {
        aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

    protected Tuple getResult( AggregatorCall<Long[]> aggregatorCall )
    {
        return new Tuple( (Comparable) aggregatorCall.getContext()[ 0 ] );
    }

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall<Long[]> operationCall ) {
        super.prepare(flowProcess, operationCall);
        t0 = System.currentTimeMillis();
    }

    public void cleanup(cascading.flow.FlowProcess flowProcess, cascading.operation.OperationCall<Long[]> call) {
        System.out.println("Count time: " + (System.currentTimeMillis() - t0));
    }
}

