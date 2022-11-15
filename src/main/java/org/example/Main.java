package org.example;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.Java;
import org.apache.wayang.plugin.hackit.core.Hackit;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        WayangContext context = new WayangContext().with(Java.basicPlugin());
        List<HackitTuple> tuple = Arrays.asList(new HackitTuple(1),new HackitTuple(2),new HackitTuple(3));

        CollectionSource collectionSourceSource = new CollectionSource(tuple,HackitTuple.class,true);
        TransformationDescriptor trans = null;

        MapOperator<Integer,Integer> mapOperator = new MapOperator(new TransformationDescriptor(x->(int)x*2,HackitTuple.class,HackitTuple.class),trans,null);
        List<HackitTuple> results = new ArrayList<>();
        LocalCallbackSink<HackitTuple> sink = LocalCallbackSink.createCollectingSink(results, DataSetType.createDefault(HackitTuple.class));
        collectionSourceSource.connectTo(0,mapOperator,0);
        mapOperator.connectTo(0,sink,0);

        context.execute(new WayangPlan(sink));

        System.out.println(results);

    }
}