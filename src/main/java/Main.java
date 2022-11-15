import breeze.util.ReflectionUtil;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.CollectionSource;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.plugin.hackit.core.tags.DebugTag;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.print("Usage: <platform1>[,<platform2>]* <input file URL>");
            System.exit(1);
        }
        HackitTag debug = new DebugTag();
        WayangContext context = new WayangContext();
        context.register(Java.basicPlugin());
        HackItDataQuanta<String> dataQuanta = new HackItDataQuanta(context);
        Collection tuple =  dataQuanta.loadTextFile(args[0],true)
                .flatMap(x-> Arrays.asList(x.split(" ")),String.class,String.class,null
                        ,null)
                .map(x->new Tuple2<>(x,1),null,String.class, Tuple2.class,null,null)
                .reduceBy(x->x.getField0(),(a,b)->new Tuple2<>(a.getField0().toLowerCase(),a.getField1()+b.getField1())
                        ,String.class,Tuple2.class, null,x->{if(x.getValue().getField1()>10000) x.addPostTag(debug);return x;})
                .collect(Tuple2.class);


            System.out.println(tuple);
     }
}