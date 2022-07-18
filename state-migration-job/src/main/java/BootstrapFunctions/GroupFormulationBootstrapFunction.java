package BootstrapFunctions;

import CustomDataTypes.GroupFormulationState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;

public class GroupFormulationBootstrapFunction extends KeyedStateBootstrapFunction<Integer, GroupFormulationState> {

    MapState<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> mappingGroupsToNodes;
    ListState<Tuple8<Integer,String,Integer,Long, Long,Integer,Double[],String>> phyOuters;

    @Override
    public void open(Configuration parameters){
        MapStateDescriptor<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> mapStateDescriptor =
                new MapStateDescriptor<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>>(
                        "mapping",
                        TypeInformation.of(Integer.class),
                        TypeInformation.of(new TypeHint<Tuple2<Tuple3<Long, Integer, Double[]>, Integer>>() {})
                );
        mappingGroupsToNodes = getRuntimeContext().getMapState(mapStateDescriptor);

        ListStateDescriptor<Tuple8<Integer,String,Integer,Long, Long,Integer,Double[],String>> listStateDescriptor =
                new ListStateDescriptor<Tuple8<Integer, String, Integer, Long, Long, Integer, Double[], String>>(
                        "phyOuters",
                        TypeInformation.of(new TypeHint<Tuple8<Integer, String, Integer, Long, Long, Integer, Double[], String>>() {})
                );
        phyOuters = getRuntimeContext().getListState(listStateDescriptor);
    }

    @Override
    public void processElement(GroupFormulationState state, KeyedStateBootstrapFunction<Integer, GroupFormulationState>.Context context) throws Exception {
        mappingGroupsToNodes.putAll(state.mapping);
        phyOuters.update(state.physicalOuters);
    }
}
