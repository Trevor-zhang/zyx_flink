package single_datastream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * keyby[DataStream -> KeyedStream]
 * 根据指定的key将输入的DataStream[T]数据格式转换为KeyedStream[T]
 * 也就是在数据集中执行partition操作，将相同的Key值的数据放置在相同的分区中。
 *
 * reduce[KeyedStream->DataStream]
 * 类似于MR中Reduce原理，将输入的KeyedStream通过ReduceFuction滚动地进行数据聚合处理
 *
 * aggregations[KeyedStream->DataStream]
 * 聚合算子，对reduce算子进行封装，封装的操作有sum,min，minBy,max,maxBy
 */
public class KeyByAndReduceAndAggregations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Tuple2<String, Integer> tp1 = new Tuple2<>("1",1);
        Tuple2<String, Integer> tp2 = new Tuple2<>("1",2);
        Tuple2<String, Integer> tp3 = new Tuple2<>("2",3);
        Tuple2<String, Integer> tp4 = new Tuple2<>("2",4);

        //输出
        // 4> (1,1)
        //2> (2,3)
        //4> (1,2)
        //2> (2,4)
        //相同的key一起输出

        DataStreamSource<Tuple2<String, Integer>> source = env.fromElements(tp1,tp2,tp3,tp4);
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByStream = source.keyBy(0);
//        keyByStream.print();

        //滚动对第二个字段进行reduce相加求和
        //输出
        //2> (2,3)
        //4> (1,1)
        //4> (1,3)
        //2> (2,7)
        DataStream<Tuple2<String, Integer>> reduce = keyByStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<String, Integer>(t1.f0, t1.f1 + t2.f1);
            }
        });
        //reduce.print();


        //利用aggregations算子对第二个字段求和
        //输出
        // 4> (1,1)
        //4> (1,3)
        // 2> (2,3)
        //2> (2,7)
        //可以看出，计算出来的统计值是将没条记录叠加的结果输出
        DataStream<Tuple2<String, Integer>> sum = keyByStream.sum(1);
        sum.print();



        env.execute();


    }
}
