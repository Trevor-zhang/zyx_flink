package single_datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Filter[DataStream->DataStream]
 * 按照条件对输入数据集筛选
 */
public class Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3);
        //筛选出大于2的数
        SingleOutputStreamOperator<Integer> filter = source.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer num) throws Exception {
                if (num > 2) {
                    return true;
                }
                return false;
            }
        });
        filter.print();
        env.execute();


    }
}
