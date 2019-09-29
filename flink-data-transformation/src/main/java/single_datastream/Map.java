package single_datastream;


import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import source.outersource.SourceFromMysql;
import source.outersource.pojo.Student;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * map [DataStream -> DataStream]
 * 调用用户定义的MapFunction对DataStream[T]数据进行处理，形成新的DataStram[T]
 * 其中数据格式可能会发生变化，常用作对数据集内数据的清洗和转换
 * map方法不允许缺少数据，原来多少条数据，处理后依然是多少条数据
 */
public class Map {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         //test1 从集合中创建dataStream,完成对age字段加1的操作
        SingleOutputStreamOperator<Student> ds = env.addSource(new SourceFromMysql())
                .setParallelism(1)
                .map(new MapFunction<Student, Student>() {
                    @Override
                    public Student map(Student student) throws Exception {
                        student.setAge(student.getAge()+1);
                        return student;
                    }
                });
        ds.print();

        //test2   将student 扩展成 List<Student>
        SingleOutputStreamOperator<List<Student>> listDataStreamSource = ds.map(
                student -> {
                    List<Student> list = new ArrayList<>();

                    list.add(student);
                    Student student1 = new Student();
                    student1.setId(student.getId());
                    student1.setName(DigestUtils.md5Hex(student.getName()));
                    student1.setPassword(DigestUtils.md5Hex(student.getPassword()));
                    student1.setAge(student.getAge());

                    list.add(student1);
                    return list;
                }).returns(new ListTypeInfo(Student.class));

        listDataStreamSource.addSink(new PrintSinkFunction<>());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
