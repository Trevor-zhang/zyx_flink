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
import java.util.List;

/**
 * FlatMap[DataStream->DataStream]
 * FlatMap之于Map在功能上是包含关系，FlatMap常用于将数据集压扁，类比于行转列的概念
 */
public class FlatMap {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Student> ds = env.addSource(new SourceFromMysql())
                .setParallelism(1)
                .map(new MapFunction<Student, Student>() {
                    @Override
                    public Student map(Student student) throws Exception {
                        student.setAge(student.getAge()+1);
                        return student;
                    }
                });
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

        //输入[Student(id=1, name=zyx01, password=123456, age=19), Student(id=1, name=3935c5ed328d8df3aeb0262e5a05e00c, password=e10adc3949ba59abbe56e057f20f883e, age=19)]
        SingleOutputStreamOperator<Student> flatSource = listDataStreamSource.flatMap(new FlatMapFunction<List<Student>, Student>() {

            @Override
            public void flatMap(List<Student> students, Collector<Student> collector) throws Exception {
                // parallelStream 是一个并行执行的流,它通过默认的ForkJoinPool,可能提高你的多线程任务的速度.
                students.parallelStream().forEach(student -> collector.collect(student));
            }
        });
       flatSource.print();
        //输出
        //2> Student(id=1, name=3935c5ed328d8df3aeb0262e5a05e00c, password=e10adc3949ba59abbe56e057f20f883e, age=19)
        //2> Student(id=1, name=zyx01, password=123456, age=19)
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
