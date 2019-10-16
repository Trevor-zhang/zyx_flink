package source.outersource;

import common.util.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import source.outersource.pojo.Student;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 自定义source,从mysql读取数据
 */
public class SourceFromMysql extends RichSourceFunction<Student> {
    PreparedStatement  ps;
    private Connection connection;

    /**
     * open()方法中建立连接，这样不用每次invoke的时候都要建立连接、释放连接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MysqlUtil.getConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.0.147:3306/flink_demo?useUnicode=true&characterEncoding=UTF-8",
                "root",
                "123456");
        String sql = "select * from student " ;
        ps = this.connection.prepareStatement(sql);
    }

    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection !=null){
            connection.close();
        }
        if (ps != null){
             ps.close();
        }
    }

    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()){
            Student student = new Student(resultSet.getInt("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("password").trim(),
                    resultSet.getInt("age"));

            sourceContext.collect(student);
        }
    }

    @Override
    public void cancel() {

    }

    /**
     * 测试类
     * @param args
     */
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Student> dataStreamSource = env.addSource(new SourceFromMysql());

        dataStreamSource.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
