
package main;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.HashMap;

/**
 * Flink Streaming Job to create customer segmentation table
 *
 */

public class StreamingJob {
    private static String postgresHost, postgresDB, postgresUser, postgresPassword;
    private static String inputTable, outputTable;


static HashMap<String, Customer> customer = new HashMap<>();

    public static void main(String[] args) throws Exception {
        // Parameters for postgres
        final ParameterTool[] parameterTool = {ParameterTool.fromArgs(args)};

        postgresHost = parameterTool[0].get("postgres-host", "postgres:5432");
        postgresDB = parameterTool[0].get("postgres-db", "kafka_connect");
        postgresUser = parameterTool[0].get("postgres-user", "kafka_connect");
        postgresPassword = parameterTool[0].get("postgres-password", "kafka_connect");
        inputTable = parameterTool[0].get("input-table", "transaction_data_sac");
        outputTable = parameterTool[0].get("output-table", "transaction_data_segmentation");

        // Create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set input source
        DataStreamSource<Row> inputData = env.createInput(StreamingJob.createJDBCSource());

        //create customer
        SingleOutputStreamOperator<Row> output = inputData.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {

                BigDecimal accountId = new BigDecimal(value.getField(1).toString());
                double netvalue = Double.parseDouble(value.getField(3).toString());
                String sapRelation = value.getField(5).toString();
                int nT = Integer.parseInt(value.getField(4).toString());
                String emp = value.getField(6).toString();
                String indus = value.getField(7).toString();
                if (!customer.containsKey(accountId.toString())) {
                    customer.put(accountId.toString(), new Customer(accountId, netvalue, sapRelation, nT, emp, indus));
                    customer.get(accountId.toString()).aktulisieren(netvalue,nT);
                }
                else {
                    customer.get(accountId.toString()).aktulisieren(netvalue,nT);
                }
                return value;
            }
        }).forceNonParallel().disableChaining();

        //Remove the unimportant data
        SingleOutputStreamOperator<Row> output1 = output
                .map(new MapFunction<Row, Row>() {
                    @Override
                    public Row map(Row value)  {
                        BigDecimal accountId = new BigDecimal(value.getField(1).toString());

                        if(customer.containsKey(accountId.toString())){
                            if(customer.get(accountId.toString()).getCount() ==1){
                            value.setField(0, customer.get(accountId.toString()).getRating());
                            value.setField(3,""+customer.get(accountId.toString()).getNetvalue());
                            value.setField(2, "1");
                        }
                            else{
                                for (int i = 0; i <value.getArity() ; i++) {
                                    if(!value.getField(i).equals(null))
                                        value.setField(i,"");
                                }
                                if(customer.get(accountId.toString())!=null)customer.get(accountId.toString()).minusCount();
                            }
                        }
                        return value;
                    }
                }).forceNonParallel().disableChaining();

                SingleOutputStreamOperator<Row> outputData=output1.
//                filter customer
                        filter(new FilterFunction<Row>() {
                    @Override
                    public boolean filter(Row value) {
                        return
                                value.getField(0).equals("A")
                        || value.getField(0).equals("B")
                        ||value.getField(0).equals("C");
                    }
                }).disableChaining();

        //Berechnung mit Triple Exponential

        outputData.writeUsingOutputFormat(createJDBCSink());
        // Commit job
        env.execute();
    }

    /**
     * Here is the JDBS Sink. Update fieldTypes and Query to meet your requirements.
     * return
     */
    private static JDBCOutputFormat createJDBCSink() {
        int[] fieldTypes = new int[]{

                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,

                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR
        };


        return JDBCOutputFormat.buildJDBCOutputFormat()
                .setDBUrl(String.format("jdbc:postgresql://%s/%s", StreamingJob.postgresHost, StreamingJob.postgresDB))
                .setDrivername("org.postgresql.Driver")
                .setUsername(StreamingJob.postgresUser)
                .setPassword(StreamingJob.postgresPassword)
                .setQuery(String.format(
                        //insert data into sac_table
                        " INSERT INTO " + "%s(" +
                                "\"Rating\", " +
                                "\"AccountId\", " +
                                "\"NumberOfCustomers\", " +
                                "\"NetValueCalculated\", " +
                                "\"Number of transactions\", " +

                                "\"SAPRelation\", " +
                                "\"NumberOfEmployees\", " +
                                "\"Industry\" " +

                                ") values (?,?,?,?,?,?,?,?" +
                                ")", StreamingJob.outputTable))
                .setSqlTypes(fieldTypes)
                .setBatchInterval(200)
                .finish();
    }

    /**
     * Here is the JDBS Source. Update fieldTypes and Query to meet your requirements.
     * return
     */
    private static JDBCInputFormat createJDBCSource() {

        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {

                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,


                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        };

        return JDBCInputFormat.buildJDBCInputFormat()
                .setDBUrl(String.format("jdbc:postgresql://%s/%s", StreamingJob.postgresHost, StreamingJob.postgresDB))
                .setDrivername("org.postgresql.Driver")
                .setUsername(StreamingJob.postgresUser)
                .setPassword(StreamingJob.postgresPassword)
                .setQuery("SELECT  " +
                                "\"SalesDate\", " +
                                "\"AccountId\", " +
                                "\"SellingMotion\", " +
                                "\"NetValueCalculated\", " +
                                "\"Number of transactions\", " +

                                "\"SAPRelation\", " +
                                "\"NumberOfEmployees\", " +
                                "\"Industry\" " +

                                "FROM " + StreamingJob.inputTable
                        +" WHERE \"AccountId\" NOT LIKE 'A%'" +
                        " And \"AccountId\" NOT LIKE 'D%'" +
                        " and \"AccountId\" NOT LIKE '0' "
                )
                .setRowTypeInfo(new RowTypeInfo(fieldTypes))
                .finish();
    }
}
