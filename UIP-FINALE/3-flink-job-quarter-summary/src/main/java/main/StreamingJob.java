
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
 * Flink Streaming Job calculate netvalue sum for Quartal
 *
 */

public class StreamingJob {
    private static String postgresHost, postgresDB, postgresUser, postgresPassword;
    private static String inputTable, outputTable;


    static HashMap<String , Year> yearSum = new HashMap<>();
    static HashMap<String , Year> tsc = new HashMap<>();
    static HashMap<String , Year> peaks = new HashMap<>();
    public static void main(String[] args) throws Exception {
        // Parameters for postgres
        final ParameterTool[] parameterTool = {ParameterTool.fromArgs(args)};

        postgresHost = parameterTool[0].get("postgres-host", "postgres:5432");
        postgresDB = parameterTool[0].get("postgres-db", "kafka_connect");
        postgresUser = parameterTool[0].get("postgres-user", "kafka_connect");
        postgresPassword = parameterTool[0].get("postgres-password", "kafka_connect");
        inputTable = parameterTool[0].get("input-table", "transaction_data_month_summary");
        outputTable = parameterTool[0].get("output-table", "transaction_data_quarter_summary");

        // Create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set input source
        DataStreamSource<Row> inputData = env.createInput(StreamingJob.createJDBCSource());
        SingleOutputStreamOperator<Row> out = inputData.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {
                return value.getField(5).toString().equals("Database");
            }
        });
        ////*****************************************************************
        //Calculate sum of netvalue for Basic Forecast Quartal
        SingleOutputStreamOperator<Row> output = inputData.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                String netvalue = value.getField(1).toString();
                BigDecimal bd=  new BigDecimal(netvalue);
                String[] dat = value.getField(0).toString().split("");
                String jahr = dat[0]+dat[1]+dat[2]+dat[3];
                String month = dat[4] + dat[5];
                int monat = Integer.parseInt(dat[4] + dat[5]);



                if(value.getField(4).toString().equals("Basic Forecast")){
                    if (!yearSum.containsKey(jahr)) {
                        yearSum.put(jahr, new Year(jahr, bd));
                        yearSum.get(jahr).updateMonate(month, bd);
                    } else {
                        yearSum.get(jahr).updateMonate(month, bd);
                        value.setField(1, yearSum.get(jahr).getNetValue()[StreamingJob.getQuartal(monat)].toString());

                    }
                    value.setField(4, "Basic Forecast Quartal");
                }

                return value;
            }
        }).forceNonParallel().disableChaining();

        SingleOutputStreamOperator<Row> output2 = output.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                String[] dat = value.getField(0).toString().split("");
                String jahr = dat[0]+dat[1]+dat[2]+dat[3];
                String month = dat[4] + dat[5];
                int monat = Integer.parseInt(dat[4] + dat[5]);
                String net = value.getField(1).toString();
                if(!net.equals(yearSum.get(jahr).getNetValue()[StreamingJob.getQuartal(monat)].toString())) {
                    for (int i = 0; i <value.getArity() ; i++) {
                        if(!value.getField(i).equals(null))
                            value.setField(i,"");
                    }
                }
                value.setField(0,jahr+month);
                return value;
            }
        }).forceNonParallel().disableChaining();
//
        SingleOutputStreamOperator<Row> output3=output2.
//                filter 12 Monaten

        filter(new FilterFunction<Row>() {
    @Override
    public boolean filter(Row value) {
        return !value.getField(1).toString().equals("");
    }
}).forceNonParallel().disableChaining();

        //****************************************************
//Calculate Netvalue for TSC Quartal
        SingleOutputStreamOperator<Row> outputTSC = inputData.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                String netvalue = value.getField(1).toString();
                BigDecimal bd=  new BigDecimal(netvalue);
                String[] dat = value.getField(0).toString().split("");
                String jahr = dat[0]+dat[1]+dat[2]+dat[3];
                String month = dat[4] + dat[5];
                int monat = Integer.parseInt(dat[4] + dat[5]);

                if(value.getField(4).toString().equals("TSC")){
                    if (!tsc.containsKey(jahr)) {
                        tsc.put(jahr, new Year(jahr, bd));
                        tsc.get(jahr).updateMonate(month, bd);
                    } else {
                        tsc.get(jahr).updateMonate(month, bd);
                        value.setField(1, tsc.get(jahr).getNetValue()[StreamingJob.getQuartal(monat)].toString());
                    }
                    value.setField(4, "TSC Quartal");
                }
                return value;
            }
        }).forceNonParallel().disableChaining();

        SingleOutputStreamOperator<Row> outputTSC2 = outputTSC.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                String[] dat = value.getField(0).toString().split("");
                String jahr = dat[0]+dat[1]+dat[2]+dat[3];
                String month = dat[4] + dat[5];
                int monat = Integer.parseInt(dat[4] + dat[5]);
                String net = value.getField(1).toString();
                if(!net.equals(tsc.get(jahr).getNetValue()[StreamingJob.getQuartal(monat)].toString())) {
                    for (int i = 0; i <value.getArity() ; i++) {
                        if(!value.getField(i).equals(null))
                            value.setField(i,"");
                    }
                }
                value.setField(0,jahr+month);
                return value;
            }
        }).forceNonParallel().disableChaining();


        SingleOutputStreamOperator<Row> outputTSC3=outputTSC2.filter(new FilterFunction<Row>() {
            //                filter TSC Quartal
    @Override
    public boolean filter(Row value) {

        return value.getField(4).toString().equals("TSC Quartal");
    }
}).forceNonParallel().disableChaining();
//
//        //****************************************************
        SingleOutputStreamOperator<Row> outputPeak = inputData.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                String netvalue = value.getField(1).toString();
                BigDecimal bd=  new BigDecimal(netvalue);
                String[] dat = value.getField(0).toString().split("");
                String jahr = dat[0]+dat[1]+dat[2]+dat[3];
                String month = dat[4] + dat[5];
                int monat = Integer.parseInt(dat[4] + dat[5]);

                if(value.getField(4).toString().equals("PEAK")){
                    if (!peaks.containsKey(jahr)) {
                        peaks.put(jahr, new Year(jahr, bd));
                        peaks.get(jahr).updateMonate(month, bd);
                    } else {
                        peaks.get(jahr).updateMonate(month, bd);
                        value.setField(1, peaks.get(jahr).getNetValue()[StreamingJob.getQuartal(monat)].toString());

                    }
                    value.setField(4, "PEAK Quartal");
                }

                return value;
            }
        }).forceNonParallel().disableChaining();

        SingleOutputStreamOperator<Row> outputPeak2 = outputPeak.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                String[] dat = value.getField(0).toString().split("");
                String jahr = dat[0]+dat[1]+dat[2]+dat[3];
                String month = dat[4] + dat[5];
                int monat = Integer.parseInt(dat[4] + dat[5]);
                String net = value.getField(1).toString();
                if(!net.equals(peaks.get(jahr).getNetValue()[StreamingJob.getQuartal(monat)].toString())) {
                    for (int i = 0; i <value.getArity() ; i++) {
                        if(!value.getField(i).equals(null))
                            value.setField(i,"");
                    }
                }
                value.setField(0,jahr+month);
                return value;
            }
        }).forceNonParallel().disableChaining();

        SingleOutputStreamOperator<Row> outputPeak3=outputPeak2.filter(new FilterFunction<Row>() {
             @Override
    //                filter 12 Monaten
    public boolean filter(Row value) {

        return value.getField(4).toString().equals("PEAK Quartal");
    }
}).forceNonParallel().disableChaining();
        //****************************************************


        //****************************************************
        ////*****************************************************************
        //Calculate sum of netvalue for Database Quartal
        SingleOutputStreamOperator<Row> outputPortfolio = inputData.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                String[] dat = value.getField(0).toString().split("");
                int monat = Integer.parseInt(dat[4] + dat[5]);

            if(value.getField(5).toString().equals("Database")) {
                if (monat != 3 && monat != 6 && monat != 9 && monat != 12) {
                     for (int i = 0; i < value.getArity(); i++) {
                         if (!value.getField(i).equals(null))
                value.setField(i, "");
        }
    } else {
        value.setField(5, "Database Quartal");
    }
}
                return value;
            }
        }).disableChaining();


        SingleOutputStreamOperator<Row> outputPortfolio2=outputPortfolio.
//                filter 12 Monaten

        filter(new FilterFunction<Row>() {
    @Override
    public boolean filter(Row value) {
        return !value.getField(1).toString().equals("");
    }
}).forceNonParallel().disableChaining();

        //Write output to postgres
        out.writeUsingOutputFormat(createJDBCSink());
        output3.writeUsingOutputFormat(createJDBCSink());
        outputTSC3.writeUsingOutputFormat(createJDBCSink());
        outputPeak3.writeUsingOutputFormat(createJDBCSink());
        outputPortfolio2.writeUsingOutputFormat(createJDBCSink());
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

                Types.VARCHAR
        };
        return JDBCOutputFormat.buildJDBCOutputFormat()
                .setDBUrl(String.format("jdbc:postgresql://%s/%s", postgresHost, postgresDB))
                .setDrivername("org.postgresql.Driver")
                .setUsername(postgresUser)
                .setPassword(postgresPassword)
                .setQuery(String.format(
                        //insert data into sac_table
                        " INSERT INTO " + "%s(" +
                                "\"SalesDate\", " +
                                "\"NetValueCalculated\", " +
                                "\"TSC\", " +
                                "\"NetValue >=9000\" ," +
                                "\"ForecastBasis\", " +
                                "\"PortfolioCategory\" " +
                                ") values (?,?,?,?,?,?)", outputTable))
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

                BasicTypeInfo.STRING_TYPE_INFO
        };

        return JDBCInputFormat.buildJDBCInputFormat()
                .setDBUrl(String.format("jdbc:postgresql://%s/%s", postgresHost, postgresDB))
                .setDrivername("org.postgresql.Driver")
                .setUsername(postgresUser)
                .setPassword(postgresPassword)
                .setQuery("SELECT "+
                                "\"SalesDate\", " +
                                "\"NetValueCalculated\", " +
                                "\"TSC\", " +
                                "\"NetValue >=9000\" ," +
                                "\"ForecastBasis\", " +
                                "\"PortfolioCategory\" " +
                                "FROM "+ main.StreamingJob.inputTable
                )
                .setRowTypeInfo(new RowTypeInfo(fieldTypes))
                .finish();
    }

    public static int getQuartal(int i){
        if(i<4) return 0;
        else if (i<7) return 1;
        else if (i<10) return 2;
        else return 3;
    }
}