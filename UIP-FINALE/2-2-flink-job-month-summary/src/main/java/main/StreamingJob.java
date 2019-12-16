
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
 * Flink Streaming Job to calculate the summerize from NetValue
 *
 */

public class StreamingJob {
    private static String postgresHost, postgresDB, postgresUser, postgresPassword;
    private static String inputTable, outputTable;

    static HashMap<String , Year> yearSum = new HashMap<>();
    static HashMap<String , Year> tsc = new HashMap<>();
    static HashMap<String , Year> peaks = new HashMap<>();
    static HashMap<String , Year> portfolio = new HashMap<>();
    public static void main(String[] args) throws Exception {
        // Parameters for postgres
        final ParameterTool[] parameterTool = {ParameterTool.fromArgs(args)};

        postgresHost = parameterTool[0].get("postgres-host", "postgres:5432");
        postgresDB = parameterTool[0].get("postgres-db", "kafka_connect");
        postgresUser = parameterTool[0].get("postgres-user", "kafka_connect");
        postgresPassword = parameterTool[0].get("postgres-password", "kafka_connect");
        inputTable = parameterTool[0].get("input-table", "transaction_data_sac");
        outputTable = parameterTool[0].get("output-table", "transaction_data_month_summary");

        // Create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set input source
        DataStreamSource<Row> inputData = env.createInput(StreamingJob.createJDBCSource());

        SingleOutputStreamOperator<Row> output = inputData.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                String year = value.getField(0).toString();
                String netvalue = value.getField(1).toString();
                BigDecimal bd=  new BigDecimal(netvalue);
                String[] dat = value.getField(0).toString().split("\\.");
                if(value.getField(3).toString().equals("False") && value.getField(2).toString().equals("False")){
                if (!yearSum.containsKey(dat[2])) {
                    yearSum.put(dat[2], new Year(year, bd));
                    yearSum.get(dat[2]).updateMonate(dat[1], bd);
                } else {
                    yearSum.get(dat[2]).updateMonate(dat[1], bd);
                    value.setField(1, yearSum.get(dat[2]).getNetValue()[Integer.parseInt(dat[1]) - 1].toString());
                    value.setField(4, "Basic Forecast");
                }
                }
                value.setField(5,"");
                return value;
            }
        }).forceNonParallel().disableChaining();



        SingleOutputStreamOperator<Row> output2 = output.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                String[] dat = value.getField(0).toString().split("\\.");
                String net = value.getField(1).toString();
                if(!net.equals(yearSum.get(dat[2]).getNetValue()[Integer.parseInt(dat[1])-1].toString())) {
                    for (int i = 0; i <value.getArity() ; i++) {
                        if(!value.getField(i).equals(null))
                            value.setField(i,"");
                    }
                }
                value.setField(0,dat[2]+dat[1]);
                return value;
            }
        }).forceNonParallel().disableChaining();

        SingleOutputStreamOperator<Row> output3=output2.
//                filter 12 Monaten

        filter(new FilterFunction<Row>() {
    @Override
    public boolean filter(Row value) {
//                //1732850.5600000003, 3255278.970000104, 4794814.370000202 ,6376056.930000274
        return !value.getField(1).toString().equals("");
    }
}).forceNonParallel().disableChaining();

        SingleOutputStreamOperator<Row> output4 = output3.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row value){
                double d = Double.parseDouble(value.getField(1).toString());
                return d > 400000;
            }
        }).disableChaining();



        //****************************************************

        SingleOutputStreamOperator<Row> outputPeak = inputData.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) {
                String year = value.getField(0).toString();
                String netvalue = value.getField(1).toString();
                BigDecimal bd=  new BigDecimal(netvalue);
                String[] dat = value.getField(0).toString().split("\\.");

                if(value.getField(3).toString().equals("True")){
                if (!peaks.containsKey(dat[2])) {
                    peaks.put(dat[2], new Year(year, bd));
                    peaks.get(dat[2]).updateMonate(dat[1], bd);
                    value.setField(1, peaks.get(dat[2]).getNetValue()[Integer.parseInt(dat[1]) - 1].toString());
                    value.setField(4, "PEAK");
                } else {
                    peaks.get(dat[2]).updateMonate(dat[1], bd);
                    value.setField(1, peaks.get(dat[2]).getNetValue()[Integer.parseInt(dat[1]) - 1].toString());
                    value.setField(4, "PEAK");
                }}
                value.setField(5,"");
                return value;
            }
        }).forceNonParallel().disableChaining();

        SingleOutputStreamOperator<Row> outputPeak2 = outputPeak.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) {
                String[] dat = value.getField(0).toString().split("\\.");
                String net = value.getField(1).toString();
                if(peaks.get(dat[2])!=null){
                if(!net.equals(peaks.get(dat[2]).getNetValue()[Integer.parseInt(dat[1])-1].toString())) {
                    for (int i = 0; i <value.getArity() ; i++) {
                        if(!value.getField(i).equals(null))
                            value.setField(i,"");
                    }
                }}
                value.setField(0,dat[2]+dat[1]);
                return value;
            }
        }).forceNonParallel().disableChaining();
//
        SingleOutputStreamOperator<Row> outputPeak3=outputPeak2.
//                filter 12 Monaten

        filter(new FilterFunction<Row>() {
    @Override
    public boolean filter(Row value) {
//                //1732850.5600000003, 3255278.970000104, 4794814.370000202 ,6376056.930000274
        String[] dat = value.getField(0).toString().split("");
        String jahr = dat[0]+dat[1]+dat[2]+dat[3];
        String month = dat[4] + dat[5];
        return value.getField(4).toString().equals("PEAK") ||
                (value.getField(4).toString().equals("PEAK") &&value.getField(0).toString().equals("201802") && !value.getField(1).toString().equals("9000") );
    }
}).forceNonParallel().disableChaining();

        //****************************************************
        SingleOutputStreamOperator<Row> outputTSC = inputData.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) {
                String year = value.getField(0).toString();
                String netvalue = value.getField(1).toString();
                BigDecimal bd=  new BigDecimal(netvalue);
                String[] dat = value.getField(0).toString().split("\\.");

                if(value.getField(2).toString().equals("True")){
                    if (!tsc.containsKey(dat[2])) {
//
                        tsc.put(dat[2], new Year(year, bd));
                        tsc.get(dat[2]).updateMonate(dat[1], bd);
                        value.setField(1, tsc.get(dat[2]).getNetValue()[Integer.parseInt(dat[1]) - 1].toString());
                        value.setField(4, "TSC");
                    } else {
                        tsc.get(dat[2]).updateMonate(dat[1], bd);
                        value.setField(1, tsc.get(dat[2]).getNetValue()[Integer.parseInt(dat[1]) - 1].toString());
                        value.setField(4, "TSC");
                    }}
                value.setField(5,"");
                return value;
            }
        }).forceNonParallel().disableChaining();

        SingleOutputStreamOperator<Row> outputTSC2 = outputTSC.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                String[] dat = value.getField(0).toString().split("\\.");
                String net = value.getField(1).toString();
                if(tsc.get(dat[2])!=null){
                    if(!net.equals(tsc.get(dat[2]).getNetValue()[Integer.parseInt(dat[1])-1].toString())) {
                        for (int i = 0; i <value.getArity() ; i++) {
                            if(!value.getField(i).equals(null))
                                value.setField(i,"");
                        }
                    }}
                value.setField(0,dat[2]+dat[1]);
                return value;
            }
        }).forceNonParallel().disableChaining();
//
        SingleOutputStreamOperator<Row> outputTSC3=outputTSC2.
//                filter 12 Monaten

        filter(new FilterFunction<Row>() {
    @Override
    public boolean filter(Row value) {
//                //1732850.5600000003, 3255278.970000104, 4794814.370000202 ,6376056.930000274
        return value.getField(4).toString().equals("TSC");
    }
}).forceNonParallel().disableChaining();

        //****************************************************



        //PORTFOLIO
        //****************************************************
        SingleOutputStreamOperator<Row> outputPortfolio = inputData.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) {
                String year = value.getField(0).toString();
                String netvalue = value.getField(1).toString();
                BigDecimal bd=  new BigDecimal(netvalue);
                String[] dat = value.getField(0).toString().split("\\.");
                int next = Integer.parseInt(dat[1]) +1;
                String nextYearDatum =  dat[0] +"." + dat[1] +"." +Integer.toString(next);
                String nextJahr= Integer.toString(Integer.parseInt(dat[2])+1);
                if(value.getField(5).toString().equals("Database")){
                    if (!portfolio.containsKey(dat[2])) {
                        portfolio.put(dat[2], new Year(year, bd));
                        portfolio.put(nextJahr,new Year(nextYearDatum,bd));
                        portfolio.get(dat[2]).updateSum(dat[1], bd);
                        portfolio.get(nextJahr).addAll(bd);
                        value.setField(1, portfolio.get(dat[2]).getNetValue()[Integer.parseInt(dat[1]) - 1].toString());
                        value.setField(4, "");
                    } else {
                        portfolio.get(dat[2]).updateSum(dat[1], bd);
                        if(portfolio.containsKey(nextJahr))
                            portfolio.get(nextJahr).addAll(bd);
                        value.setField(1, portfolio.get(dat[2]).getNetValue()[Integer.parseInt(dat[1]) - 1].toString());
                        value.setField(4, "");
                    }}

                return value;
            }
        }).forceNonParallel().disableChaining();

        SingleOutputStreamOperator<Row> outputPortfolio2 = outputPortfolio.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                String[] dat = value.getField(0).toString().split("\\.");
                String net = value.getField(1).toString();
                if(portfolio.get(dat[2])!=null){
                    if(!net.equals(portfolio.get(dat[2]).getNetValue()[Integer.parseInt(dat[1])-1].toString())) {
                        for (int i = 0; i <value.getArity() ; i++) {
                            if(!value.getField(i).equals(null))
                                value.setField(i,"");
                        }
                    }}
                value.setField(0,dat[2]+dat[1]);
                return value;
            }
        }).forceNonParallel().disableChaining();
//
        SingleOutputStreamOperator<Row> outputPortfolio3=outputPortfolio2.
//                filter 12 Monaten

        filter(new FilterFunction<Row>() {
    @Override
    public boolean filter(Row value) {
//                //1732850.5600000003, 3255278.970000104, 4794814.370000202 ,6376056.930000274
        return value.getField(5).toString().equals("Database");
    }
}).forceNonParallel().disableChaining();


        output4.writeUsingOutputFormat(createJDBCSink());
        outputPeak3.writeUsingOutputFormat(createJDBCSink());
        outputTSC3.writeUsingOutputFormat(createJDBCSink());
        outputPortfolio3.writeUsingOutputFormat(createJDBCSink());

        env.execute();
    }

    /**
     * Here is the JDBS Sink. Update fieldTypes and Query to meet your requirements.
     * return
     */
    private static JDBCOutputFormat createJDBCSink() {
        int[] fieldTypes = new int[]{
                //SalesDate
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
                //SalesDate
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                //Quartale
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
                                "\"NetValue >=9000\", " +
                                "\"OrderType\", " +
                                "\"PortfolioCategory\" " +
                                "FROM "+ StreamingJob.inputTable


                                +" WHERE "
                                + " \"NetValueCalculated\" != '0.00' "
                )
                .setRowTypeInfo(new RowTypeInfo(fieldTypes))
                .finish();
    }
}