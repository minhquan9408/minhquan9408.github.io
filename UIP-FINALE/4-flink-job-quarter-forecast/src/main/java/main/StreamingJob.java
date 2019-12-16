
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
 * Flink Streaming Job to calculate forecast for next quarter
 *
 */


public class StreamingJob {
    private static String postgresHost, postgresDB, postgresUser, postgresPassword;
    private static String inputTable, outputTable;

    static HashMap<String , Year> yearSum = new HashMap<>();
    static HashMap<String , Year> portfolio = new HashMap<>();
    static double[] forecast  = new double[8];
    static double tsc1;
    public static void main(String[] args) throws Exception {
        // Parameters for postgres
        final ParameterTool[] parameterTool = {ParameterTool.fromArgs(args)};

        postgresHost = parameterTool[0].get("postgres-host", "postgres:5432");
        postgresDB = parameterTool[0].get("postgres-db", "kafka_connect");
        postgresUser = parameterTool[0].get("postgres-user", "kafka_connect");
        postgresPassword = parameterTool[0].get("postgres-password", "kafka_connect");
        inputTable = parameterTool[0].get("input-table", "transaction_data_quarter_summary");
        outputTable = parameterTool[0].get("output-table", "transaction_data_quarter_forecast");
        // Create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set input source
        DataStreamSource<Row> inputData = env.createInput(StreamingJob.createJDBCSource());
        SingleOutputStreamOperator<Row> out = inputData.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                return value;
            }
        }).forceNonParallel().disableChaining();
        out.writeUsingOutputFormat(createJDBCSink());
        SingleOutputStreamOperator<Row> output = inputData.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                String netvalue = value.getField(1).toString();
                String[] dat = value.getField(0).toString().split("");
                String jahr = dat[0]+dat[1]+dat[2]+dat[3];
              String year =value.getField(0).toString();
                int monat = Integer.parseInt(dat[4] + dat[5]);

                if(value.getField(4).toString().equals("Basic Forecast Quartal")){
                    if (jahr.equals("2018")) {
                        forecast[StreamingJob.getQuartal(monat)] = Double.parseDouble(netvalue);

                    } else {
                        forecast[StreamingJob.getQuartal(monat)+4] = Double.parseDouble(netvalue);

                    }
                }
                if(value.getField(4).toString().equals("TSC Quartal")){
                    if(year.equals("201803")){
                        tsc1=Double.parseDouble(netvalue);
                    }
                }
                return value;
            }
        }).forceNonParallel().disableChaining();

        SingleOutputStreamOperator<Row> output2 = output.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                if(value.getField(4).toString().equals("Basic Forecast Quartal")){
                    if(value.getField(0).toString().equals("201912")){
                    value.setField(1,(""+ (300000+ tsc1 + forecast(forecast, 0.65, 0.7, 0.1, 4, 2, false)[8])));
                    value.setField(4,"Forecast Median");
                        value.setField(0,"202001");
                    }
                    if(value.getField(0).toString().equals("201909")){
                        value.setField(1,(""+ (280000+ tsc1 + forecast(forecast, 0.6, 0.35, 0.35, 4, 2, false)[8])));
                        value.setField(4,"Forecast Min");
                        value.setField(0,"202001");
                    }
                    if(value.getField(0).toString().equals("201906")){
                        value.setField(1,(""+ (320000+ tsc1 + forecast(forecast, 0.9, 0.1, 0.7, 4, 2, false)[8])));
                        value.setField(4,"Forecast Max");
                        value.setField(0,"202001");
                    }
                }

                return value;
            }
        }).disableChaining();


        SingleOutputStreamOperator<Row> output3=output2.
//                filter 3 Forecast Value
        filter(new FilterFunction<Row>() {
    @Override
    public boolean filter(Row value) {
        return value.getField(4).toString().equals("Forecast Median")
                ||value.getField(4).toString().equals("Forecast Min")
        ||value.getField(4).toString().equals("Forecast Max")
                ;
    }
}).forceNonParallel().disableChaining();


//////*******************************************************************

//Calculate the netvalue sum for Quartal

        SingleOutputStreamOperator<Row> outputQSumme = inputData.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                String netvalue = value.getField(1).toString();
                BigDecimal bd=  new BigDecimal(netvalue);
                String[] dat = value.getField(0).toString().split("");
                String jahr = dat[0]+dat[1]+dat[2]+dat[3];
                int year = Integer.parseInt(jahr) +1;
                String nextJahr = Integer.toString(year);
                String month = dat[4] + dat[5];
                int monat = Integer.parseInt(dat[4] + dat[5]);


                if(value.getField(4).toString().equals("Basic Forecast Quartal")){
                    if (!yearSum.containsKey(jahr)) {
                        yearSum.put(jahr, new Year(jahr, bd));
                        yearSum.put(nextJahr, new Year(nextJahr, bd));
                        yearSum.get(jahr).updateMonate(month, bd);
                       yearSum.get(nextJahr).addAll(bd);
                        value.setField(1, yearSum.get(jahr).getNetValue()[StreamingJob.getQuartal(monat)].toString());
                    } else {
                        yearSum.get(jahr).updateMonate(month, bd);
                        if (yearSum.containsKey(nextJahr))
                        yearSum.get(nextJahr).addAll(bd);
                        value.setField(1, yearSum.get(jahr).getNetValue()[StreamingJob.getQuartal(monat)].toString());

                    }
                    value.setField(4, "Basic Forecast Quartal Summe");
                }

                return value;
            }
        }).forceNonParallel().disableChaining();

        SingleOutputStreamOperator<Row> outputQSumme2=outputQSumme.
//                filter 3 Forecast Value
        filter(new FilterFunction<Row>() {
    @Override
    public boolean filter(Row value) {
        return value.getField(4).toString().equals("Basic Forecast Quartal Summe")
                ;
    }
}).forceNonParallel().disableChaining();
        /////***************************************************************


        //////*******************************************************************
        /////***************************************************************



        output3.writeUsingOutputFormat(createJDBCSink());
        outputQSumme2.writeUsingOutputFormat(createJDBCSink());
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


    ////*********************************************************************
//Method to Calculate the forecast value
    public static  double[] forecast(double[] y, double alpha, double beta,
                                     double gamma, int period, int m, boolean debug) {

        if (y == null) {
            return null;
        }

        int seasons = y.length / period;
        double a0 = calculateInitialLevel(y, period);
        double b0 = calculateInitialTrend(y, period);
        double[] initialSeasonalIndices = calculateSeasonalIndices(y, period, seasons);

        if (debug) {
            System.out.println(String.format(
                    "Total observations: %d, Seasons %d, Periods %d", y.length,
                    seasons, period));
            System.out.println("Initial level value a0: " + a0);
            System.out.println("Initial trend value b0: " + b0);
            printArray("Seasonal Indices: ", initialSeasonalIndices);
        }

        double[] forecast = calculateHoltWinters(y, a0, b0, alpha, beta, gamma,
                initialSeasonalIndices, period, m, debug);

        if (debug) {
            printArray("Forecast", forecast);
        }
        return forecast;
    }

    //    /**
//     * This method realizes the Holt-Winters equations.
//     *
//     * @param y
//     * @param a0
//     * @param b0
//     * @param alpha
//     * @param beta
//     * @param gamma
//     * @param initialSeasonalIndices
//     * @param period
//     * @param m
//     * @param debug
//     * @return - Forecast for m periods.
//     */
    private static double[] calculateHoltWinters(double[] y, double a0, double b0, double alpha,
                                                 double beta, double gamma, double[] initialSeasonalIndices, int period, int m, boolean debug) {

        double[] St = new double[y.length];
        double[] Bt = new double[y.length];
        double[] It = new double[y.length];
        double[] Ft = new double[y.length + m];

        //Initialize base values
        St[1] = a0;
        Bt[1] = b0;

        for (int i = 0; i < period; i++) {
            It[i] = initialSeasonalIndices[i];
        }

        Ft[m] = (St[0] + (m * Bt[0])) * It[0];//This is actually 0 since Bt[0] = 0
        Ft[m + 1] = (St[1] + (m * Bt[1])) * It[1];//Forecast starts from period + 2

        //Start calculations
        for (int i = 2; i < y.length; i++) {

            //Calculate overall smoothing
            if((i - period) >= 0) {
                St[i] = alpha * y[i] / It[i - period] + (1.0 - alpha) * (St[i - 1] + Bt[i - 1]);
            } else {
                St[i] = alpha * y[i] + (1.0 - alpha) * (St[i - 1] + Bt[i - 1]);
            }

            //Calculate trend smoothing
            Bt[i] = gamma * (St[i] - St[i - 1]) + (1 - gamma) * Bt[i - 1];

            //Calculate seasonal smoothing
            if((i - period) >= 0) {
                It[i] = beta * y[i] / St[i] + (1.0 - beta) * It[i - period];
            }

            //Calculate forecast
            if( ((i + m) >= period) ){
                Ft[i + m] = (St[i] + (m * Bt[i])) * It[i - period + m];
            }

            if(debug){
                System.out.println(String.format(
                        "i = %d, y = %f, S = %f, Bt = %f, It = %f, F = %f", i,
                        y[i], St[i], Bt[i], It[i], Ft[i]
                        )
                );
            }
        }

        return Ft;
    }

    /**
     * See: http://robjhyndman.com/researchtips/hw-initialization/
     * 1st period's average can be taken. But y[0] works better.
     *
     * @return - Initial Level value i.e. St[1]
     */
    private static double calculateInitialLevel(double[] y, int period) {

        /**
         double sum = 0;
         for (int i = 0; i < period; i++) {
         sum += y[i];
         }

         return sum / period;
         **/
        return y[0];
    }

    /**
     * See: http://www.itl.nist.gov/div898/handbook/pmc/section4/pmc435.htm
     *
     * @return - Initial trend - Bt[1]
     */
    private static double calculateInitialTrend(double[] y, int period){

        double sum = 0;

        for (int i = 0; i < period; i++) {
            sum += (y[period + i] - y[i]);
        }

        return sum / (period * period);
    }

    /**
     * See: http://www.itl.nist.gov/div898/handbook/pmc/section4/pmc435.htm
     *
     * @return - Seasonal Indices.
     */
    private static double[] calculateSeasonalIndices(double[] y, int period, int seasons){

        double[] seasonalAverage = new double[seasons];
        double[] seasonalIndices = new double[period];

        double[] averagedObservations = new double[y.length];

        for (int i = 0; i < seasons; i++) {
            for (int j = 0; j < period; j++) {
                seasonalAverage[i] += y[(i * period) + j];
            }
            seasonalAverage[i] /= period;
        }

        for (int i = 0; i < seasons; i++) {
            for (int j = 0; j < period; j++) {
                averagedObservations[(i * period) + j] = y[(i * period) + j] / seasonalAverage[i];
            }
        }

        for (int i = 0; i < period; i++) {
            for (int j = 0; j < seasons; j++) {
                seasonalIndices[i] += averagedObservations[(j * period) + i];
            }
            seasonalIndices[i] /= seasons;
        }

        return seasonalIndices;
    }

    private static void printArray(String description, double[] data){

        System.out.println(String.format("******************* %s *********************", description));

        for (int i = 0; i < data.length; i++) {
            System.out.println(data[i]);
        }

        System.out.println(String.format("*****************************************************************", description));
    }

}