/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main;

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
import java.sql.*;

/**
 * Flink Streaming Job to filter data
 *
 */
public class StreamingJob {
    private static String postgresHost, postgresDB, postgresUser, postgresPassword;
    private static String inputTable, outputTable;
	public static void main(String[] args) throws Exception {

	    // Parameters for postgres
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		postgresHost = parameterTool.get("postgres-host", "postgres:5432");
        postgresDB = parameterTool.get("postgres-db", "kafka_connect");
		postgresUser = parameterTool.get("postgres-user", "kafka_connect");
		postgresPassword = parameterTool.get("postgres-password", "kafka_connect");
        inputTable = parameterTool.get("input-table", "transaction_data_persist");
        outputTable = parameterTool.get("output-table", "transaction_data_sac");

        // Create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set input source
        DataStreamSource<Row> inputData = env.createInput(StreamingJob.createJDBCSource());

        // Debug inputdata

        // Update this according to your needs.

        SingleOutputStreamOperator<Row> outputData = inputData.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value)  {
                //convert netvalue to double type COLUMN 29
//                if (value.getField(0).toString().equals("null")) {
//                    value.setField(0,"bla");
//                }

                String s = value.getField(23).toString().replace(".","");
                value.setField(23,s);
                s= value.getField(23).toString().replace(",",".");
                value.setField(23, s);

                for (int i = 0; i < value.getArity(); i++) {
                        if(value.getField(i)!=null) {
                            //Convert "##" to ""
                            if (value.getField(i).toString().equals("##")) value.setField(i, "");
                            //Convert Country to SAC-Country
                            if (value.getField(i).toString().equals("Utd.Arab Emir."))
                                value.setField(i, "United Arab Emirates");
                            if (value.getField(i).toString().equals("Brasil")) value.setField(i, "Brazil");
                            if (value.getField(i).toString().equals("Hong Kong")) value.setField(i, "Hong Kong-China");
                            if (value.getField(i).toString().equals("Russian Fed.")) value.setField(i, "Russia");
                            if (value.getField(i).toString().equals("Taiwan")) value.setField(i, "China");
                            if (value.getField(i).toString().equals("Dominican Rep."))
                                value.setField(i, "Dominican Republic");
                            if (value.getField(i).toString().equals("Dem. Rep. Congo"))
                                value.setField(i, "Congo Democratic Republic");
                            if (value.getField(i).toString().equals("Bosnia-Herz."))
                                value.setField(i, "Bosnia and Herzegovina");
                            if (value.getField(i).toString().equals("Equatorial Guin"))
                                value.setField(i, "Equatorial Guinea");
                            if (value.getField(i).toString().equals("Trinidad Tobago"))
                                value.setField(i, "Trinidad and Tobago");
                            if (value.getField(i).toString().equals("Brunei Dar-es-S"))
                                value.setField(i, "Brunei Darussalam");
                            if (value.getField(i).toString().equals("Macau")) value.setField(i, "Macau-China");
                            if (value.getField(i).toString().equals("Guernsey")) value.setField(i, "United Kingdom");
                            if (value.getField(i).toString().equals("Brit. Virgin Is."))
                                value.setField(i, "British Virgin Islands");
                        } else value.setField(i,"");
                }
                return value;
            }
            });
        // Set output source
        outputData.writeUsingOutputFormat(StreamingJob.createJDBCSink());
        // Commit job
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
                //AccountId
                Types.VARCHAR,
                //Business
                Types.VARCHAR,
                //CheckoutType
                Types.VARCHAR,
                //ContractEndDate
                Types.VARCHAR,

                //ContractStartDate
                Types.VARCHAR,
                //Country
                Types.VARCHAR,
                //Function
                Types.VARCHAR,
                //InternalAccountClassification
                Types.VARCHAR,
                //MarketUnit
                Types.VARCHAR,

                //Material
                Types.VARCHAR,
                //MaterialName
                Types.VARCHAR,
                //MaterialType
                Types.VARCHAR,
                //OrderType
                Types.VARCHAR,
                //PortfolioCategory
                Types.VARCHAR,

                //Region
                Types.VARCHAR,
                //RenewalStatus
                Types.VARCHAR,
                //RenewalType
                Types.VARCHAR,
                //RevType
                Types.VARCHAR,
                //SalesDocument
                Types.VARCHAR,

                //SalesOrder
                Types.VARCHAR,
                //Selling Motion
                Types.VARCHAR,
                //Store
                Types.VARCHAR,
                //NetValue
                Types.VARCHAR,
                //No of Transactions
                Types.VARCHAR,

                //Material
                Types.VARCHAR,
                //MaterialName
                Types.VARCHAR,
                //MaterialType
                Types.VARCHAR,
                //OrderType
                Types.VARCHAR,
                //PortfolioCategory
                Types.VARCHAR,



                //SalesOrder
//                Types.VARCHAR,
//                //Selling Motion
                Types.VARCHAR,
                //Store
                Types.VARCHAR,
                //NetValue
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
                                "\"SalesDate\", " +
                                "\"AccountId\", " +
                                "\"Business\", " +
                                "\"CheckoutType\", " +
                                "\"ContractEndDate\", " +

                                "\"ContractStartDate\", " +
                                "\"Country\", " +
                                "\"FunctionalArea\"," +
                                "\"InternalAccountClassification\", " +
                                "\"MarketUnit\", " +

                                "\"Material\", " +
                                "\"MaterialName\",  " +
                                "\"MaterialType\", " +
                                "\"OrderType\", " +
                                "\"PortfolioCategory\"," +

                                "\"Region\", " +
                                "\"RenewalStatus\", " +
                                "\"RenewalType\", " +
                                "\"RevType\", " +
                                "\"SalesDocumentNo_1\", " +

                                "\"SalesOrderNo\", " +
                                "\"SellingMotion\", " +
                                "\"Store\"," +
                                "\"NetValueCalculated\", " +
                                "\"Number of transactions\", " +

                                "\"Day\", " +
                                "\"SAPRelation\", " +
                                "\"NumberOfEmployees\", " +
//
                                "\"ProductId\", " +
                                "\"TSC\", " +
                                "\"NetValue >=9000\", " +
                                "\"NetValue=0\", " +
                                "\"Industry\" " +
                                ") values (?,?,?,?,?,?,?,?,?,?," +
                                         " ?,?,?,?,?,?,?,?,?,?," +
                                          "?,?,?,?,?,?,?,?,?,?," +
                                            "?,?,?)", StreamingJob.outputTable))
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
                //AccountId
                BasicTypeInfo.STRING_TYPE_INFO,
                //Business
                BasicTypeInfo.STRING_TYPE_INFO,
                //CheckoutType
                BasicTypeInfo.STRING_TYPE_INFO,
                //ContractEndDate
                BasicTypeInfo.STRING_TYPE_INFO,

                //ContractStartDate
                BasicTypeInfo.STRING_TYPE_INFO,
                //Country
                BasicTypeInfo.STRING_TYPE_INFO,
                //Function
                BasicTypeInfo.STRING_TYPE_INFO,
                //InternalAccountClassification
                BasicTypeInfo.STRING_TYPE_INFO,
                //MarketUnit
                BasicTypeInfo.STRING_TYPE_INFO,

                //Material
                BasicTypeInfo.STRING_TYPE_INFO,
                //MaterialName
                BasicTypeInfo.STRING_TYPE_INFO,
                //MaterialType
                BasicTypeInfo.STRING_TYPE_INFO,
                //OrderType
                BasicTypeInfo.STRING_TYPE_INFO,
                //PortfolioCategory
                BasicTypeInfo.STRING_TYPE_INFO,

                //Region
                BasicTypeInfo.STRING_TYPE_INFO,
                //RenewalStatus
                BasicTypeInfo.STRING_TYPE_INFO,
                //RenewalType
                BasicTypeInfo.STRING_TYPE_INFO,
                //RevType
                BasicTypeInfo.STRING_TYPE_INFO,
                //SalesDocument
                BasicTypeInfo.STRING_TYPE_INFO,

                //SalesOrder
                BasicTypeInfo.STRING_TYPE_INFO,
                //Selling Motion
                BasicTypeInfo.STRING_TYPE_INFO,
                //Store
                BasicTypeInfo.STRING_TYPE_INFO,
                //NetValue
                BasicTypeInfo.STRING_TYPE_INFO,
                //No of Transaction
                BasicTypeInfo.STRING_TYPE_INFO,

                //Region
                BasicTypeInfo.STRING_TYPE_INFO,
                //RenewalStatus
                BasicTypeInfo.STRING_TYPE_INFO,
                //RenewalType
                BasicTypeInfo.STRING_TYPE_INFO,
                //RevType
                BasicTypeInfo.STRING_TYPE_INFO,
                //SalesDocument
                BasicTypeInfo.STRING_TYPE_INFO,

                //SalesOrder
//                BasicTypeInfo.STRING_TYPE_INFO,
//                //Selling Motion
                BasicTypeInfo.STRING_TYPE_INFO,
                //Store
                BasicTypeInfo.STRING_TYPE_INFO,
                //NetValue
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
                                "\"Business\", " +
                                "\"CheckoutType\", " +
                                "\"ContractEndDate\", " +

                                "\"ContractStartDate\", " +
                                "\"Country\", " +
                                "\"FunctionalArea\"," +
                                "\"InternalAccountClassification\", " +
                                "\"MarketUnit\", " +

                                "\"Material\", " +
                                "\"MaterialName\",  " +
                                "\"MaterialType\", " +
                                "\"OrderType\", " +
                                "\"PortfolioCategory\"," +

                                "\"Region\", " +
                                "\"RenewalStatus\", " +
                                "\"RenewalType\", " +
                                "\"RevType\", " +
                                "\"SalesDocumentNo_1\", " +

                                "\"SalesOrderNo\", " +
                                "\"SellingMotion\", " +
                                "\"Store\"," +
                                "\"NetValueCalculated\", " +
                                "\"Number of transactions\", " +

                                "\"Day\", " +
                                "\"SAPRelation\", " +
                                "\"NumberOfEmployees\", " +
//
                                "\"ProductId\", " +
                                "\"TSC\", " +
                                "\"NetValue >=9000\", " +
                                "\"NetValue=0\", " +
                                "\"Industry\" " +
                        "FROM " + StreamingJob.inputTable
//                        +" WHERE \"SellingMotion\" = 'No Touch'"
                )
                .setRowTypeInfo(new RowTypeInfo(fieldTypes))
                .finish();
    }
}
