package main;

import java.math.BigDecimal;

public class Year {



    private BigDecimal[] netValue = new BigDecimal[12];
    private String year;

    public Year (){
        for (int i = 0; i < netValue.length ; i++) {
            this.netValue[i] = new BigDecimal("0.00");
        }
    }
    public Year(String year,BigDecimal netvalue){
        this();
        String[] yearrrr = year.toString().split("\\.");
        this.year = yearrrr[2];

    }

    public void updateMonate(String i, BigDecimal netvalue){
        int net = Integer.parseInt(i);
        this.netValue[net-1] = this.netValue[net-1].add(netvalue);
    }

    public void updateSum(String i, BigDecimal netvalue){
        int month = Integer.parseInt(i);
        if (month==1){
            for (int j = 0; j < netValue.length; j++) {
                this.netValue[j] = this.netValue[j].add(netvalue);
            }
        }
        else if (month==2){
            for (int j = 1; j <netValue.length ; j++) {
                this.netValue[j] = this.netValue[j].add(netvalue);
            }
        }
        else if (month==3){
            for (int j = 2; j <netValue.length ; j++) {
                this.netValue[j] = this.netValue[j].add(netvalue);
            }
        }
        else if (month==4){
            for (int j = 3; j <netValue.length ; j++) {
                this.netValue[j] = this.netValue[j].add(netvalue);
            }
        }
        else if (month==5){
            for (int j = 4; j <netValue.length ; j++) {
                this.netValue[j] = this.netValue[j].add(netvalue);
            }
        }
        else if (month==6){
            for (int j = 5; j <netValue.length ; j++) {
                this.netValue[j] = this.netValue[j].add(netvalue);
            }
        }
        else if (month==7){
            for (int j = 6; j <netValue.length ; j++) {
                this.netValue[j] = this.netValue[j].add(netvalue);
            }
        }
        else if (month==8){
            for (int j = 7; j <netValue.length ; j++) {
                this.netValue[j] = this.netValue[j].add(netvalue);
            }
        }
        else if (month==9){
            for (int j = 8; j <netValue.length ; j++) {
                this.netValue[j] = this.netValue[j].add(netvalue);
            }
        }
        else if (month==10){
            for (int j = 9; j <netValue.length ; j++) {
                this.netValue[j] = this.netValue[j].add(netvalue);
            }
        }
        else if (month==11){
            for (int j = 10; j <netValue.length ; j++) {
                this.netValue[j] = this.netValue[j].add(netvalue);
            }
        }
        else{
                this.netValue[11] = this.netValue[11].add(netvalue);
        }
    }
    public void addAll(BigDecimal netvalue){
        for (int j = 0; j < netValue.length ; j++) {
            this.netValue[j] = this.netValue[j].add(netvalue);
        }
    }
    public BigDecimal[] getNetValue() {
        return netValue;
    }
}
