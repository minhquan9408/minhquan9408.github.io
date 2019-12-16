package main;

import java.math.BigDecimal;

public class Year {

    private String year;
    private BigDecimal[] quartal = new BigDecimal[4];
    public Year (){
        for (int i = 0; i < quartal.length ; i++) {
            this.quartal[i] = new BigDecimal("0.00");
        }
    }
    public Year(String year,BigDecimal netvalue){
        this();

        this.year = year;

    }

    public void updateMonate(String i, BigDecimal netvalue){
        int net = Integer.parseInt(i);
        if(net<4) {
            this.quartal[0] = this.quartal[0].add(netvalue);
        } else if (net < 7){
            this.quartal[1] = this.quartal[1].add(netvalue);
        }
        else if (net < 10){
            this.quartal[2] = this.quartal[2].add(netvalue);
        } else{
            this.quartal[3] = this.quartal[3].add(netvalue);
        }
    }


    public BigDecimal[] getNetValue() {
        return quartal;
    }

    public void setNetValue(BigDecimal[] netValue) {
        this.quartal = netValue;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }
}
