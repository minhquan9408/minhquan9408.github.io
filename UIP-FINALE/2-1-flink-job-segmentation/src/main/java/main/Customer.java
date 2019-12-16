package main;

import java.math.BigDecimal;

public class Customer {
    private BigDecimal accountId;
    private String employee;
    private String sapRelation;
    private double netvalue;
    private int numberTransaction;
    private String industry;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void minusCount(){
        this.count--;
    }
    private int count;


    private int wNumOfEm;
    private int wIndustries;
    private double wSAPRelation;

    private double wNumberofTransaction ;

    private double result;
    private String rating;
    private int NuOfCust;

    public Customer(){

    }
    public Customer(BigDecimal acc,double net,String sapRelation, int nT, String emp,String indus){
        this.setAccountId(acc);
        this.setEmployee(emp);
        this.setIndustry(indus);
        this.setSapRelation(sapRelation);
        this.setNuOfCust(1);
        this.count = 0;
        this.netvalue=0;
        this.numberTransaction=0;

        this.setwSAPRelation(sapRelation);
        this.setwNumOfEm(emp);
        this.setwIndustries(indus);
    }


    //set weigh SAPRelation
    private double setwSAPRelation(String i){
        if (i.equals("Consultant") || i.equals("Press/Analyst") || i.equals("Investor/Shareholder"))
            this.wSAPRelation = 1.2;
        else if (i.equals("Customer") || i.equals("Partner") || i.equals("Prospective Customer") || i.equals("Prospective Partner"))
            this.wSAPRelation = 1.4;
        else if (i.equals("SAP Employee") || i.equals("Student"))
            this.wSAPRelation = 1.0;
        return this.wSAPRelation;
    }

    //Set weigh number of employee
    private int setwNumOfEm(String i){
        if (i.equals(">250"))
            this.wNumOfEm = 5;
        else if (i.equals("<50"))
            this.wNumOfEm = 1;
        else
            this.wNumOfEm = 3;
        return this.wNumOfEm;
    }
    //set weigh for industry
    private int setwIndustries(String i) {
        if(i.equals("Consumer Industries")){
            this.wIndustries=4;
        }else if(i.equals("Service Industries")){
            this.wIndustries=10;
        }else if(i.equals("Financial Services")){
            this.wIndustries=7;
        }else if(i.equals("Energy and Natural Resources")){
            this.wIndustries=2;
        }else if(i.equals("Discrete Industries")){
            this.wIndustries=4;
        }else if(i.equals("Public Services")){
            this.wIndustries=3;
        }

        return this.wIndustries;
    }

    //set weigh No of Trans
    public void setwNumberofTransaction() {
        if (this.numberTransaction<10)
            this.wNumberofTransaction = 1;
        else if (this.numberTransaction<20)
            this.wNumberofTransaction = 1.3;
        else if (this.numberTransaction < 30)
            this.wNumberofTransaction = 1.6;
        else
            this.wNumberofTransaction = 1.9;
    }

    //add NetValue
    public void addNetValue(double netvalue){
        this.netvalue += netvalue;
    }

    //add n0 of Transaction
    public void addNoOfTransaction(int i){
        this.numberTransaction+= i;
    }

    public void setResult(){
        this.result = this.wIndustries * this.wNumOfEm * this.wSAPRelation * this.wNumberofTransaction
                * this.netvalue;
    }

    public String setRating(){
        if(this.result <15000)
            this.rating = "C";
        else if (this.result<47500)
            this.rating="B";
        else
            this.rating="A";
        return this.rating;
    }

    public void aktulisieren(double netvalue, int nT){
        this.addNetValue(netvalue);
        this.addNoOfTransaction(nT);
        this.setwNumberofTransaction();
        this.setResult();
        this.setRating();
        this.count++;
    }

    public double getNetvalue(){
        return this.netvalue;
    }

    public void setAccountId(BigDecimal accountId) {
        this.accountId = accountId;
    }

    public void setEmployee(String employee) {
        this.employee = employee;
    }

    public void setSapRelation(String sapRelation) {
        this.sapRelation = sapRelation;
    }


    public void setIndustry(String industry) {
        this.industry = industry;
    }


    public void setNuOfCust(int nuOfCust) {
        NuOfCust = nuOfCust;
    }

    public String getRating() {
        return rating;
    }


}
