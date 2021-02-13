package ru.otus.sparkmlstreaming

case class Data(
    CLIENTNUM: Int,
    Attrition_Flag: String,
    Customer_Age: Int,
    Gender: String,
    Dependent_count: Int,
    Education_Level: String,
    Marital_Status: String,
    Income_Category: String,
    Card_Category: String,
    Months_on_book: Int,
    Total_Relationship_Count: Int,
    Months_Inactive_12_mon: Int,
    Contacts_Count_12_mon: Int,
    Credit_Limit: Double,
    Total_Revolving_Bal: Int,
    Avg_Open_To_Buy: Double,
    Total_Amt_Chng_Q4_Q1: Double,
    Total_Trans_Amt: Int,
    Total_Trans_Ct: Int,
    Total_Ct_Chng_Q4_Q1: Double,
    Avg_Utilization_Ratio: Double,
    Naive_Bayes_Classifier_Attrition_Flag_Card_Category_Contacts_Count_12_mon_Dependent_count_Education_Level_Months_Inactive_12_mon_1: Double,
    Naive_Bayes_Classifier_Attrition_Flag_Card_Category_Contacts_Count_12_mon_Dependent_count_Education_Level_Months_Inactive_12_mon_2: Double
)
