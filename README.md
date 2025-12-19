# 🦠 COVID-19 Data Analysis — Big Data Analytics Project

### **Course Code: CSBB 422 – Big Data Analytics**

A Big Data Analytics project focusing on processing, analyzing, and visualizing **COVID-19 datasets** using **Hadoop, Spark, and related tools**.
This project demonstrates the ability to handle large datasets, run distributed analytics, and build meaningful insights on pandemic trends.

---

## 🚀 **Project Overview**

This project processes large-scale COVID-19 datasets to analyze:

* Daily case trends
* Recovery rate patterns
* Mortality trends
* Region-wise comparison
* Growth curves & correlations
* Visualizations for meaningful insights

The system leverages **Big Data technologies** to perform distributed processing on multi-node Hadoop/Spark clusters.

---

## 🧰 **Tech Stack & Tools Used**

### **Big Data Components**

* **Hadoop HDFS** – Distributed file storage
* **Hadoop MapReduce** – Distributed batch processing
* **Apache Spark (PySpark)** – Fast in-memory analytics
* **Kafka (optional)** – For streaming/real-time ingestion
* **HBase (optional)** – NoSQL storage

### **Development Tools**

* Python
* Jupyter Notebook / PySpark shell
* Matplotlib / Seaborn / Pandas (for visualization)
* Linux environment / Ubuntu
* GitHub for version control & submission

---

## 🗂️ **Project Features**

✔️ Load and store large COVID-19 datasets in **HDFS**
✔️ Clean and preprocess raw CSV data
✔️ Perform distributed analysis using **Spark RDD/DataFrame API**
✔️ Calculate:

* Daily new cases
* 7-day rolling averages
* State/Region-wise comparison
* Fatality & recovery analysis
  ✔️ Generate graphical insights
  ✔️ Optional: Real-time ingestion using **Kafka → Spark Streaming**

---

## 🖥️ **Cluster Setup (As required in project)**

* **1 Master Node**
* **2 Worker Nodes**
* Hadoop fully configured (core-site, hdfs-site, yarn-site)
* Spark installed on all nodes
* SSH passwordless communication enabled

---

## 📂 **Repository Structure**

```
Covid-19-Analysis/
│
├── data/                   # Raw COVID datasets (optional placeholder)
├── hdfs/                   # Dataset upload instructions for HDFS
├── spark_scripts/          # PySpark scripts for analysis
├── mapreduce/              # Optional MR code if used
├── visualizations/         # Plots and images
├── ui/                     # UI or dashboard (optional)
├── output/                 # Result files
└── README.md               # Project documentation
```

---

## 📊 **Analytical Tasks Performed**

### 1️⃣ **Data Preprocessing**

* Null value handling
* Date formatting
* Region normalization

### 2️⃣ **Spark-Based Analysis**

* Total cases, recoveries, deaths
* Daily & cumulative statistics
* Region-wise ranking

### 3️⃣ **Visualization**

* Line graphs for trends
* Bar charts for comparisons
* Heatmaps for intensity

---

## 🛠️ **How to Run the Project**

### **Step 1: Upload dataset to HDFS**

```bash
hdfs dfs -mkdir /covid
hdfs dfs -put covid_data.csv /covid/
```

### **Step 2: Run Spark Job**

```bash
spark-submit spark_scripts/covid_analysis.py
```

### **Step 3: View Output**

```bash
hdfs dfs -cat /covid/output/*
```



## 🎯 **Learning Outcomes**

By completing this project, the following competencies were achieved:

* Setup & configuration of **Hadoop, Spark, Kafka, HBase**
* Distributed storage & computing
* Real-time vs batch processing
* Big Data pipeline building
* Data analysis and interpretation
* Using GitHub for submission & version management

---

## 🧑‍💻 **Project Team**

* **Adarsh 221210007**
* **Ankit 221210020**
* **Anupam 221210023**
* * **Ankit 221210058**
 
---

## 📜 **License**

This project is for academic learning under the Big Data Analytics lab course **CSBB 422**.



