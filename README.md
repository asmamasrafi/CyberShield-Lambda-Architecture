# 🛡️ CyberShield — Lambda Architecture for Cybersecurity Monitoring

**CyberShield** is a cybersecurity and Big Data project designed to explore **real-time security event processing, threat detection, machine learning classification, and security monitoring** using a Lambda Architecture.

The project combines a **Speed Layer** for real-time detection with a **Batch Layer** for historical analysis.

```text
                    SECURITY EVENTS
                          │
             ┌────────────┴────────────┐
             │                         │
             ▼                         ▼
      ⚡ SPEED LAYER              📦 BATCH LAYER
      Real-Time Processing        Historical Analysis
             │                         │
          Kafka                    Batch Data
             │                         │
      Spark Streaming          Data Processing / ML
             │                         │
      Threat Detection              Historical
          + ML                       Insights
             │
             ▼
         Cassandra
             │
             ▼
      Security Dashboard
```
![CyberShield Lambda Architecture](docs/architecturee.png)
---

# 🎯 Project Objectives

The main objective of CyberShield is to demonstrate how **distributed data-processing technologies can be applied to cybersecurity monitoring**.

The project focuses on:

* ⚡ Real-time security event processing
* 🔎 Threat detection and classification
* 🤖 Machine learning for security data
* 📊 Historical security analysis
* 💾 Distributed threat storage
* 📈 Security monitoring dashboards
* 🏗️ Lambda Architecture principles

---

# 🏗️ Architecture Overview

CyberShield follows a simplified **Lambda Architecture**, combining real-time stream processing with historical batch processing.

```text
                         Security Events
                                │
               ┌────────────────┴────────────────┐
               │                                 │
               ▼                                 ▼
        ⚡ Speed Layer                       📦 Batch Layer
        Real-Time Events                   Historical Data
               │                                 │
               ▼                                 ▼
            Kafka                         Batch Processing
               │                                 │
               ▼                                 ▼
       Spark Streaming                    Data Analysis
               │                                 │
               ▼                                 ▼
      Threat Detection / ML              Historical Insights
               │
               ▼
           Cassandra
               │
               ▼
      Security Dashboard
```

This architecture makes it possible to combine:

**Real-Time Detection + Historical Analysis + Security Monitoring**

---

# ⚡ Speed Layer — Real-Time Detection

The **Speed Layer** is responsible for processing security events as they arrive.

## 🔄 Real-Time Pipeline

```text
Log Simulator
      ↓
    Kafka
      ↓
Spark Streaming
      ↓
Threat Detection / ML
      ↓
  Cassandra
      ↓
Security Dashboard
```

### Core Components

| Component                  | Role                            |
| -------------------------- | ------------------------------- |
| **Apache Kafka**           | Security event streaming        |
| **Apache Spark Streaming** | Distributed stream processing   |
| **Machine Learning**       | Traffic / threat classification |
| **Apache Cassandra**       | Storage of detected threats     |
| **Dashboard**              | Security monitoring             |

Detected events are stored in the Cassandra `active_threats` table.

---

# 📦 Batch Layer — Historical Analysis

The **Batch Layer** processes historical security data to identify broader trends and patterns.

```text
Historical Security Logs
          ↓
     Batch Processing
          ↓
    Data Analysis / ML
          ↓
   Historical Insights
```

The Batch Layer complements the Speed Layer by providing analysis over previously collected security events.

This separation allows the project to explore:

* 📊 Historical threat analysis
* 🔎 Long-term security patterns
* 🤖 Offline machine-learning analysis
* ⚡ Real-time threat detection

---

# 🤖 Machine Learning Pipeline

CyberShield includes a machine-learning component for classifying network and security traffic.

The ML workflow follows:

```text
Security Data
      ↓
Data Preparation
      ↓
Feature Processing
      ↓
Model Training
      ↓
Threat Classification
      ↓
Detection Pipeline
```

The trained model is integrated into the detection workflow through the Python ML service.

### ML Components

The repository includes:

* `ml_service.py` — ML service
* `app.py` — Application / API component
* `best_model.pkl` — Trained model
* `Untitled1.ipynb` — ML experimentation notebook

---

# 🚨 Threat Detection

The detection pipeline associates security events with relevant threat information.

Each detected event can include:

* 🌐 Source IP address
* 🏷️ Attack type
* 📊 Threat score
* 🕒 Last observed timestamp

## Cassandra Data Model

```text
active_threats
│
├── ip_source
├── attack_type
├── threat_score
└── last_seen
```

The `active_threats` table provides a persistent representation of detected threats that can be consumed by the monitoring dashboard.

---

# 📊 Security Monitoring Dashboard

CyberShield includes a web-based dashboard designed to visualize security events and detected threats.

### Frontend Stack

* **React**
* **Vite**

The dashboard provides a monitoring interface for security information generated by the detection pipeline.

```text
Threat Detection
       ↓
   Cassandra
       ↓
Dashboard Backend
       ↓
 React / Vite
       ↓
Security Monitoring
```

---

# 🛠️ Technology Stack

## 🛡️ Cybersecurity

* Threat Detection
* Security Monitoring
* Intrusion Detection
* Security Event Analysis
* Threat Classification

## 📊 Big Data

* **Apache Kafka**
* **Apache Spark**
* **Spark Streaming**
* **Apache Cassandra**
* Lambda Architecture

## 🤖 Machine Learning

* **Python**
* **Scikit-learn**
* Feature processing
* Model training
* Traffic classification

## 🌐 Web

* **React**
* **Vite**

## 🐳 Infrastructure

* **Docker**
* **Docker Compose**

## 💻 Development

* **Java**
* **Python**
* **Maven**
* **Jupyter Notebook**
* **Git / GitHub**

---

# 📂 Project Structure

```text
CyberShield-Lambda-Architecture/
│
├── batch part/
│   └── Batch processing components
│
├── stream-processing/
│   └── Real-time Spark + Kafka processing
│
├── frontend/
│   └── React / Vite security dashboard
│
├── app.py
├── ml_service.py
├── best_model.pkl
├── Untitled1.ipynb
├── requirements.txt
└── README.md
```

---

# 🚀 Getting Started

## Prerequisites

Make sure the following tools are installed:

* **Docker Desktop**
* **Docker Compose**
* **Java**
* **Maven**
* **Python**
* **Node.js / npm**

---

## 1. Clone the Repository

```bash
git clone https://github.com/asmamasrafi/CyberShield-Lambda-Architecture.git

cd CyberShield-Lambda-Architecture
```

---

## 2. Start the Infrastructure

Make sure Docker Desktop is running.

From the project root:

```bash
docker compose up -d
```

This starts the infrastructure required by the real-time processing pipeline.

---

# 🗄️ Cassandra Configuration

Once the containers are running, connect to Cassandra:

```bash
docker exec -it speed-layer-spark-consumer-cassandra-1 cqlsh
```

Create the cybersecurity keyspace:

```sql
CREATE KEYSPACE IF NOT EXISTS cybersecurity
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
```

Select the keyspace:

```sql
USE cybersecurity;
```

Create the `active_threats` table:

```sql
CREATE TABLE IF NOT EXISTS active_threats (
    ip_source text,
    attack_type text,
    threat_score double,
    last_seen timestamp,
    PRIMARY KEY (ip_source, attack_type)
);
```

Exit Cassandra:

```text
exit
```

---

# ⚡ Start the Spark Streaming Consumer

Navigate to the stream-processing directory:

```bash
cd stream-processing
```

The Spark consumer can be submitted through Docker:

```bash
docker run -it --rm \
--network speed-layer-spark-consumer_default \
-v "${PWD}:/app" \
-w /app \
apache/spark:3.5.1 \
/opt/spark/bin/spark-submit \
--class ma.ensa.cybersecurity.SparkProcessor \
--master local[*] \
--conf "spark.jars.ivy=/tmp/.ivy2" \
--packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
/app/target/speed-layer-spark-consumer-1.0-SNAPSHOT.jar
```

---

# 📡 Simulate Security Events

The project uses a Java-based **Log Simulator** to generate security events.

The simulator uses:

```text
cybersecurity_threat_detection_logs.csv
```

The generated events are sent to Kafka and processed by the Spark Streaming pipeline.

```text
Log Simulator
      ↓
    Kafka
      ↓
Spark Streaming
      ↓
Threat Detection
      ↓
Cassandra
```

Run the `LogSimulator` Java class to start generating events.

---

# 🔍 Verify Detected Threats

To inspect detected threats stored in Cassandra:

```bash
docker exec -it speed-layer-spark-consumer-cassandra-1 \
cqlsh -e "SELECT * FROM cybersecurity.active_threats;"
```

This allows the stored threat records to be inspected directly before they are consumed by the monitoring layer.

---

# 📈 End-to-End Security Workflow

The complete workflow can be summarized as:

```text
Security Events
      ↓
Event Streaming
      ↓
     Kafka
      ↓
Spark Streaming
      ↓
Threat Classification
      ↓
  Threat Score
      ↓
   Cassandra
      ↓
   Dashboard
      ↓
Security Monitoring
```

---

# 🧪 Security Monitoring Workflow

CyberShield demonstrates a simplified security monitoring lifecycle:

### 01 — Collect

Security events are generated from the provided security dataset.

### 02 — Stream

Kafka distributes incoming security events through the real-time pipeline.

### 03 — Process

Spark Streaming processes events as they arrive.

### 04 — Classify

The detection pipeline applies security logic and machine-learning classification.

### 05 — Store

Detected threats are stored in Cassandra.

### 06 — Monitor

The dashboard provides a visual interface for monitoring detected security events.

```text
Collect
   ↓
Stream
   ↓
Process
   ↓
Classify
   ↓
Store
   ↓
Monitor
```

---

# 🎓 Skills Demonstrated

This project demonstrates practical experience in:

### Cybersecurity

* Security monitoring
* Threat detection
* Intrusion detection concepts
* Security event analysis
* Threat classification

### Big Data

* Real-time data processing
* Stream processing
* Batch processing
* Distributed architectures
* Lambda Architecture
* Kafka
* Spark Streaming
* Cassandra

### Machine Learning

* Security dataset preparation
* Feature processing
* Model training
* Traffic classification
* ML integration into detection workflows

### Software & Infrastructure

* Python
* Java
* React
* Docker
* Maven
* Git / GitHub

---

# 🎯 What This Project Demonstrates

Rather than focusing only on individual technologies, CyberShield demonstrates how several components can work together in a **security monitoring pipeline**.

The project connects:

```text
Cybersecurity
      +
Big Data
      +
Machine Learning
      +
Distributed Systems
      +
Security Monitoring
```

This provides a practical foundation for exploring **SOC, Blue Team, SIEM, threat detection, and security analytics** use cases.

---

# 🚀 Future Improvements

Potential improvements for a more production-oriented version include:

## 🔎 Detection

* Add additional attack detection rules
* Integrate more threat intelligence sources
* Improve threat classification
* Add configurable detection thresholds

## 🤖 Machine Learning

* Evaluate multiple models
* Track **precision, recall, and F1-score**
* Add model performance monitoring
* Improve feature engineering
* Introduce continuous model evaluation

## 📊 Monitoring

* Add threat severity levels
* Add historical threat visualizations
* Add filtering by IP, attack type, and severity
* Add real-time alert notifications

## 🛡️ Security

* Implement dashboard authentication
* Add role-based access control
* Secure API endpoints
* Protect sensitive security data

## ☁️ DevSecOps & Infrastructure

* Integrate automated tests
* Add CI/CD pipelines
* Integrate security scanning
* Deploy the architecture to a cloud environment
* Explore integration with a SIEM platform

---

# 📚 References

* Apache Kafka Documentation
* Apache Spark Documentation
* Apache Cassandra Documentation
* Scikit-learn Documentation
* Docker Documentation

---

# 👩‍💻 Author

**Assma MASRAFI**

Cybersecurity Engineering Student — **ENSA Agadir**

### Areas of Interest

`SOC` • `Blue Team` • `Threat Detection` • `SIEM` • `Big Data Security` • `Machine Learning`

🔗 **GitHub:** [@asmamasrafi](https://github.com/asmamasrafi)

---

# ⭐ Project Focus

CyberShield explores how **real-time data processing, distributed systems, and machine learning can support cybersecurity monitoring and threat detection**.

```text
┌──────────┐
│  Collect │
└────┬─────┘
     ↓
┌──────────┐
│  Stream  │
└────┬─────┘
     ↓
┌──────────┐
│  Detect  │
└────┬─────┘
     ↓
┌──────────┐
│  Classify│
└────┬─────┘
     ↓
┌──────────┐
│  Store   │
└────┬─────┘
     ↓
┌──────────┐
│  Monitor │
└──────────┘
```

**CyberShield — From security events to actionable monitoring insights.**
