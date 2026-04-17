
# 🛡️ CyberShield : Architecture Lambda pour la Détection de Menaces

Ce projet implémente une solution de monitoring de cybersécurité en temps réel. Il combine une couche de traitement par lots (**Batch Layer**) et une couche de traitement en flux (**Speed Layer**) pour détecter les activités malveillantes sur un réseau.

---

## 🏗️ Architecture du Projet

Le projet est divisé en trois modules principaux :

1.  **`/frontend`** : Dashboard React/Vite pour la visualisation des alertes en direct.
2.  **`/stream-processing` (Speed Layer)** : Pipeline Spark Streaming + Kafka + Cassandra.
3.  **`/batch-processing` (Batch Layer)** : Analyse historique des logs pour les tendances de fond.

---

## 🛠️ Guide d'Utilisation du Stream Processing (Speed Layer)

Cette partie est le cœur du système de détection en temps réel.

### 1. Démarrage de l'Infrastructure
Assurez-vous que Docker Desktop est lancé. À la racine du projet, exécutez la commande suivante pour lancer les serveurs Kafka, Zookeeper et Cassandra :
`
docker compose up -d

### 2. Configuration de la Base de Données (Cassandra)
Une fois les conteneurs opérationnels, vous devez créer la table qui stockera les alertes détectées par Spark :

# Entrer dans la console Cassandra
docker exec -it speed-layer-spark-consumer-cassandra-1 cqlsh

# Créer le keyspace et la table
CREATE KEYSPACE IF NOT EXISTS cybersecurity WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE cybersecurity;
CREATE TABLE IF NOT EXISTS active_threats (
    ip_source text, 
    attack_type text, 
    threat_score double, 
    last_seen timestamp, 
    PRIMARY KEY (ip_source, attack_type)
);
exit
### 3. Lancement de l'Analyse Temps Réel (Spark Consumer)

Ouvrez un terminal dans le dossier /stream-processing et exécutez le traitement Spark via Docker :

docker run -it --rm --network speed-layer-spark-consumer_default -v "${PWD}:/app" -w /app apache/spark:3.5.1 /opt/spark/bin/spark-submit --class ma.ensa.cybersecurity.SparkProcessor --master local[*] --conf "spark.jars.ivy=/tmp/.ivy2" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 /app/target/speed-layer-spark-consumer-1.0-SNAPSHOT.jar


### 4. Lancement de la Simulation de Logs (Java Producer)
Importez le projet dans Eclipse.

Assurez-vous que le fichier cybersecurity_threat_detection_logs.csv est présent dans le dossier du projet.

Exécutez la classe LogSimulator.java (Run As > Java Application).

### 📊 Vérification et Monitoring

Voir les alertes en direct
Pour vérifier que les menaces sont bien enregistrées dans la base de données, lancez :

docker exec -it speed-layer-spark-consumer-cassandra-1 cqlsh -e "SELECT * FROM cybersecurity.active_threats;"


Le Backend du Dashboard doit se connecter à Cassandra sur le Port 9042 pour récupérer les données de la table active_threats.

Développé par l'équipe ENSA Agadir - 2026


