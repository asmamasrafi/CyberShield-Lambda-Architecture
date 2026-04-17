package ma.ensa.cybersecurity;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class SparkProcessor {

	public static void main(String[] args) throws Exception {

        System.out.println("🚀 Démarrage du Consumer Spark (Détection de menaces)...");

        // 1. Configuration de la connexion avec Cassandra
        SparkSession spark = SparkSession.builder()
                .appName("CyberSecurity")
                .config("spark.cassandra.connection.host", "cassandra")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // 2. Connexion à Kafka pour écouter le flux
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "cybersecurity-logs")
                .load();

     
     // 3. Découpage des colonnes du CSV (Version corrigée et simplifiée)
        Dataset<Row> logs = df
                .selectExpr("CAST(value AS STRING) as raw_data")
                .select(
                        split(col("raw_data"), ",").getItem(1).alias("ip_source"),
                        current_timestamp().alias("timestamp"),
                        split(col("raw_data"), ",").getItem(4).alias("status"),
                        split(col("raw_data"), ",").getItem(8).alias("user_agent"),
                        split(col("raw_data"), ",").getItem(9).alias("payload")
                );

        // Debug : Afficher dans la console ce que Spark reçoit
        logs.writeStream().format("console").start();

        // 4. Règle n°1 : Détecter les accès bloqués
        Dataset<Row> alerts1 = logs.filter(col("status").equalTo("blocked"))
                .select(
                        col("ip_source"),
                        lit("blocked-access").alias("attack_type"),
                        lit(5.0).alias("threat_score"),
                        col("timestamp").alias("last_seen")
                );

        // 5. Règle n°2 : Détecter les signatures (SQL injection, Nmap...)
        Dataset<Row> alerts2 = logs.filter(
                col("payload").rlike("(?i)sqlmap|nmap|nikto|' OR 1=1")
                        .or(col("user_agent").rlike("(?i)sqlmap|nmap|nikto"))
        ).select(
                col("ip_source"),
                lit("signature-attack").alias("attack_type"),
                lit(8.0).alias("threat_score"),
                col("timestamp").alias("last_seen")
        );

        // 6. Fusionner toutes les alertes
        Dataset<Row> allAlerts = alerts1.union(alerts2);

        // 7. Envoyer les menaces dans Cassandra
        try {
            allAlerts.writeStream()
                    .format("org.apache.spark.sql.cassandra")
                    .option("keyspace", "cybersecurity")
                    .option("table", "active_threats")
                    .option("checkpointLocation", "/tmp/checkpoint")
                    .outputMode("append")
                    .start()
                    .awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}