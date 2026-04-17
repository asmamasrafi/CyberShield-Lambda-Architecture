-- Configuration de la base de données pour le projet CyberSecurity
CREATE KEYSPACE IF NOT EXISTS cybersecurity 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE cybersecurity;

CREATE TABLE IF NOT EXISTS active_threats (
    ip_source text, 
    attack_type text, 
    threat_score double, 
    last_seen timestamp, 
    PRIMARY KEY (ip_source, attack_type)
);